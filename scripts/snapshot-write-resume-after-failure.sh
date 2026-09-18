#!/usr/bin/env bash
#
# Test 5 — Writes Resume After Snapshot Failure
#
# Ansible owns the NISD lifecycle, so this test is split into phases:
#
#   baseline
#       Verify the existing live client can write/read before the fault.
#
#   expect-snapshot-failure
#       Run while NISD is DOWN. Snapshot creation must return a real failure
#       (not merely time out), and CP must report state=failed.
#
#   verify-writes-resume
#       Run after the SAME NISD is restarted on the SAME backing storage.
#       The existing live client must recover, accept a new write, and read it
#       back correctly.
#

set -Eeuo pipefail

PHASE="${1:-}"
case "${PHASE}" in
    baseline|expect-snapshot-failure|verify-writes-resume) ;;
    *)
        echo "Usage: $0 {baseline|expect-snapshot-failure|verify-writes-resume}" >&2
        exit 2
        ;;
esac

: "${VDEV_UUID:?VDEV_UUID is required}"
: "${LIVE_DEVICE:?LIVE_DEVICE is required}"
: "${NIOVA_SNAPSHOT:?NIOVA_SNAPSHOT is required}"
: "${TEST_LOGDIR:?TEST_LOGDIR is required}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_TEST_HELPERS="${SNAPSHOT_TEST_HELPERS:-${SCRIPT_DIR}/snapshot-test-helpers.sh}"

[[ -r "${SNAPSHOT_TEST_HELPERS}" ]] || {
    echo "Missing snapshot helper: ${SNAPSHOT_TEST_HELPERS}" >&2
    exit 1
}

# shellcheck source=/dev/null
source "${SNAPSHOT_TEST_HELPERS}"

SNAPSHOT_NAME="${SNAPSHOT_NAME:-snapshot-failure-resume-$$}"
SNAPSHOT_FAIL_TIMEOUT="${SNAPSHOT_FAIL_TIMEOUT:-45}"
RECOVERY_TIMEOUT="${RECOVERY_TIMEOUT:-90}"
POST_FAILURE_IO_TIMEOUT="${POST_FAILURE_IO_TIMEOUT:-45}"

# Same fixed region as the other basic snapshot tests.
SEED_A="5001"
SEED_B="5002"
PATTERN_A="0xAAAAAAAA"
PATTERN_B="0xBBBBBBBB"

STATE_FILE="${TEST_LOGDIR}/snapshot-failure-resume-state.json"
LAST_FAILED_SNAPSHOT_ID=""

mkdir -p "${TEST_LOGDIR}"

save_state()
{
    local baseline_hash="$1"
    local failed_snapshot_id="${2:-}"

    BASELINE_HASH="${baseline_hash}" \
    FAILED_SNAPSHOT_ID="${failed_snapshot_id}" \
    VDEV_UUID_SAVE="${VDEV_UUID}" \
    SNAPSHOT_NAME_SAVE="${SNAPSHOT_NAME}" \
    python3 - "${STATE_FILE}" <<'PY'
import json
import os
import sys

snapshot_id = os.environ.get("FAILED_SNAPSHOT_ID", "").strip()

state = {
    "vdev_uuid": os.environ["VDEV_UUID_SAVE"],
    "snapshot_name": os.environ["SNAPSHOT_NAME_SAVE"],
    "baseline_hash": os.environ["BASELINE_HASH"],
    "failed_snapshot_id": int(snapshot_id) if snapshot_id else None,
}

with open(sys.argv[1], "w", encoding="utf-8") as fp:
    json.dump(state, fp, indent=2, sort_keys=True)
    fp.write("\n")
PY
}

update_failed_snapshot_id()
{
    local failed_snapshot_id="$1"

    python3 - "${STATE_FILE}" "${failed_snapshot_id}" <<'PY'
import json
import sys

path = sys.argv[1]
snapshot_id = int(sys.argv[2])

with open(path, encoding="utf-8") as fp:
    state = json.load(fp)

state["failed_snapshot_id"] = snapshot_id

with open(path, "w", encoding="utf-8") as fp:
    json.dump(state, fp, indent=2, sort_keys=True)
    fp.write("\n")
PY
}

state_field()
{
    local field="$1"

    python3 - "${STATE_FILE}" "${field}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fp:
    state = json.load(fp)

value = state.get(sys.argv[2])
if value is not None:
    print(value)
PY
}

# Run a helper function in a subshell with an explicit wall-clock bound.
# This is important for the barrier-release assertion: a blocked write must not
# be allowed to hang the test forever.
run_bounded()
{
    local timeout_seconds="$1"
    local description="$2"
    shift 2

    "$@" &
    local pid=$!
    local deadline=$((SECONDS + timeout_seconds))

    while kill -0 "${pid}" 2>/dev/null; do
        if (( SECONDS >= deadline )); then
            kill -TERM "${pid}" 2>/dev/null || true
            sleep 1
            kill -KILL "${pid}" 2>/dev/null || true
            wait "${pid}" 2>/dev/null || true
            fail "${description} timed out after ${timeout_seconds}s"
        fi
        sleep 0.2
    done

    if ! wait "${pid}"; then
        fail "${description} failed"
    fi
}

wait_for_live_recovery()
{
    local deadline=$((SECONDS + RECOVERY_TIMEOUT))
    local probe_log="${TEST_LOGDIR}/live-recovery-probe.log"

    log "Waiting up to ${RECOVERY_TIMEOUT}s for existing live client to recover"

    while (( SECONDS < deadline )); do
        if timeout 5s sudo dd \
            if="${LIVE_DEVICE}" \
            of=/dev/null \
            bs=4k \
            skip=262144 \
            count=1 \
            iflag=direct \
            status=none \
            >"${probe_log}" 2>&1
        then
            log "Existing live device is responsive again"
            return 0
        fi

        sleep 1
    done

    fail "live device did not recover within ${RECOVERY_TIMEOUT}s"
}

snapshot_create_must_fail()
{
    local log_file="${TEST_LOGDIR}/${SNAPSHOT_NAME}-expected-failure.log"
    local rc

    log "Creating snapshot '${SNAPSHOT_NAME}' while required NISD is unavailable"
    log "Snapshot create is EXPECTED to fail"

    set +e
    timeout --foreground "${SNAPSHOT_FAIL_TIMEOUT}s" \
        "${NIOVA_SNAPSHOT}" \
            --vdev "${VDEV_UUID}" \
            --op create \
            --snapshot-name "${SNAPSHOT_NAME}" \
            --keep \
        2>&1 | tee "${log_file}"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ ${rc} -eq 0 ]]; then
        fail "snapshot unexpectedly succeeded while NISD was down"
    fi

    # A CLI timeout does not prove that the snapshot operation itself completed
    # with failure. It could still be pending and could leave the barrier held.
    if [[ ${rc} -eq 124 ]]; then
        fail "snapshot command timed out instead of returning a snapshot failure"
    fi

    log "Snapshot command returned expected failure rc=${rc}"
}

wait_for_snapshot_failed()
{
    local deadline response state cp_id cp_name cp_vdev

    cp_login
    deadline=$((SECONDS + SNAPSHOT_TIMEOUT))

    while (( SECONDS < deadline )); do
        if response="$(cp_get_snapshot "${SNAPSHOT_NAME}" 2>"${TEST_LOGDIR}/failed-snapshot-cp.err")"; then
            printf '%s\n' "${response}" > "${TEST_LOGDIR}/failed-snapshot-cp.json"

            state="$(printf '%s' "${response}" | json_get_field state || true)"
            cp_id="$(printf '%s' "${response}" | json_get_field snapshot_id || true)"
            cp_name="$(printf '%s' "${response}" | json_get_field name || true)"
            cp_vdev="$(printf '%s' "${response}" | json_get_field vdev_id || true)"

            log "CP lookup after create failure: id=${cp_id:-unknown} state=${state:-unknown}"

            if [[ "${state}" == "applied" ]]; then
                fail "failed snapshot was incorrectly recorded as applied"
            fi

            if [[ "${state}" == "failed" ]]; then
                [[ "${cp_id}" =~ ^[1-9][0-9]*$ ]] ||
                    fail "failed snapshot has invalid snapshot ID '${cp_id}'"

                [[ -z "${cp_name}" || "${cp_name}" == "${SNAPSHOT_NAME}" ]] ||
                    fail "snapshot name mismatch: expected ${SNAPSHOT_NAME}, got ${cp_name}"

                [[ -z "${cp_vdev}" || "${cp_vdev}" == "${VDEV_UUID}" ]] ||
                    fail "snapshot vdev mismatch: expected ${VDEV_UUID}, got ${cp_vdev}"

                LAST_FAILED_SNAPSHOT_ID="${cp_id}"
                return 0
            fi
        fi

        sleep 0.5
    done

    fail "CP did not report snapshot state=failed within ${SNAPSHOT_TIMEOUT}s"
}

require_command timeout
require_command dd
require_block_device "${LIVE_DEVICE}"

case "${PHASE}" in
baseline)
    log "Establishing healthy pre-fault baseline"

    run_bounded \
        "${POST_FAILURE_IO_TIMEOUT}" \
        "baseline write" \
        fio_write_generation \
        "${LIVE_DEVICE}" \
        "baseline-A" \
        "${SEED_A}" \
        "${SNAP_OFFSET}" \
        "${SNAP_SIZE}" \
        "${SNAP_BS}" \
        "${PATTERN_A}"

    run_bounded \
        "${POST_FAILURE_IO_TIMEOUT}" \
        "baseline readback" \
        fio_verify_generation \
        "${LIVE_DEVICE}" \
        "baseline-A" \
        "${SEED_A}" \
        "${SNAP_OFFSET}" \
        "${SNAP_SIZE}" \
        "${SNAP_BS}" \
        "${PATTERN_A}" \
        "baseline-readback"

    A_HASH="$(region_sha256 "${LIVE_DEVICE}")"
    [[ "${A_HASH}" =~ ^[0-9a-f]{64}$ ]] ||
        fail "invalid baseline SHA-256 '${A_HASH}'"

    save_state "${A_HASH}"

    cat <<EOF
PASS: pre-fault baseline
  live device:  ${LIVE_DEVICE}
  baseline:     A
  baseline hash:${A_HASH}
EOF
    ;;

expect-snapshot-failure)
    [[ -f "${STATE_FILE}" ]] ||
        fail "missing state file ${STATE_FILE}; baseline phase must run first"

    [[ "$(state_field vdev_uuid)" == "${VDEV_UUID}" ]] ||
        fail "vdev changed between phases"
    [[ "$(state_field snapshot_name)" == "${SNAPSHOT_NAME}" ]] ||
        fail "snapshot name changed between phases"

    snapshot_create_must_fail
    wait_for_snapshot_failed

    update_failed_snapshot_id "${LAST_FAILED_SNAPSHOT_ID}"

    cat <<EOF
PASS: snapshot failed as expected
  snapshot:    ${SNAPSHOT_NAME}
  snapshot id: ${LAST_FAILED_SNAPSHOT_ID}
  CP state:    failed
EOF
    ;;

verify-writes-resume)
    [[ -f "${STATE_FILE}" ]] ||
        fail "missing state file ${STATE_FILE}"

    SAVED_VDEV="$(state_field vdev_uuid)"
    SAVED_NAME="$(state_field snapshot_name)"
    A_HASH="$(state_field baseline_hash)"
    FAILED_SNAPSHOT_ID="$(state_field failed_snapshot_id)"

    [[ "${SAVED_VDEV}" == "${VDEV_UUID}" ]] ||
        fail "vdev changed between phases"
    [[ "${SAVED_NAME}" == "${SNAPSHOT_NAME}" ]] ||
        fail "snapshot name changed between phases"
    [[ "${FAILED_SNAPSHOT_ID}" =~ ^[1-9][0-9]*$ ]] ||
        fail "failed snapshot ID was not recorded"

    # NISD has been restarted by Ansible. Do NOT restart the live ublk/nclient:
    # this test needs the same client instance that experienced snapshot failure.
    wait_for_live_recovery

    # Critical assertion: this write must complete in a bounded time. If the
    # snapshot failure path forgot to release the write barrier, it will time out.
    run_bounded \
        "${POST_FAILURE_IO_TIMEOUT}" \
        "post-failure write (possible stuck snapshot barrier)" \
        fio_write_generation \
        "${LIVE_DEVICE}" \
        "post-failure-B" \
        "${SEED_B}" \
        "${SNAP_OFFSET}" \
        "${SNAP_SIZE}" \
        "${SNAP_BS}" \
        "${PATTERN_B}"

    # Read back the exact generation we just wrote.
    run_bounded \
        "${POST_FAILURE_IO_TIMEOUT}" \
        "post-failure readback" \
        fio_verify_generation \
        "${LIVE_DEVICE}" \
        "post-failure-B" \
        "${SEED_B}" \
        "${SNAP_OFFSET}" \
        "${SNAP_SIZE}" \
        "${SNAP_BS}" \
        "${PATTERN_B}" \
        "post-failure-readback"

    B_HASH="$(region_sha256 "${LIVE_DEVICE}")"
    [[ "${B_HASH}" =~ ^[0-9a-f]{64}$ ]] ||
        fail "invalid post-failure SHA-256 '${B_HASH}'"

    [[ "${B_HASH}" != "${A_HASH}" ]] ||
        fail "post-failure write did not change the test region"

    # The failed snapshot must remain failed; it must never transition to applied
    # merely because NISD came back.
    wait_for_snapshot_failed
    [[ "${LAST_FAILED_SNAPSHOT_ID}" == "${FAILED_SNAPSHOT_ID}" ]] ||
        fail "failed snapshot ID changed after NISD restart"

    cat <<EOF
PASS: writes resume after snapshot failure
  snapshot:         ${SNAPSHOT_NAME}
  snapshot id:      ${FAILED_SNAPSHOT_ID}
  snapshot state:   failed
  subsequent write: success
  subsequent read:  correct
  baseline hash:    ${A_HASH}
  new-data hash:    ${B_HASH}
EOF
    ;;
esac
