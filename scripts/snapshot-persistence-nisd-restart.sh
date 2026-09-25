#!/usr/bin/env bash
# Test 3: Snapshot persistence across NISD restart.

set -Eeuo pipefail

PHASE="${1:-}"
case "${PHASE}" in
    prepare|verify-after-restart) ;;
    *) echo "Usage: $0 {prepare|verify-after-restart}" >&2; exit 2 ;;
esac

: "${VDEV_UUID:?VDEV_UUID must be set}"
: "${LIVE_DEVICE:?LIVE_DEVICE must be set}"

SNAPSHOT_NAME="${SNAPSHOT_NAME:-snapshot-persistence-$$}"
NIOVA_SNAPSHOT="${NIOVA_SNAPSHOT:-scripts/niova-snapshot}"
NIOVA_UBLK="${NIOVA_UBLK:-niova-ublk}"
TEST_LOGDIR="${TEST_LOGDIR:-/tmp/snapshot-persistence-nisd-restart}"
SNAPSHOT_TIMEOUT="${SNAPSHOT_TIMEOUT:-30}"
UBLK_TIMEOUT="${UBLK_TIMEOUT:-30}"
RECOVERY_TIMEOUT="${RECOVERY_TIMEOUT:-90}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_TEST_HELPERS="${SNAPSHOT_TEST_HELPERS:-${SCRIPT_DIR}/snapshot-test-helpers.sh}"
[[ -r "${SNAPSHOT_TEST_HELPERS}" ]] || { echo "Missing helper: ${SNAPSHOT_TEST_HELPERS}" >&2; exit 1; }
# shellcheck source=snapshot-test-helpers.sh
source "${SNAPSHOT_TEST_HELPERS}"

SEED_A=1001
SEED_B=1002
PATTERN_A=0xAAAAAAAA
PATTERN_B=0xBBBBBBBB
STATE_FILE="${TEST_LOGDIR}/snapshot-persistence-state.json"

cleanup()
{
    local rc=$?
    stop_snapshot_mount || true
    [[ ${rc} -eq 0 ]] || log "Phase '${PHASE}' failed. Logs retained at ${TEST_LOGDIR}"
    return "${rc}"
}
trap cleanup EXIT

save_state()
{
    local a_hash="$1" b_hash="$2" snapshot_id="$3"

    A_HASH="${a_hash}" B_HASH="${b_hash}" SNAPSHOT_ID="${snapshot_id}" \
    VDEV_UUID_SAVE="${VDEV_UUID}" SNAPSHOT_NAME_SAVE="${SNAPSHOT_NAME}" \
    python3 - "${STATE_FILE}" <<'PY'
import json, os, sys
state = {
    "vdev_uuid": os.environ["VDEV_UUID_SAVE"],
    "snapshot_name": os.environ["SNAPSHOT_NAME_SAVE"],
    "snapshot_id": int(os.environ["SNAPSHOT_ID"]),
    "a_hash": os.environ["A_HASH"],
    "b_hash": os.environ["B_HASH"],
}
with open(sys.argv[1], "w", encoding="utf-8") as fp:
    json.dump(state, fp, indent=2, sort_keys=True)
    fp.write("\n")
PY
}

state_field()
{
    local field="$1"
    python3 - "${STATE_FILE}" "${field}" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as fp:
    state = json.load(fp)
print(state[sys.argv[2]])
PY
}

wait_for_live_recovery()
{
    local deadline=$((SECONDS + RECOVERY_TIMEOUT))
    local probe_log="${TEST_LOGDIR}/live-recovery-probe.log"

    log "Waiting up to ${RECOVERY_TIMEOUT}s for live client/NISD recovery"
    while (( SECONDS < deadline )); do
        if timeout 5s sudo dd \
            if="${LIVE_DEVICE}" of=/dev/null \
            bs=4k skip=262144 count=1 iflag=direct status=none \
            >"${probe_log}" 2>&1
        then
            log "Live device is responsive again"
            return 0
        fi
        sleep 1
    done
    fail "live device did not recover within ${RECOVERY_TIMEOUT}s after NISD restart"
}

require_command fio
require_command curl
require_command python3
require_command sha256sum
require_command dd
require_command timeout
require_command "${NIOVA_SNAPSHOT}"
require_command "${NIOVA_UBLK}"
require_block_device "${LIVE_DEVICE}"

if [[ "${PHASE}" == prepare ]]; then
    log "PRE-RESTART phase: VBLKs 262144..294911"

    # A before snapshot.
    fio_write_generation "${LIVE_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}"
    fio_verify_generation "${LIVE_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" pre-snapshot
    A_HASH="$(region_sha256 "${LIVE_DEVICE}")"

    # Create and verify snapshot.
    snapshot_create "${SNAPSHOT_NAME}"
    CLI_ID="${LAST_CLI_SNAPSHOT_ID}"
    cp_login
    wait_for_snapshot_applied "${SNAPSHOT_NAME}" "${CLI_ID}" pre-restart
    SNAPSHOT_ID="${LAST_CP_SNAPSHOT_ID}"
    [[ "${SNAPSHOT_ID}" =~ ^[1-9][0-9]*$ ]] || fail "invalid snapshot ID '${SNAPSHOT_ID}'"

    # B becomes live.
    fio_write_generation "${LIVE_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}"
    fio_verify_generation "${LIVE_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}" pre-restart-live
    B_HASH="$(region_sha256 "${LIVE_DEVICE}")"
    [[ "${A_HASH}" != "${B_HASH}" ]] || fail "A and B are byte-identical"

    LIVE_HASH="$(region_sha256 "${LIVE_DEVICE}")"
    [[ "${LIVE_HASH}" == "${B_HASH}" ]] || fail "live view is not B before restart"

    start_snapshot_mount "${SNAPSHOT_NAME}" pre-restart
    fio_verify_generation "${RO_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" pre-restart-snapshot
    SNAP_HASH="$(region_sha256 "${RO_DEVICE}")"
    [[ "${SNAP_HASH}" == "${A_HASH}" ]] || fail "snapshot does not contain A before restart"
    [[ "${SNAP_HASH}" != "${B_HASH}" ]] || fail "snapshot exposes B before restart"
    stop_snapshot_mount

    save_state "${A_HASH}" "${B_HASH}" "${SNAPSHOT_ID}"

    cat <<EOF_SUMMARY
PASS: snapshot persistence pre-restart phase
  snapshot id: ${SNAPSHOT_ID}
  live:         B (${B_HASH})
  snapshot:     A (${A_HASH})
  state file:   ${STATE_FILE}
EOF_SUMMARY
else
    [[ -f "${STATE_FILE}" ]] || fail "state file missing: ${STATE_FILE}; run prepare first"

    SAVED_VDEV_UUID="$(state_field vdev_uuid)"
    SAVED_SNAPSHOT_NAME="$(state_field snapshot_name)"
    SAVED_SNAPSHOT_ID="$(state_field snapshot_id)"
    A_HASH="$(state_field a_hash)"
    B_HASH="$(state_field b_hash)"

    [[ "${SAVED_VDEV_UUID}" == "${VDEV_UUID}" ]] || fail "vdev changed between phases"
    [[ "${SAVED_SNAPSHOT_NAME}" == "${SNAPSHOT_NAME}" ]] || fail "snapshot name changed between phases"

    wait_for_live_recovery

    cp_login
    wait_for_snapshot_applied "${SNAPSHOT_NAME}" "${SAVED_SNAPSHOT_ID}" post-restart
    [[ "${LAST_CP_SNAPSHOT_ID}" == "${SAVED_SNAPSHOT_ID}" ]] || fail "snapshot ID changed across restart"

    fio_verify_generation "${LIVE_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}" post-restart-live
    LIVE_HASH="$(region_sha256 "${LIVE_DEVICE}")"
    [[ "${LIVE_HASH}" == "${B_HASH}" ]] || fail "live B did not survive NISD restart"
    [[ "${LIVE_HASH}" != "${A_HASH}" ]] || fail "live device reverted to A after restart"

    start_snapshot_mount "${SNAPSHOT_NAME}" post-restart
    fio_verify_generation "${RO_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" post-restart-snapshot
    SNAP_HASH="$(region_sha256 "${RO_DEVICE}")"
    [[ "${SNAP_HASH}" == "${A_HASH}" ]] || fail "snapshot A did not survive NISD restart"
    [[ "${SNAP_HASH}" != "${B_HASH}" ]] || fail "snapshot exposes live B after restart"

    cat <<EOF_SUMMARY
PASS: snapshot persistence across NISD restart
  snapshot id:       ${SAVED_SNAPSHOT_ID}
  post-restart live: B (${B_HASH})
  post-restart snap: A (${SNAP_HASH})
EOF_SUMMARY
fi
