#!/usr/bin/env bash
# Test 2: Multiple snapshots preserve multiple versions.

set -Eeuo pipefail

: "${VDEV_UUID:?VDEV_UUID must be set}"

LIVE_DEVICE="${LIVE_DEVICE:-/dev/ublkb0}"
SNAPSHOT1_NAME="${SNAPSHOT1_NAME:-snapshot-multi-1-$$}"
SNAPSHOT2_NAME="${SNAPSHOT2_NAME:-snapshot-multi-2-$$}"
NIOVA_SNAPSHOT="${NIOVA_SNAPSHOT:-scripts/niova-snapshot}"
NIOVA_UBLK="${NIOVA_UBLK:-niova-ublk}"
TEST_LOGDIR="${TEST_LOGDIR:-/tmp/snapshot-multiple-versions}"
SNAPSHOT_TIMEOUT="${SNAPSHOT_TIMEOUT:-30}"
UBLK_TIMEOUT="${UBLK_TIMEOUT:-30}"
DELETE_SNAPSHOTS_ON_SUCCESS="${DELETE_SNAPSHOTS_ON_SUCCESS:-1}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_TEST_HELPERS="${SNAPSHOT_TEST_HELPERS:-${SCRIPT_DIR}/snapshot-test-helpers.sh}"
[[ -r "${SNAPSHOT_TEST_HELPERS}" ]] || { echo "Missing helper: ${SNAPSHOT_TEST_HELPERS}" >&2; exit 1; }
# shellcheck source=snapshot-test-helpers.sh
source "${SNAPSHOT_TEST_HELPERS}"

SEED_A=1001
SEED_B=1002
SEED_C=1003
PATTERN_A=0xAAAAAAAA
PATTERN_B=0xBBBBBBBB
PATTERN_C=0xCCCCCCCC

SNAPSHOT1_ID=""
SNAPSHOT2_ID=""
SNAPSHOT1_CREATED=0
SNAPSHOT2_CREATED=0
TEST_PASSED=0

cleanup()
{
    local rc=$?
    stop_snapshot_mount || true

    if [[ "${TEST_PASSED}" -eq 1 && "${DELETE_SNAPSHOTS_ON_SUCCESS}" -eq 1 ]]; then
        [[ "${SNAPSHOT2_CREATED}" -eq 1 ]] && snapshot_delete "${SNAPSHOT2_NAME}" || true
        [[ "${SNAPSHOT1_CREATED}" -eq 1 ]] && snapshot_delete "${SNAPSHOT1_NAME}" || true
    fi

    [[ ${rc} -eq 0 ]] || log "Test failed. Logs retained under ${TEST_LOGDIR}"
    return "${rc}"
}
trap cleanup EXIT

require_command fio
require_command curl
require_command python3
require_command sha256sum
require_command dd
require_command "${NIOVA_SNAPSHOT}"
require_command "${NIOVA_UBLK}"
require_block_device "${LIVE_DEVICE}"

cp_login
log "Test region: ${LIVE_DEVICE}, offset=${SNAP_OFFSET}, size=${SNAP_SIZE}, VBLKs 262144..294911"

# A -> snapshot 1
fio_write_generation "${LIVE_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}"
fio_verify_generation "${LIVE_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" A
A_HASH="$(region_sha256 "${LIVE_DEVICE}")"

snapshot_create "${SNAPSHOT1_NAME}"
SNAPSHOT1_CREATED=1
SNAP1_CLI_ID="${LAST_CLI_SNAPSHOT_ID}"
wait_for_snapshot_applied "${SNAPSHOT1_NAME}" "${SNAP1_CLI_ID}" snapshot1
SNAPSHOT1_ID="${LAST_CP_SNAPSHOT_ID}"

# B -> snapshot 2
fio_write_generation "${LIVE_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}"
fio_verify_generation "${LIVE_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}" B
B_HASH="$(region_sha256 "${LIVE_DEVICE}")"
[[ "${A_HASH}" != "${B_HASH}" ]] || fail "A and B are byte-identical"

snapshot_create "${SNAPSHOT2_NAME}"
SNAPSHOT2_CREATED=1
SNAP2_CLI_ID="${LAST_CLI_SNAPSHOT_ID}"
wait_for_snapshot_applied "${SNAPSHOT2_NAME}" "${SNAP2_CLI_ID}" snapshot2
SNAPSHOT2_ID="${LAST_CP_SNAPSHOT_ID}"

[[ "${SNAPSHOT1_ID}" =~ ^[1-9][0-9]*$ ]] || fail "invalid snapshot1 ID: ${SNAPSHOT1_ID}"
[[ "${SNAPSHOT2_ID}" =~ ^[1-9][0-9]*$ ]] || fail "invalid snapshot2 ID: ${SNAPSHOT2_ID}"
[[ "${SNAPSHOT2_ID}" != "${SNAPSHOT1_ID}" ]] || fail "snapshot IDs were reused"
(( SNAPSHOT2_ID > SNAPSHOT1_ID )) || fail "snapshot2 ID is not greater than snapshot1 ID"

# C becomes the live generation.
fio_write_generation "${LIVE_DEVICE}" data-C "${SEED_C}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_C}"
fio_verify_generation "${LIVE_DEVICE}" data-C "${SEED_C}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_C}" C
C_HASH="$(region_sha256 "${LIVE_DEVICE}")"

[[ "${A_HASH}" != "${C_HASH}" ]] || fail "A and C are byte-identical"
[[ "${B_HASH}" != "${C_HASH}" ]] || fail "B and C are byte-identical"
LIVE_HASH="$(region_sha256 "${LIVE_DEVICE}")"
[[ "${LIVE_HASH}" == "${C_HASH}" ]] || fail "live view is not generation C"

# Snapshot 1 must preserve A.
start_snapshot_mount "${SNAPSHOT1_NAME}" snapshot1
fio_verify_generation "${RO_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" snapshot1-A
SNAP1_HASH="$(region_sha256 "${RO_DEVICE}")"
[[ "${SNAP1_HASH}" == "${A_HASH}" ]] || fail "snapshot1 does not contain A"
[[ "${SNAP1_HASH}" != "${B_HASH}" && "${SNAP1_HASH}" != "${C_HASH}" ]] || fail "snapshot1 exposes a newer generation"
stop_snapshot_mount

# Snapshot 2 must preserve B.
start_snapshot_mount "${SNAPSHOT2_NAME}" snapshot2
fio_verify_generation "${RO_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}" snapshot2-B
SNAP2_HASH="$(region_sha256 "${RO_DEVICE}")"
[[ "${SNAP2_HASH}" == "${B_HASH}" ]] || fail "snapshot2 does not contain B"
[[ "${SNAP2_HASH}" != "${A_HASH}" && "${SNAP2_HASH}" != "${C_HASH}" ]] || fail "snapshot2 exposes the wrong generation"
stop_snapshot_mount

# Re-read snapshot 1 after snapshot 2 exists to prove its boundary did not move.
start_snapshot_mount "${SNAPSHOT1_NAME}" snapshot1-recheck
fio_verify_generation "${RO_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" snapshot1-recheck-A
SNAP1_RECHECK_HASH="$(region_sha256 "${RO_DEVICE}")"
[[ "${SNAP1_RECHECK_HASH}" == "${A_HASH}" ]] || fail "snapshot1 changed after snapshot2 was created"

TEST_PASSED=1
cat <<EOF_SUMMARY
PASS: multiple snapshots preserve multiple versions
  snapshot 1: ${SNAPSHOT1_NAME} id=${SNAPSHOT1_ID} -> A (${A_HASH})
  snapshot 2: ${SNAPSHOT2_NAME} id=${SNAPSHOT2_ID} -> B (${B_HASH})
  live:       ${LIVE_DEVICE} -> C (${C_HASH})
  snapshot 1 recheck: ${SNAP1_RECHECK_HASH}
EOF_SUMMARY
