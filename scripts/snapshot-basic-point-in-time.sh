#!/usr/bin/env bash
# Test 1: Basic snapshot create + point-in-time correctness.

set -Eeuo pipefail

: "${VDEV_UUID:?VDEV_UUID must be set}"

LIVE_DEVICE="${LIVE_DEVICE:-/dev/ublkb0}"
SNAPSHOT_NAME="${SNAPSHOT_NAME:-snapshot-basic-$$}"
NIOVA_SNAPSHOT="${NIOVA_SNAPSHOT:-scripts/niova-snapshot}"
NIOVA_UBLK="${NIOVA_UBLK:-niova-ublk}"
TEST_LOGDIR="${TEST_LOGDIR:-/tmp/snapshot-basic-point-in-time}"
SNAPSHOT_TIMEOUT="${SNAPSHOT_TIMEOUT:-30}"
UBLK_TIMEOUT="${UBLK_TIMEOUT:-30}"
DELETE_SNAPSHOT_ON_SUCCESS="${DELETE_SNAPSHOT_ON_SUCCESS:-1}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_TEST_HELPERS="${SNAPSHOT_TEST_HELPERS:-${SCRIPT_DIR}/snapshot-test-helpers.sh}"
[[ -r "${SNAPSHOT_TEST_HELPERS}" ]] || { echo "Missing helper: ${SNAPSHOT_TEST_HELPERS}" >&2; exit 1; }
# shellcheck source=snapshot-test-helpers.sh
source "${SNAPSHOT_TEST_HELPERS}"

SEED_A=1001
SEED_B=1002
PATTERN_A=0xAAAAAAAA
PATTERN_B=0xBBBBBBBB

TEST_PASSED=0
SNAPSHOT_CREATED=0
SNAPSHOT_ID=""

cleanup()
{
    local rc=$?
    stop_snapshot_mount || true

    if [[ "${TEST_PASSED}" -eq 1 && "${DELETE_SNAPSHOT_ON_SUCCESS}" -eq 1 && "${SNAPSHOT_CREATED}" -eq 1 ]]; then
        snapshot_delete "${SNAPSHOT_NAME}" || true
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

log "Test region: ${LIVE_DEVICE}, offset=${SNAP_OFFSET}, size=${SNAP_SIZE}, VBLKs 262144..294911"

# 1-2. Write and verify A.
fio_write_generation "${LIVE_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}"
fio_verify_generation "${LIVE_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" pre-snapshot
A_HASH="$(region_sha256 "${LIVE_DEVICE}")"
[[ "${A_HASH}" =~ ^[0-9a-f]{64}$ ]] || fail "invalid SHA-256 for generation A: ${A_HASH}"

# 3-5. Create snapshot and verify CP state.
snapshot_create "${SNAPSHOT_NAME}"
SNAPSHOT_CREATED=1
SNAPSHOT_ID="${LAST_CLI_SNAPSHOT_ID}"
cp_login
wait_for_snapshot_applied "${SNAPSHOT_NAME}" "${SNAPSHOT_ID}" basic
SNAPSHOT_ID="${LAST_CP_SNAPSHOT_ID}"
[[ "${SNAPSHOT_ID}" =~ ^[1-9][0-9]*$ ]] || fail "invalid snapshot ID: ${SNAPSHOT_ID}"

# 6-8. Overwrite same VBLKs with B and verify live=B.
fio_write_generation "${LIVE_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}"
fio_verify_generation "${LIVE_DEVICE}" data-B "${SEED_B}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_B}" live-B
B_HASH="$(region_sha256 "${LIVE_DEVICE}")"
[[ "${B_HASH}" =~ ^[0-9a-f]{64}$ ]] || fail "invalid SHA-256 for generation B: ${B_HASH}"
[[ "${A_HASH}" != "${B_HASH}" ]] || fail "generation B is byte-identical to A"

LIVE_HASH="$(region_sha256 "${LIVE_DEVICE}")"
[[ "${LIVE_HASH}" == "${B_HASH}" ]] || fail "live view is not generation B"
[[ "${LIVE_HASH}" != "${A_HASH}" ]] || fail "live view still contains generation A"

# 9-10. Mount snapshot and verify snapshot=A.
start_snapshot_mount "${SNAPSHOT_NAME}" basic
fio_verify_generation "${RO_DEVICE}" data-A "${SEED_A}" "${SNAP_OFFSET}" "${SNAP_SIZE}" "${SNAP_BS}" "${PATTERN_A}" snapshot-A
SNAPSHOT_HASH="$(region_sha256 "${RO_DEVICE}")"
[[ "${SNAPSHOT_HASH}" == "${A_HASH}" ]] || fail "snapshot does not contain generation A"
[[ "${SNAPSHOT_HASH}" != "${B_HASH}" ]] || fail "snapshot incorrectly exposes generation B"

TEST_PASSED=1
cat <<EOF_SUMMARY
PASS: basic snapshot point-in-time test
  vdev:            ${VDEV_UUID}
  snapshot:        ${SNAPSHOT_NAME}
  snapshot id:     ${SNAPSHOT_ID}
  live device:     ${LIVE_DEVICE}
  snapshot device: ${RO_DEVICE}
  generation A:    ${A_HASH}
  generation B:    ${B_HASH}
  snapshot hash:   ${SNAPSHOT_HASH}
  CP state:        applied
EOF_SUMMARY
