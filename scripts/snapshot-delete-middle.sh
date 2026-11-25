#!/usr/bin/env bash
# SR-03: Delete the middle of a snapshot chain.
#
# Create S1, S2 and S3.
# Delete S2 and verify that S1, S3 and the live RW device
# retain their expected data.

set -euo pipefail

: "${VDEV_UUID:?VDEV_UUID is required}"
: "${LIVE_DEVICE:?LIVE_DEVICE is required}"
: "${NIOVA_SNAPSHOT:?NIOVA_SNAPSHOT is required}"
: "${NIOVA_UBLK:?NIOVA_UBLK is required}"
: "${TEST_LOGDIR:?TEST_LOGDIR is required}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_TEST_HELPERS="${SNAPSHOT_TEST_HELPERS:-${SCRIPT_DIR}/snapshot_helper.sh}"

source "${SNAPSHOT_TEST_HELPERS}"

SNAPSHOT_S1="${SNAPSHOT_S1:-sr03-s1}"
SNAPSHOT_S2="${SNAPSHOT_S2:-sr03-s2}"
SNAPSHOT_S3="${SNAPSHOT_S3:-sr03-s3}"

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-4000}"
MYSQL_USER="${MYSQL_USER:-root}"

DELETE_RETRIES="${DELETE_RETRIES:-90}"

PATTERN_BASE="${TEST_LOGDIR}/baseline.bin"
PATTERN_A="${TEST_LOGDIR}/A.bin"
PATTERN_B="${TEST_LOGDIR}/B.bin"
PATTERN_C="${TEST_LOGDIR}/C.bin"

REGION_1=0
REGION_2=64
REGION_3=128

DB_SCHEMA=""
S1_ID=""
S2_ID=""
S3_ID=""

trap stop_snapshot_mount EXIT

require_command "${NIOVA_SNAPSHOT}"
require_command "${NIOVA_UBLK}"
require_command mysql
require_command python3
require_command curl
require_command dd
require_command md5sum
require_block_device "${LIVE_DEVICE}"

# ------------------------------------------------------------
# Helper: Execute a TiDB query.
# ------------------------------------------------------------

mysql_query()
{
    mysql \
        --protocol=TCP \
        --host="${MYSQL_HOST}" \
        --port="${MYSQL_PORT}" \
        --user="${MYSQL_USER}" \
        --connect-timeout=5 \
        --batch \
        --skip-column-names \
        --raw \
        "$@"
}

db_query()
{
    mysql_query \
        --database="${DB_SCHEMA}" \
        --execute="$1"
}

# ------------------------------------------------------------
# Helper: Resolve the tenant schema containing the VDEV.
# ------------------------------------------------------------

resolve_db_schema()
{
    local schemas schema count
    local matches=()

    [[ "${VDEV_UUID}" =~ ^[0-9a-fA-F-]{36}$ ]] ||
        fail "Invalid VDEV UUID: ${VDEV_UUID}"

    schemas="$(
        mysql_query --execute="
            SELECT TABLE_SCHEMA
            FROM information_schema.TABLES
            WHERE TABLE_NAME = 'snapshot_delete_event';
        "
    )"

    [[ -n "${schemas}" ]] ||
        fail "No snapshot deletion schema found"

    while IFS= read -r schema; do
        [[ -n "${schema}" ]] || continue

        count="$(
            mysql_query \
                --database="${schema}" \
                --execute="
                    SELECT COUNT(*)
                    FROM vdev
                    WHERE id = '${VDEV_UUID}';
                "
        )"

        if [[ "${count}" == "1" ]]; then
            matches+=("${schema}")
        fi

    done <<< "${schemas}"

    [[ "${#matches[@]}" -eq 1 ]] ||
        fail "Expected exactly one tenant schema for ${VDEV_UUID}"

    DB_SCHEMA="${matches[0]}"

    log "Resolved TiDB tenant schema: ${DB_SCHEMA}"
}

# ------------------------------------------------------------
# Helper: Write a 1 MiB pattern at a given offset.
# ------------------------------------------------------------

write_pattern()
{
    local source_file="$1"
    local offset="$2"

    sudo dd \
        if="${source_file}" \
        of="${LIVE_DEVICE}" \
        bs=1M \
        seek="${offset}" \
        count=1 \
        oflag=direct \
        conv=notrunc,fsync \
        status=none

    log "Written $(basename "${source_file}") at offset ${offset} MiB"
}

# ------------------------------------------------------------
# Helper: Calculate MD5 of a 1 MiB device region.
# ------------------------------------------------------------

region_md5()
{
    local device="$1"
    local offset="$2"

    require_block_device "${device}"

    sudo dd \
        if="${device}" \
        bs=1M \
        skip="${offset}" \
        count=1 \
        iflag=direct,fullblock \
        status=none |
        md5sum |
        awk '{print $1}'
}

# ------------------------------------------------------------
# Helper: Verify a single region.
# ------------------------------------------------------------

verify_region()
{
    local device="$1"
    local offset="$2"
    local expected_hash="$3"
    local tag="$4"
    local actual_hash

    actual_hash="$(region_md5 "${device}" "${offset}")"

    log "${tag}: offset=${offset} MiB"
    log "${tag}: expected=${expected_hash}"
    log "${tag}: actual=${actual_hash}"

    [[ "${actual_hash}" == "${expected_hash}" ]] ||
        fail "${tag}: Data mismatch at offset ${offset} MiB"
}

# ------------------------------------------------------------
# Helper: Verify all three regions.
# ------------------------------------------------------------

verify_regions()
{
    local device="$1"
    local expected_region1="$2"
    local expected_region2="$3"
    local expected_region3="$4"
    local tag="$5"

    verify_region \
        "${device}" "${REGION_1}" "${expected_region1}" "${tag}"

    verify_region \
        "${device}" "${REGION_2}" "${expected_region2}" "${tag}"

    verify_region \
        "${device}" "${REGION_3}" "${expected_region3}" "${tag}"

    log "${tag}: All three regions verified successfully"
}

# ------------------------------------------------------------
# Helper: Count snapshot rows for a specific snapshot ID.
# ------------------------------------------------------------

snapshot_row_count()
{
    local snapshot_id="$1"

    db_query "
        SELECT COUNT(*)
        FROM snapshot
        WHERE vdev_id = '${VDEV_UUID}'
          AND snapshot_id = ${snapshot_id};
    "
}

# ------------------------------------------------------------
# Helper: Count remaining deletion tracking rows for S2.
# ------------------------------------------------------------

delete_tracking_count()
{
    db_query "
        SELECT COUNT(*)
        FROM snapshot_delete_event
        WHERE vdev_id = '${VDEV_UUID}'
          AND snapshot_id = ${S2_ID};
    "
}

# ------------------------------------------------------------
# Helper: Retrieve Action-6 events for S2.
# ------------------------------------------------------------

get_delete_events()
{
    db_query "
        SELECT
            nisd_id,
            COUNT(*),
            SUM(expire_at IS NOT NULL),
            SUM(
                JSON_TYPE(
                    JSON_EXTRACT(params, '$.snapshot_id')
                ) IN ('INTEGER', 'UNSIGNED INTEGER')
            )
        FROM nisd_event
        WHERE action = 6
          AND JSON_UNQUOTE(
                JSON_EXTRACT(params, '$.vdev_id')
              ) = '${VDEV_UUID}'
          AND JSON_UNQUOTE(
                JSON_EXTRACT(params, '$.snapshot_id')
              ) = '${S2_ID}'
        GROUP BY nisd_id
        ORDER BY nisd_id;
    "
}

# ------------------------------------------------------------
# Helper: Wait for actual asynchronous deletion completion.
# ------------------------------------------------------------

wait_for_snapshot_delete()
{
    local snapshot_rows tracking_rows
    local event_rows actual_nisds
    local all_acked valid_payloads
    local nisd_id event_count ack_count numeric_count

    for ((attempt=1; attempt<=DELETE_RETRIES; attempt++)); do

        snapshot_rows="$(snapshot_row_count "${S2_ID}")"
        tracking_rows="$(delete_tracking_count)"
        event_rows="$(get_delete_events)"

        all_acked=1
        valid_payloads=1

        actual_nisds="$(
            printf '%s\n' "${event_rows}" | cut -f1
        )"

        if [[ -n "${event_rows}" ]]; then

            printf '%s\n' "${event_rows}" \
                >> "${TEST_LOGDIR}/s2-delete-events.log"

            while IFS=$'\t' read -r \
                nisd_id event_count ack_count numeric_count; do

                [[ "${event_count}" == "1" ]] ||
                    fail "Duplicate Action-6 events for ${nisd_id}"

                [[ "${ack_count}" == "1" ]] ||
                    all_acked=0

                [[ "${numeric_count}" == "1" ]] ||
                    valid_payloads=0

            done <<< "${event_rows}"

        else
            all_acked=0
            valid_payloads=0
        fi

        log "S2 deletion: snapshot_rows=${snapshot_rows}, tracking_rows=${tracking_rows}, all_acked=${all_acked}"

        if [[ "${snapshot_rows}" == "0" &&
              "${tracking_rows}" == "0" &&
              "${actual_nisds}" == "${EXPECTED_NISDS}" &&
              "${all_acked}" == "1" &&
              "${valid_payloads}" == "1" ]]; then

            printf '%s\n' "${event_rows}" \
                > "${TEST_LOGDIR}/s2-final-delete-events.txt"

            log "S2 snapshot row removed"
            log "S2 deletion tracking rows removed"
            log "All expected Action-6 events acknowledged"

            return 0
        fi

        sleep 1
    done

    fail "S2 asynchronous deletion did not complete"
}

# ------------------------------------------------------------
# Helper: Verify S2 is not found through CP.
# ------------------------------------------------------------

verify_snapshot_not_found()
{
    local selector="$1"
    local value="$2"
    local response_file http_status cp_status

    response_file="${TEST_LOGDIR}/s2-cp-lookup-${selector}.json"

    http_status="$(
        curl --silent --show-error \
            --output "${response_file}" \
            --write-out '%{http_code}' \
            --header "Authorization: Bearer ${CP_TOKEN}" \
            --get \
            --data-urlencode "vdev_id=${VDEV_UUID}" \
            --data-urlencode "${selector}=${value}" \
            "${MDSVC_API_URL}/api/snapshot"
    )"

    [[ "${http_status}" == "404" ]] ||
        fail "Expected HTTP 404, got ${http_status}"

    cp_status="$(json_get_field status < "${response_file}")"

    [[ "${cp_status}" == "300" ]] ||
        fail "Expected StatusNotFound=300, got ${cp_status}"

    log "S2 lookup by ${selector}: not found"
}

# ------------------------------------------------------------
# 1. Resolve database schema and expected NISDs.
# ------------------------------------------------------------

resolve_db_schema

EXPECTED_NISDS="$(
    db_query "
        SELECT DISTINCT nisd_id
        FROM vdev_chunk
        WHERE vdev_id = '${VDEV_UUID}'
        ORDER BY nisd_id;
    "
)"

[[ -n "${EXPECTED_NISDS}" ]] ||
    fail "No NISDs hold chunks for VDEV ${VDEV_UUID}"

printf '%s\n' "${EXPECTED_NISDS}" \
    > "${TEST_LOGDIR}/expected-nisds.txt"

# ------------------------------------------------------------
# 2. Generate baseline and three distinct data patterns.
# ------------------------------------------------------------

python3 - \
    "${PATTERN_BASE}" \
    "${PATTERN_A}" \
    "${PATTERN_B}" \
    "${PATTERN_C}" <<'PY'
import sys

for path, pattern in zip(sys.argv[1:], (b"X", b"A", b"B", b"C")):
    with open(path, "wb") as f:
        f.write(pattern * 1024 * 1024)
PY

BASE_HASH="$(md5sum "${PATTERN_BASE}" | awk '{print $1}')"
A_HASH="$(md5sum "${PATTERN_A}" | awk '{print $1}')"
B_HASH="$(md5sum "${PATTERN_B}" | awk '{print $1}')"
C_HASH="$(md5sum "${PATTERN_C}" | awk '{print $1}')"

log "Baseline MD5=${BASE_HASH}"
log "Pattern A MD5=${A_HASH}"
log "Pattern B MD5=${B_HASH}"
log "Pattern C MD5=${C_HASH}"

# ------------------------------------------------------------
# 3. Initialize all regions with baseline data.
#
# This guarantees known data in regions 2 and 3 before S1.
# ------------------------------------------------------------

write_pattern "${PATTERN_BASE}" "${REGION_1}"
write_pattern "${PATTERN_BASE}" "${REGION_2}"
write_pattern "${PATTERN_BASE}" "${REGION_3}"

verify_regions \
    "${LIVE_DEVICE}" \
    "${BASE_HASH}" \
    "${BASE_HASH}" \
    "${BASE_HASH}" \
    "Initial baseline"

# ------------------------------------------------------------
# 4. Write region 1 and create S1.
#
# Expected S1: A, BASE, BASE
# ------------------------------------------------------------

write_pattern "${PATTERN_A}" "${REGION_1}"

verify_regions \
    "${LIVE_DEVICE}" \
    "${A_HASH}" \
    "${BASE_HASH}" \
    "${BASE_HASH}" \
    "Before S1 creation"

snapshot_create "${SNAPSHOT_S1}"

cp_login

wait_for_snapshot_applied \
    "${SNAPSHOT_S1}" \
    "${LAST_CLI_SNAPSHOT_ID}" \
    s1

S1_ID="${LAST_CP_SNAPSHOT_ID}"

log "S1 created: id=${S1_ID}"

printf '%s\n' "${S1_ID}" > "${TEST_LOGDIR}/s1-id.txt"

# ------------------------------------------------------------
# 5. Write region 2 and create S2.
#
# Expected S2: A, B, BASE
# ------------------------------------------------------------

write_pattern "${PATTERN_B}" "${REGION_2}"

verify_regions \
    "${LIVE_DEVICE}" \
    "${A_HASH}" \
    "${B_HASH}" \
    "${BASE_HASH}" \
    "Before S2 creation"

snapshot_create "${SNAPSHOT_S2}"

cp_login

wait_for_snapshot_applied \
    "${SNAPSHOT_S2}" \
    "${LAST_CLI_SNAPSHOT_ID}" \
    s2

S2_ID="${LAST_CP_SNAPSHOT_ID}"

log "S2 created: id=${S2_ID}"

printf '%s\n' "${S2_ID}" > "${TEST_LOGDIR}/s2-id.txt"

# ------------------------------------------------------------
# 6. Write region 3 and create S3.
#
# Expected S3: A, B, C
# ------------------------------------------------------------

write_pattern "${PATTERN_C}" "${REGION_3}"

verify_regions \
    "${LIVE_DEVICE}" \
    "${A_HASH}" \
    "${B_HASH}" \
    "${C_HASH}" \
    "Before S3 creation"

snapshot_create "${SNAPSHOT_S3}"

cp_login

wait_for_snapshot_applied \
    "${SNAPSHOT_S3}" \
    "${LAST_CLI_SNAPSHOT_ID}" \
    s3

S3_ID="${LAST_CP_SNAPSHOT_ID}"

log "S3 created: id=${S3_ID}"

printf '%s\n' "${S3_ID}" > "${TEST_LOGDIR}/s3-id.txt"

[[ "${S1_ID}" -lt "${S2_ID}" &&
   "${S2_ID}" -lt "${S3_ID}" ]] ||
    fail "Snapshot IDs are not monotonically increasing"

# ------------------------------------------------------------
# 7. Verify all three snapshots exist before deletion.
# ------------------------------------------------------------

for snapshot_id in "${S1_ID}" "${S2_ID}" "${S3_ID}"; do

    [[ "$(snapshot_row_count "${snapshot_id}")" == "1" ]] ||
        fail "Snapshot ${snapshot_id} missing before deletion"

done

[[ "$(delete_tracking_count)" == "0" ]] ||
    fail "Unexpected S2 deletion tracking rows before deletion"

[[ -z "$(get_delete_events)" ]] ||
    fail "Unexpected S2 Action-6 events before deletion"

# ------------------------------------------------------------
# 8. Verify S1 and S3 before deleting S2.
#
# This establishes that the retained snapshots are correct
# before testing the effect of the deletion operation.
# ------------------------------------------------------------

start_snapshot_mount "${SNAPSHOT_S1}" s1-before-delete

[[ "${RO_DEVICE}" != "${LIVE_DEVICE}" ]] ||
    fail "S1 snapshot mount returned the live device"

verify_regions \
    "${RO_DEVICE}" \
    "${A_HASH}" \
    "${BASE_HASH}" \
    "${BASE_HASH}" \
    "S1 before deletion"

stop_snapshot_mount

start_snapshot_mount "${SNAPSHOT_S3}" s3-before-delete

[[ "${RO_DEVICE}" != "${LIVE_DEVICE}" ]] ||
    fail "S3 snapshot mount returned the live device"

verify_regions \
    "${RO_DEVICE}" \
    "${A_HASH}" \
    "${B_HASH}" \
    "${C_HASH}" \
    "S3 before deletion"

stop_snapshot_mount

# ------------------------------------------------------------
# 9. Delete the middle snapshot S2 by ID.
# ------------------------------------------------------------

log "Deleting middle snapshot S2: id=${S2_ID}"

sudo -E "${NIOVA_SNAPSHOT}" \
    --vdev "${VDEV_UUID}" \
    --op delete \
    --snapshot-id "${S2_ID}" \
    --keep \
    2>&1 | tee "${TEST_LOGDIR}/s2-delete.log"

# ------------------------------------------------------------
# 10. Wait for complete asynchronous deletion.
# ------------------------------------------------------------

wait_for_snapshot_delete

# ------------------------------------------------------------
# 11. Verify S2 lookup fails by ID and name.
# ------------------------------------------------------------

if sudo -E "${NIOVA_SNAPSHOT}" \
    --vdev "${VDEV_UUID}" \
    --op lookup \
    --snapshot-id "${S2_ID}" \
    > "${TEST_LOGDIR}/s2-lookup-id.log" 2>&1; then

    fail "Lookup unexpectedly succeeded for deleted S2 ID"
fi

if snapshot_lookup "${SNAPSHOT_S2}" \
    > "${TEST_LOGDIR}/s2-lookup-name.log" 2>&1; then

    fail "Lookup unexpectedly succeeded for deleted S2 name"
fi

cp_login

verify_snapshot_not_found snapshot_id "${S2_ID}"
verify_snapshot_not_found name "${SNAPSHOT_S2}"

# ------------------------------------------------------------
# 12. Verify S1 and S3 remain accessible and applied.
# ------------------------------------------------------------

snapshot_lookup "${SNAPSHOT_S1}" \
    2>&1 | tee "${TEST_LOGDIR}/s1-lookup.log"

wait_for_snapshot_applied \
    "${SNAPSHOT_S1}" \
    "${S1_ID}" \
    s1-after-delete

snapshot_lookup "${SNAPSHOT_S3}" \
    2>&1 | tee "${TEST_LOGDIR}/s3-lookup.log"

wait_for_snapshot_applied \
    "${SNAPSHOT_S3}" \
    "${S3_ID}" \
    s3-after-delete

# ------------------------------------------------------------
# 13. Mount S1 read-only after deleting S2.
#
# Expected:
#   Region 1: A
#   Region 2: BASE
#   Region 3: BASE
# ------------------------------------------------------------

start_snapshot_mount "${SNAPSHOT_S1}" s1-after-delete

[[ "${RO_DEVICE}" != "${LIVE_DEVICE}" ]] ||
    fail "S1 snapshot mount returned the live device"

verify_regions \
    "${RO_DEVICE}" \
    "${A_HASH}" \
    "${BASE_HASH}" \
    "${BASE_HASH}" \
    "S1 after S2 deletion"

stop_snapshot_mount

# ------------------------------------------------------------
# 14. Mount S3 read-only after deleting S2.
#
# Expected:
#   Region 1: A
#   Region 2: B
#   Region 3: C
# ------------------------------------------------------------

start_snapshot_mount "${SNAPSHOT_S3}" s3-after-delete

[[ "${RO_DEVICE}" != "${LIVE_DEVICE}" ]] ||
    fail "S3 snapshot mount returned the live device"

verify_regions \
    "${RO_DEVICE}" \
    "${A_HASH}" \
    "${B_HASH}" \
    "${C_HASH}" \
    "S3 after S2 deletion"

stop_snapshot_mount

# ------------------------------------------------------------
# 15. Verify the live RW device remains unchanged.
#
# Expected: A, B, C
# ------------------------------------------------------------

verify_regions \
    "${LIVE_DEVICE}" \
    "${A_HASH}" \
    "${B_HASH}" \
    "${C_HASH}" \
    "Live RW device after S2 deletion"

# ------------------------------------------------------------
# 16. Verify final database state.
# ------------------------------------------------------------

[[ "$(snapshot_row_count "${S1_ID}")" == "1" ]] ||
    fail "Older snapshot S1 was incorrectly removed"

[[ "$(snapshot_row_count "${S2_ID}")" == "0" ]] ||
    fail "Middle snapshot S2 still exists"

[[ "$(snapshot_row_count "${S3_ID}")" == "1" ]] ||
    fail "Newer snapshot S3 was incorrectly removed"

[[ "$(delete_tracking_count)" == "0" ]] ||
    fail "S2 deletion tracking rows still exist"

# ------------------------------------------------------------
# 17. Report success.
# ------------------------------------------------------------

cat <<EOF
PASS: snapshot delete middle test
  vdev:                   ${VDEV_UUID}
  older snapshot:         ${SNAPSHOT_S1}
  older snapshot ID:      ${S1_ID}
  deleted snapshot:       ${SNAPSHOT_S2}
  deleted snapshot ID:    ${S2_ID}
  newer snapshot:         ${SNAPSHOT_S3}
  newer snapshot ID:      ${S3_ID}
  live device:            ${LIVE_DEVICE}
  baseline MD5:           ${BASE_HASH}
  pattern A MD5:          ${A_HASH}
  pattern B MD5:          ${B_HASH}
  pattern C MD5:          ${C_HASH}
  S1 state:               applied
  S2 lookup by ID:        not found
  S2 lookup by name:      not found
  S3 state:               applied
  S2 database rows:       0
  S2 delete tracking:     0
  Action-6 events:        verified and acknowledged
  S1 regions:             A, BASE, BASE
  S3 regions:             A, B, C
  Live RW regions:        A, B, C
EOF