#!/usr/bin/env bash
# SR-01: Basic delete, keep the newer snapshot.
#
# Verify that deleting S1 does not affect S2 or the live RW device.

set -euo pipefail

: "${VDEV_UUID:?VDEV_UUID is required}"
: "${LIVE_DEVICE:?LIVE_DEVICE is required}"
: "${NIOVA_SNAPSHOT:?NIOVA_SNAPSHOT is required}"
: "${NIOVA_UBLK:?NIOVA_UBLK is required}"
: "${TEST_LOGDIR:?TEST_LOGDIR is required}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_TEST_HELPERS="${SNAPSHOT_TEST_HELPERS:-${SCRIPT_DIR}/snapshot_helper.sh}"

source "${SNAPSHOT_TEST_HELPERS}"

SNAPSHOT_S1="${SNAPSHOT_S1:-s1}"
SNAPSHOT_S2="${SNAPSHOT_S2:-s2}"

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-4000}"
MYSQL_USER="${MYSQL_USER:-root}"

DELETE_RETRIES="${DELETE_RETRIES:-90}"

PATTERN_A="${TEST_LOGDIR}/A.bin"
PATTERN_B="${TEST_LOGDIR}/B.bin"

S1_ID=""
S2_ID=""
DB_SCHEMA=""

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
# Helper: Resolve the tenant schema containing the test VDEV.
# Avoid hardcoding the tenant database name.
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
# Helper: Write a 1 MiB pattern at the specified offset.
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

    log "Written ${source_file} at offset ${offset} MiB"
}

# ------------------------------------------------------------
# Helper: Calculate MD5 of a 1 MiB block-device region.
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
# Helper: Verify patterns A and B on a device.
# ------------------------------------------------------------

verify_patterns()
{
    local device="$1"
    local tag="$2"
    local actual_a actual_b

    actual_a="$(region_md5 "${device}" 0)"
    actual_b="$(region_md5 "${device}" 64)"

    log "${tag}: Pattern A MD5=${actual_a}"
    log "${tag}: Pattern B MD5=${actual_b}"

    [[ "${actual_a}" == "${A_HASH}" ]] ||
        fail "${tag}: Pattern A mismatch"

    [[ "${actual_b}" == "${B_HASH}" ]] ||
        fail "${tag}: Pattern B mismatch"

    log "${tag}: Both patterns verified successfully"
}

# ------------------------------------------------------------
# Helper: Verify deleted snapshot lookup through CP.
#
# HTTP 404 and StatusNotFound=300 distinguish a missing
# snapshot from authentication or other API failures.
# ------------------------------------------------------------

verify_snapshot_not_found()
{
    local selector="$1"
    local value="$2"
    local response_file
    local http_status cp_status

    response_file="${TEST_LOGDIR}/s1-cp-lookup-${selector}.json"

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
        fail "Expected HTTP 404 for ${selector}=${value}, got ${http_status}"

    cp_status="$(json_get_field status < "${response_file}")"

    [[ "${cp_status}" == "300" ]] ||
        fail "Expected StatusNotFound=300, got ${cp_status}"

    log "CP lookup by ${selector}=${value}: not found"
}

# ------------------------------------------------------------
# Helper: Count remaining S1 snapshot rows.
# ------------------------------------------------------------

snapshot_row_count()
{
    db_query "
        SELECT COUNT(*)
        FROM snapshot
        WHERE vdev_id = '${VDEV_UUID}'
          AND snapshot_id = ${S1_ID};
    "
}

# ------------------------------------------------------------
# Helper: Count remaining deletion tracking rows for S1.
# ------------------------------------------------------------

delete_tracking_count()
{
    db_query "
        SELECT COUNT(*)
        FROM snapshot_delete_event
        WHERE vdev_id = '${VDEV_UUID}'
          AND snapshot_id = ${S1_ID};
    "
}

# ------------------------------------------------------------
# Helper: Retrieve Action-6 events for the deleted snapshot.
#
# Columns:
#   nisd_id
#   event count
#   acknowledged event count
#   valid numeric snapshot_id count
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
              ) = '${S1_ID}'
        GROUP BY nisd_id
        ORDER BY nisd_id;
    "
}

# ------------------------------------------------------------
# Helper: Wait for complete asynchronous snapshot deletion.
#
# Success requires:
#   - one Action-6 event per expected NISD;
#   - every event acknowledged;
#   - snapshot_delete_event rows removed;
#   - snapshot row removed.
#
# CP removes snapshot tracking rows before marking events
# expired, so all conditions must be polled together.
# ------------------------------------------------------------

wait_for_snapshot_delete()
{
    local snapshot_rows tracking_rows
    local event_rows actual_nisds
    local all_acked valid_payloads
    local nisd_id event_count ack_count numeric_count

    for ((attempt=1; attempt<=DELETE_RETRIES; attempt++)); do

        snapshot_rows="$(snapshot_row_count)"
        tracking_rows="$(delete_tracking_count)"
        event_rows="$(get_delete_events)"

        all_acked=1
        valid_payloads=1

        actual_nisds="$(
            printf '%s\n' "${event_rows}" | cut -f1
        )"

        if [[ -n "${event_rows}" ]]; then

            printf '%s\n' "${event_rows}" \
                >> "${TEST_LOGDIR}/s1-delete-events.log"

            while IFS=$'\t' read -r \
                nisd_id event_count ack_count numeric_count; do

                [[ "${event_count}" == "1" ]] ||
                    fail "Duplicate Action-6 events for NISD ${nisd_id}"

                if [[ "${ack_count}" != "1" ]]; then
                    all_acked=0
                fi

                if [[ "${numeric_count}" != "1" ]]; then
                    valid_payloads=0
                fi

            done <<< "${event_rows}"

        else
            all_acked=0
            valid_payloads=0
        fi

        log "Deletion status: snapshot_rows=${snapshot_rows}, tracking_rows=${tracking_rows}, all_acked=${all_acked}"

        if [[ "${snapshot_rows}" == "0" &&
              "${tracking_rows}" == "0" &&
              "${actual_nisds}" == "${EXPECTED_NISDS}" &&
              "${all_acked}" == "1" &&
              "${valid_payloads}" == "1" ]]; then

            printf '%s\n' "${event_rows}" \
                > "${TEST_LOGDIR}/s1-final-delete-events.txt"

            log "One Action-6 event verified per expected NISD"
            log "All deletion events acknowledged"
            log "Snapshot deletion tracking rows removed"
            log "Snapshot S1 database row removed"

            return 0
        fi

        sleep 1
    done

    fail "Snapshot deletion or Action-6 event verification did not complete"
}

# ------------------------------------------------------------
# 1. Resolve database schema and identify expected NISDs.
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

log "Expected Action-6 recipients:"
printf '%s\n' "${EXPECTED_NISDS}"

# ------------------------------------------------------------
# 2. Generate deterministic patterns A and B.
# ------------------------------------------------------------

python3 - "${PATTERN_A}" "${PATTERN_B}" <<'PY'
import sys

with open(sys.argv[1], "wb") as f:
    f.write(b"A" * 1024 * 1024)

with open(sys.argv[2], "wb") as f:
    f.write(b"B" * 1024 * 1024)
PY

A_HASH="$(md5sum "${PATTERN_A}" | awk '{print $1}')"
B_HASH="$(md5sum "${PATTERN_B}" | awk '{print $1}')"

[[ "${A_HASH}" != "${B_HASH}" ]] ||
    fail "Patterns A and B must be different"

log "Source pattern A MD5=${A_HASH}"
log "Source pattern B MD5=${B_HASH}"

# ------------------------------------------------------------
# 3. Write pattern A at offset 0.
# ------------------------------------------------------------

write_pattern "${PATTERN_A}" 0

[[ "$(region_md5 "${LIVE_DEVICE}" 0)" == "${A_HASH}" ]] ||
    fail "Initial pattern A verification failed"

# ------------------------------------------------------------
# 4. Create S1 and wait until applied.
# ------------------------------------------------------------

snapshot_create "${SNAPSHOT_S1}"

cp_login

wait_for_snapshot_applied \
    "${SNAPSHOT_S1}" \
    "${LAST_CLI_SNAPSHOT_ID}" \
    s1

S1_ID="${LAST_CP_SNAPSHOT_ID}"

log "S1 created: name=${SNAPSHOT_S1}, id=${S1_ID}"

# ------------------------------------------------------------
# 5. Write pattern B at offset 64 MiB.
# ------------------------------------------------------------

write_pattern "${PATTERN_B}" 64

verify_patterns "${LIVE_DEVICE}" "Live device before deletion"

# ------------------------------------------------------------
# 6. Create S2 and wait until applied.
# ------------------------------------------------------------

snapshot_create "${SNAPSHOT_S2}"

cp_login

wait_for_snapshot_applied \
    "${SNAPSHOT_S2}" \
    "${LAST_CLI_SNAPSHOT_ID}" \
    s2

S2_ID="${LAST_CP_SNAPSHOT_ID}"

[[ "${S2_ID}" -gt "${S1_ID}" ]] ||
    fail "S2 must have a greater snapshot ID than S1"

log "S2 created: name=${SNAPSHOT_S2}, id=${S2_ID}"

printf '%s\n' "${S1_ID}" > "${TEST_LOGDIR}/s1-id.txt"
printf '%s\n' "${S2_ID}" > "${TEST_LOGDIR}/s2-id.txt"

# ------------------------------------------------------------
# 7. Verify database state before deletion.
# ------------------------------------------------------------

[[ "$(snapshot_row_count)" == "1" ]] ||
    fail "S1 snapshot row missing before deletion"

[[ "$(delete_tracking_count)" == "0" ]] ||
    fail "Unexpected snapshot deletion tracking rows"

[[ -z "$(get_delete_events)" ]] ||
    fail "Unexpected Action-6 events before S1 deletion"

# ------------------------------------------------------------
# 8. Delete S1 by snapshot ID.
# ------------------------------------------------------------

log "Deleting S1: id=${S1_ID}"

sudo -E "${NIOVA_SNAPSHOT}" \
    --vdev "${VDEV_UUID}" \
    --op delete \
    --snapshot-id "${S1_ID}" \
    --keep \
    2>&1 | tee "${TEST_LOGDIR}/s1-delete.log"

log "S1 deletion request accepted"

# ------------------------------------------------------------
# 9. Wait for actual deletion completion.
# ------------------------------------------------------------

wait_for_snapshot_delete

# ------------------------------------------------------------
# 10. Verify S1 lookup fails by ID and name.
# ------------------------------------------------------------

if sudo -E "${NIOVA_SNAPSHOT}" \
    --vdev "${VDEV_UUID}" \
    --op lookup \
    --snapshot-id "${S1_ID}" \
    > "${TEST_LOGDIR}/s1-lookup-id.log" 2>&1; then

    fail "Lookup unexpectedly succeeded for deleted S1 ID"
fi

if snapshot_lookup "${SNAPSHOT_S1}" \
    > "${TEST_LOGDIR}/s1-lookup-name.log" 2>&1; then

    fail "Lookup unexpectedly succeeded for deleted S1 name"
fi

# Independently verify the actual CP not-found status.

cp_login

verify_snapshot_not_found snapshot_id "${S1_ID}"
verify_snapshot_not_found name "${SNAPSHOT_S1}"

log "S1 lookup by ID and name correctly failed"

# ------------------------------------------------------------
# 11. Verify retained snapshot S2 still exists.
# ------------------------------------------------------------

snapshot_lookup "${SNAPSHOT_S2}" \
    2>&1 | tee "${TEST_LOGDIR}/s2-lookup-name.log"

sudo -E "${NIOVA_SNAPSHOT}" \
    --vdev "${VDEV_UUID}" \
    --op lookup \
    --snapshot-id "${S2_ID}" \
    2>&1 | tee "${TEST_LOGDIR}/s2-lookup-id.log"

wait_for_snapshot_applied \
    "${SNAPSHOT_S2}" \
    "${S2_ID}" \
    s2-after-delete

log "S2 lookup succeeded and CP state remains applied"

# ------------------------------------------------------------
# 12. Mount S2 read-only.
# ------------------------------------------------------------

start_snapshot_mount "${SNAPSHOT_S2}" s2

[[ "${RO_DEVICE}" != "${LIVE_DEVICE}" ]] ||
    fail "Snapshot mount returned the live device"

# ------------------------------------------------------------
# 13. Verify both patterns on S2.
# ------------------------------------------------------------

verify_patterns "${RO_DEVICE}" "Snapshot S2"

# ------------------------------------------------------------
# 14. Verify both patterns on the live RW device.
# ------------------------------------------------------------

verify_patterns "${LIVE_DEVICE}" "Live RW device after deletion"

# ------------------------------------------------------------
# 15. Verify final deletion state.
# ------------------------------------------------------------

[[ "$(snapshot_row_count)" == "0" ]] ||
    fail "S1 snapshot row still exists"

[[ "$(delete_tracking_count)" == "0" ]] ||
    fail "S1 deletion tracking rows still exist"

# ------------------------------------------------------------
# 16. Report success.
# ------------------------------------------------------------

cat <<EOF
PASS: basic snapshot delete test
  vdev:                  ${VDEV_UUID}
  deleted snapshot:      ${SNAPSHOT_S1}
  deleted snapshot ID:   ${S1_ID}
  retained snapshot:     ${SNAPSHOT_S2}
  retained snapshot ID:  ${S2_ID}
  live device:           ${LIVE_DEVICE}
  snapshot device:       ${RO_DEVICE}
  pattern A MD5:         ${A_HASH}
  pattern B MD5:         ${B_HASH}
  S1 lookup by ID:       not found
  S1 lookup by name:     not found
  S1 database rows:      0
  delete tracking rows:  0
  Action-6 events:       verified and acknowledged
  S2 CP state:           applied
  S2 data:               verified
  live RW data:          verified
EOF