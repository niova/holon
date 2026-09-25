#!/usr/bin/env bash
# Common helpers for snapshot system tests.
# Source this file from individual snapshot test scripts.

set -euo pipefail

: "${VDEV_UUID:?VDEV_UUID is required}"
: "${NIOVA_SNAPSHOT:?NIOVA_SNAPSHOT is required}"
: "${TEST_LOGDIR:?TEST_LOGDIR is required}"

mkdir -p "${TEST_LOGDIR}"

VBLK_SIZE_BYTES="${VBLK_SIZE_BYTES:-4096}"
SNAP_OFFSET="${SNAP_OFFSET:-1G}"
SNAP_SIZE="${SNAP_SIZE:-128M}"
SNAP_BS="${SNAP_BS:-4k}"
SNAP_IODEPTH="${SNAP_IODEPTH:-32}"
SNAPSHOT_TIMEOUT="${SNAPSHOT_TIMEOUT:-30}"
UBLK_TIMEOUT="${UBLK_TIMEOUT:-30}"

MDSVC_API_URL="${MDSVC_API_URL:-http://127.0.0.1:8081}"
CP_USERNAME="${INTEGRATION_ADMIN_USERNAME:-admin}"
CP_PASSWORD="${INTEGRATION_ADMIN_PASSWORD:-admin}"

CP_TOKEN="${CP_TOKEN:-}"
LAST_CLI_SNAPSHOT_ID="${LAST_CLI_SNAPSHOT_ID:-}"
LAST_CP_SNAPSHOT_ID="${LAST_CP_SNAPSHOT_ID:-}"
RO_LAUNCH_PID="${RO_LAUNCH_PID:-}"
RO_DEVICE="${RO_DEVICE:-}"
BACKGROUND_WRITER_PID="${BACKGROUND_WRITER_PID:-}"

log()
{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

fail()
{
    echo "FAIL: $*" >&2
    exit 1
}

require_command()
{
    local command_name="$1"

    if [[ "${command_name}" == */* ]]; then
        [[ -x "${command_name}" ]] || fail "required executable not found: ${command_name}"
    else
        command -v "${command_name}" >/dev/null 2>&1 ||
            fail "required command not found: ${command_name}"
    fi
}

require_block_device()
{
    local device="$1"
    [[ -b "${device}" ]] || fail "block device does not exist: ${device}"
}

# Write one deterministic generation.
# Usage:
#   fio_write_generation DEVICE NAME SEED [OFFSET] [SIZE] [BS] [VERIFY_PATTERN]
fio_write_generation()
{
    local device="$1"
    local job_name="$2"
    local seed="$3"
    local offset="${4:-${SNAP_OFFSET}}"
    local size="${5:-${SNAP_SIZE}}"
    local bs="${6:-${SNAP_BS}}"
    local pattern="${7:-}"
    local log_file="${TEST_LOGDIR}/${job_name}-write.log"
    local -a pattern_args=()

    require_block_device "${device}"

    [[ -n "${pattern}" ]] && pattern_args+=(--verify_pattern="${pattern}")

    log "Writing ${job_name}: device=${device} seed=${seed} offset=${offset} size=${size}${pattern:+ pattern=${pattern}}"

    set +e
    sudo fio \
        --name="${job_name}" \
        --filename="${device}" \
        --rw=randwrite \
        --bs="${bs}" \
        --size="${size}" \
        --offset="${offset}" \
        --ioengine=io_uring \
        --iodepth="${SNAP_IODEPTH}" \
        --direct=1 \
        --verify=crc32c \
        "${pattern_args[@]}" \
        --verify_fatal=1 \
        --group_reporting \
        --randseed="${seed}" \
        2>&1 | tee "${log_file}"
    local rc=${PIPESTATUS[0]}
    set -e

    [[ ${rc} -eq 0 ]] || fail "fio write failed: ${job_name}, rc=${rc}"
    grep -Eq 'err= *0' "${log_file}" || fail "fio write did not report err=0: ${job_name}"

    log "Write verification succeeded: ${job_name}"
}

# Read an existing generation and check its fio CRC32C verification data.
# Usage:
#   fio_verify_generation DEVICE NAME SEED [OFFSET] [SIZE] [BS] [VERIFY_PATTERN] [LOG_SUFFIX]
fio_verify_generation()
{
    local device="$1"
    local job_name="$2"
    local seed="$3"
    local offset="${4:-${SNAP_OFFSET}}"
    local size="${5:-${SNAP_SIZE}}"
    local bs="${6:-${SNAP_BS}}"
    local pattern="${7:-}"
    local suffix="${8:-verify-$(basename "${device}")}"
    local log_file="${TEST_LOGDIR}/${job_name}-${suffix}.log"
    local -a pattern_args=()

    require_block_device "${device}"

    [[ -n "${pattern}" ]] && pattern_args+=(--verify_pattern="${pattern}")

    log "Verifying ${job_name}: device=${device} seed=${seed}${pattern:+ pattern=${pattern}}"

    set +e
    sudo fio \
        --name="${job_name}" \
        --filename="${device}" \
        --rw=read \
        --bs="${bs}" \
        --size="${size}" \
        --offset="${offset}" \
        --ioengine=io_uring \
        --iodepth="${SNAP_IODEPTH}" \
        --direct=1 \
        --verify=crc32c \
        "${pattern_args[@]}" \
        --verify_only \
        --verify_fatal=1 \
        --group_reporting \
        --randseed="${seed}" \
        2>&1 | tee "${log_file}"
    local rc=${PIPESTATUS[0]}
    set -e

    [[ ${rc} -eq 0 ]] || fail "fio verification failed: ${job_name}, rc=${rc}"
    grep -Eq 'err= *0' "${log_file}" || fail "fio verification did not report err=0: ${job_name}"

    log "Verification succeeded: ${job_name}"
}

snapshot_create()
{
    local snapshot_name="$1"
    local log_file="${TEST_LOGDIR}/${snapshot_name}-create.log"

    LAST_CLI_SNAPSHOT_ID=""
    log "Creating snapshot ${snapshot_name}"

    set +e
    "${NIOVA_SNAPSHOT}" \
        --vdev "${VDEV_UUID}" \
        --op create \
        --snapshot-name "${snapshot_name}" \
        --keep \
        2>&1 | tee "${log_file}"
    local rc=${PIPESTATUS[0]}
    set -e

    [[ ${rc} -eq 0 ]] || fail "snapshot create failed: ${snapshot_name}"

    LAST_CLI_SNAPSHOT_ID="$(sed -nE 's/.*[Ss]napshot id[=: ]+([0-9]+).*/\1/p' "${log_file}" | head -1)"
    log "Snapshot create command succeeded: ${snapshot_name}${LAST_CLI_SNAPSHOT_ID:+ id=${LAST_CLI_SNAPSHOT_ID}}"
}

snapshot_lookup()
{
    local snapshot_name="$1"

    "${NIOVA_SNAPSHOT}" \
        --vdev "${VDEV_UUID}" \
        --op lookup \
        --snapshot-name "${snapshot_name}"
}

snapshot_delete()
{
    local snapshot_name="$1"
    local log_file="${TEST_LOGDIR}/${snapshot_name}-delete.log"

    log "Deleting snapshot ${snapshot_name}"

    set +e
    "${NIOVA_SNAPSHOT}" \
        --vdev "${VDEV_UUID}" \
        --op delete \
        --snapshot-name "${snapshot_name}" \
        2>&1 | tee "${log_file}"
    local rc=${PIPESTATUS[0]}
    set -e

    if [[ ${rc} -ne 0 ]]; then
        log "Snapshot delete failed: ${snapshot_name}, rc=${rc}" >&2
        return "${rc}"
    fi

    log "Snapshot delete succeeded: ${snapshot_name}"
}

json_get_field()
{
    local field="$1"

    python3 -c '
import json
import sys
field = sys.argv[1]
data = json.load(sys.stdin)
candidates = []
if isinstance(data, dict):
    candidates.append(data)
    inner = data.get("data")
    if isinstance(inner, dict):
        candidates.append(inner)
        p = inner.get("payload")
        if isinstance(p, dict):
            candidates.append(p)
    payload = data.get("payload")
    if isinstance(payload, dict):
        candidates.append(payload)
for obj in candidates:
    if field in obj and obj[field] is not None:
        value = obj[field]
        if isinstance(value, bool):
            print("true" if value else "false")
        elif isinstance(value, (dict, list)):
            print(json.dumps(value))
        else:
            print(value)
        sys.exit(0)
sys.exit(1)
' "${field}"
}

cp_login()
{
    local payload response

    payload="$(CP_USERNAME="${CP_USERNAME}" CP_PASSWORD="${CP_PASSWORD}" python3 - <<'PY'
import json
import os
print(json.dumps({"username": os.environ["CP_USERNAME"], "password": os.environ["CP_PASSWORD"]}))
PY
)"

    response="$(
        curl --fail --silent --show-error \
            -X POST \
            -H 'Content-Type: application/json' \
            --data "${payload}" \
            "${MDSVC_API_URL}/users/login"
    )" || fail "failed to login to mdsvc API at ${MDSVC_API_URL}"

    CP_TOKEN="$(printf '%s' "${response}" | json_get_field access_token || true)"
    [[ -n "${CP_TOKEN}" ]] || fail "login response did not contain access_token"
}

cp_get_snapshot()
{
    local snapshot_name="$1"

    curl --fail --silent --show-error \
        -H "Authorization: Bearer ${CP_TOKEN}" \
        -H 'Content-Type: application/json' \
        --get \
        --data-urlencode "vdev_id=${VDEV_UUID}" \
        --data-urlencode "name=${snapshot_name}" \
        "${MDSVC_API_URL}/api/snapshot"
}

# Wait until CP reports state=applied. Sets LAST_CP_SNAPSHOT_ID.
# Usage: wait_for_snapshot_applied SNAPSHOT_NAME [EXPECTED_ID] [LOG_TAG]
wait_for_snapshot_applied()
{
    local snapshot_name="$1"
    local expected_id="${2:-}"
    local tag="${3:-${snapshot_name}}"
    local deadline response state cp_id cp_name cp_vdev

    LAST_CP_SNAPSHOT_ID=""
    deadline=$((SECONDS + SNAPSHOT_TIMEOUT))

    while (( SECONDS < deadline )); do
        if response="$(cp_get_snapshot "${snapshot_name}" 2>"${TEST_LOGDIR}/${tag}-cp.err")"; then
            printf '%s\n' "${response}" > "${TEST_LOGDIR}/${tag}-cp.json"

            state="$(printf '%s' "${response}" | json_get_field state || true)"
            cp_id="$(printf '%s' "${response}" | json_get_field snapshot_id || true)"
            cp_name="$(printf '%s' "${response}" | json_get_field name || true)"
            cp_vdev="$(printf '%s' "${response}" | json_get_field vdev_id || true)"

            log "CP snapshot lookup: name=${snapshot_name} id=${cp_id:-unknown} state=${state:-unknown}"

            case "${state}" in
                applied)
                    [[ "${cp_id}" =~ ^[1-9][0-9]*$ ]] ||
                        fail "CP returned applied snapshot with invalid snapshot_id='${cp_id}'"
                    [[ -z "${cp_name}" || "${cp_name}" == "${snapshot_name}" ]] ||
                        fail "snapshot name mismatch: expected ${snapshot_name}, got ${cp_name}"
                    [[ -z "${cp_vdev}" || "${cp_vdev}" == "${VDEV_UUID}" ]] ||
                        fail "snapshot vdev mismatch: expected ${VDEV_UUID}, got ${cp_vdev}"
                    [[ -z "${expected_id}" || "${cp_id}" == "${expected_id}" ]] ||
                        fail "snapshot ID mismatch: expected=${expected_id}, CP=${cp_id}"

                    LAST_CP_SNAPSHOT_ID="${cp_id}"
                    return 0
                    ;;
                failed|abandoned)
                    fail "snapshot '${snapshot_name}' reached terminal state '${state}'"
                    ;;
            esac
        fi

        sleep 0.5
    done

    fail "snapshot '${snapshot_name}' did not reach state=applied within ${SNAPSHOT_TIMEOUT}s"
}

# Hash the fixed 128 MiB region starting at 1 GiB by default.
region_sha256()
{
    local device="$1"
    local hash_bs="${2:-4M}"
    local hash_skip="${3:-256}"
    local hash_count="${4:-32}"

    require_block_device "${device}"

    sudo dd \
        if="${device}" \
        bs="${hash_bs}" \
        skip="${hash_skip}" \
        count="${hash_count}" \
        iflag=direct \
        status=none \
        | sha256sum \
        | awk '{print $1}'
}

list_ublk_devices()
{
    shopt -s nullglob
    local devices=(/dev/ublkb*)
    shopt -u nullglob
    printf '%s\n' "${devices[@]}"
}

# Mount a snapshot through a separate niova-ublk process.
# Sets RO_LAUNCH_PID and RO_DEVICE.
# Usage: start_snapshot_mount SNAPSHOT_NAME [LOG_TAG]
start_snapshot_mount()
{
    local snapshot_name="$1"
    local tag="${2:-${snapshot_name}}"
    local ro_client_uuid before_file after_file deadline new_devices

    : "${NIOVA_UBLK:?NIOVA_UBLK is required for snapshot mount}"
    require_command "${NIOVA_UBLK}"

    stop_snapshot_mount

    ro_client_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
    before_file="${TEST_LOGDIR}/${tag}-ublk-before.txt"
    after_file="${TEST_LOGDIR}/${tag}-ublk-after.txt"

    list_ublk_devices | sort > "${before_file}"
    log "Mounting snapshot '${snapshot_name}' read-only"

    setsid sudo -E "${NIOVA_UBLK}" \
        -t cp \
        -v "${VDEV_UUID}" \
        -u "${ro_client_uuid}" \
        -k "snapshot_name=${snapshot_name}" \
        -q 128 \
        -b 1048576 \
        >"${TEST_LOGDIR}/${tag}-snapshot-ublk.log" 2>&1 &

    RO_LAUNCH_PID=$!
    RO_DEVICE=""
    deadline=$((SECONDS + UBLK_TIMEOUT))

    while (( SECONDS < deadline )); do
        list_ublk_devices | sort > "${after_file}"
        new_devices="$(comm -13 "${before_file}" "${after_file}" || true)"

        if [[ $(printf '%s\n' "${new_devices}" | sed '/^$/d' | wc -l) -eq 1 ]]; then
            RO_DEVICE="$(printf '%s\n' "${new_devices}" | sed '/^$/d')"
            require_block_device "${RO_DEVICE}"
            log "Snapshot '${snapshot_name}' mounted as ${RO_DEVICE}"
            return 0
        fi

        if ! kill -0 "${RO_LAUNCH_PID}" 2>/dev/null; then
            tail -100 "${TEST_LOGDIR}/${tag}-snapshot-ublk.log" >&2 || true
            fail "snapshot niova-ublk exited before creating a device for ${snapshot_name}"
        fi

        sleep 0.2
    done

    tail -100 "${TEST_LOGDIR}/${tag}-snapshot-ublk.log" >&2 || true
    fail "timed out waiting for RO device for snapshot ${snapshot_name}"
}

stop_snapshot_mount()
{
    [[ -n "${RO_LAUNCH_PID:-}" ]] || return 0

    if kill -0 "${RO_LAUNCH_PID}" 2>/dev/null; then
        sudo kill -TERM -- "-${RO_LAUNCH_PID}" 2>/dev/null ||
            kill -TERM "${RO_LAUNCH_PID}" 2>/dev/null || true

        local deadline=$((SECONDS + 10))
        while kill -0 "${RO_LAUNCH_PID}" 2>/dev/null && (( SECONDS < deadline )); do
            sleep 0.2
        done

        if kill -0 "${RO_LAUNCH_PID}" 2>/dev/null; then
            sudo kill -KILL -- "-${RO_LAUNCH_PID}" 2>/dev/null ||
                kill -KILL "${RO_LAUNCH_PID}" 2>/dev/null || true
        fi
    fi

    RO_LAUNCH_PID=""
    RO_DEVICE=""
}

# Used by writes-resume tests. The PID is stored in BACKGROUND_WRITER_PID.
fio_start_background_writer()
{
    local device="$1"
    local runtime="${2:-30}"
    local seed="${3:-5001}"
    local log_file="${TEST_LOGDIR}/background-writer.log"

    require_block_device "${device}"

    sudo fio \
        --name=snapshot-background-writer \
        --filename="${device}" \
        --rw=randwrite \
        --bs=4k \
        --size=512M \
        --offset=2G \
        --ioengine=io_uring \
        --iodepth=32 \
        --direct=1 \
        --time_based \
        --runtime="${runtime}" \
        --randseed="${seed}" \
        --group_reporting \
        >"${log_file}" 2>&1 &

    BACKGROUND_WRITER_PID=$!
    log "Background writer started: pid=${BACKGROUND_WRITER_PID}"
}

wait_for_writer()
{
    local pid="${1:-${BACKGROUND_WRITER_PID}}"
    [[ -n "${pid}" ]] || fail "background writer PID is not set"

    if wait "${pid}"; then
        log "Background writer completed successfully"
    else
        fail "background writer failed"
    fi
}
