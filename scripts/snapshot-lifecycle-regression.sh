#!/usr/bin/env bash
# Repeated normal snapshot lifecycle regression test for niova-block.
set -Eeuo pipefail

: "${VDEV_UUID:?VDEV_UUID must be set}"
: "${LIVE_DEVICE:?LIVE_DEVICE must be set}"

NIOVA_SNAPSHOT="${NIOVA_SNAPSHOT:-scripts/niova-snapshot}"
NIOVA_UBLK="${NIOVA_UBLK:-niova-ublk}"
MDSVC_API_URL="${MDSVC_API_URL:-http://127.0.0.1:8081}"
CP_USERNAME="${INTEGRATION_ADMIN_USERNAME:-admin}"
CP_PASSWORD="${INTEGRATION_ADMIN_PASSWORD:-admin}"
TEST_LOGDIR="${TEST_LOGDIR:-/tmp/snapshot-lifecycle-regression}"
MAX_ITERATIONS="${MAX_ITERATIONS:-1000}"
BASE_SEED="${BASE_SEED:-100000}"
FIO_OFFSET="${FIO_OFFSET:-1G}"
FIO_SIZE="${FIO_SIZE:-128M}"
FIO_BS="${FIO_BS:-4k}"
FIO_IODEPTH="${FIO_IODEPTH:-32}"
OP_TIMEOUT="${OP_TIMEOUT:-120}"
FIO_TIMEOUT="${FIO_TIMEOUT:-300}"
POLL_TIMEOUT="${POLL_TIMEOUT:-60}"
UBLK_TIMEOUT="${UBLK_TIMEOUT:-45}"
STOP_TIMEOUT="${STOP_TIMEOUT:-15}"
NISD_PID="${NISD_PID:-}"
LIVE_UBLK_PID="${LIVE_UBLK_PID:-}"
NISD_LOG="${NISD_LOG:-}"
LIVE_UBLK_LOG="${LIVE_UBLK_LOG:-}"

CP_TOKEN=""; RO_PID=""; RO_DEVICE=""; ITER_DIR=""; SNAPSHOT_NAME=""; SNAPSHOT_ID=""
SNAPSHOT_CREATED=0; ITERATION_OK=0
mkdir -p "$TEST_LOGDIR"
exec > >(tee -a "$TEST_LOGDIR/regression.log") 2>&1

log(){ printf '[%s] %s\n' "$(date --iso-8601=seconds)" "$*"; }
die(){ log "FAIL: $*"; exit 1; }
run_bounded(){ local seconds=$1 logfile=$2; shift 2; timeout --signal=TERM --kill-after=10s "${seconds}s" "$@" >"$logfile" 2>&1; }
require(){ command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }
is_alive(){ [[ -n "$1" ]] && kill -0 "$1" 2>/dev/null; }

json_field(){ python3 -c '
import json,sys
k=sys.argv[1]; d=json.load(sys.stdin)
q=[d] if isinstance(d,dict) else []
for x in list(q):
  for y in (x.get("data"),x.get("payload")):
    if isinstance(y,dict): q.append(y)
for x in q:
  if x.get(k) is not None:
    v=x[k]; print(json.dumps(v) if isinstance(v,(dict,list)) else str(v).lower() if isinstance(v,bool) else v); raise SystemExit
raise SystemExit(1)' "$1"; }

cp_login(){
  local body response
  body="$(CP_USERNAME="$CP_USERNAME" CP_PASSWORD="$CP_PASSWORD" python3 -c 'import json,os; print(json.dumps({"username":os.environ["CP_USERNAME"],"password":os.environ["CP_PASSWORD"]}))')"
  response="$(timeout "${OP_TIMEOUT}s" curl --max-time "$OP_TIMEOUT" --fail --silent --show-error -H 'Content-Type: application/json' -d "$body" "$MDSVC_API_URL/users/login")" || die "control-plane login failed"
  CP_TOKEN="$(printf %s "$response" | json_field access_token || true)"
  [[ -n "$CP_TOKEN" ]] || die "login response has no access_token"
}

cp_lookup(){
  timeout "${OP_TIMEOUT}s" curl --max-time "$OP_TIMEOUT" --fail --silent --show-error \
    -H "Authorization: Bearer $CP_TOKEN" --get --data-urlencode "vdev_id=$VDEV_UUID" \
    --data-urlencode "name=$SNAPSHOT_NAME" "$MDSVC_API_URL/api/snapshot"
}

wait_applied(){
  local deadline=$((SECONDS+POLL_TIMEOUT)) response state id n=0
  : >"$ITER_DIR/control-plane-lookups.jsonl"
  while ((SECONDS < deadline)); do
    n=$((n+1))
    if response="$(cp_lookup 2>"$ITER_DIR/cp-lookup-${n}.err")"; then
      printf '%s\n' "$response" >>"$ITER_DIR/control-plane-lookups.jsonl"
      state="$(printf %s "$response" | json_field state || true)"
      id="$(printf %s "$response" | json_field snapshot_id || true)"
      case "$state" in
        applied) [[ "$id" =~ ^[1-9][0-9]*$ ]] || die "applied snapshot returned invalid ID: $id"; SNAPSHOT_ID="$id"; return;;
        failed|abandoned) die "snapshot entered terminal state=$state";;
      esac
    fi
    sleep .5
  done
  die "snapshot did not become applied within ${POLL_TIMEOUT}s"
}

wait_deleted(){
  local deadline=$((SECONDS+POLL_TIMEOUT)) response n=0
  while ((SECONDS < deadline)); do
    n=$((n+1))
    if ! response="$(cp_lookup 2>"$ITER_DIR/cp-delete-lookup-${n}.err")"; then return 0; fi
    printf '%s\n' "$response" >>"$ITER_DIR/control-plane-delete-lookups.jsonl"
    state="$(printf %s "$response" | json_field state || true)"
    [[ "$state" == deleted || "$state" == not_found ]] && return 0
    sleep .5
  done
  die "snapshot still exists after ${POLL_TIMEOUT}s"
}

list_ublk(){ shopt -s nullglob; local d=(/dev/ublkb*); shopt -u nullglob; printf '%s\n' "${d[@]}"; }
wait_device_gone(){ local deadline=$((SECONDS+UBLK_TIMEOUT)); while ((SECONDS<deadline)); do [[ ! -e "$1" ]] && return; sleep .2; done; die "snapshot device did not disappear: $1"; }

health_check(){
  [[ -b "$LIVE_DEVICE" ]] || die "live block device disappeared: $LIVE_DEVICE"
  [[ -z "$NISD_PID" ]] || is_alive "$NISD_PID" || die "NISD PID $NISD_PID exited"
  [[ -z "$LIVE_UBLK_PID" ]] || is_alive "$LIVE_UBLK_PID" || die "live niova-ublk PID $LIVE_UBLK_PID exited"
}

copy_service_logs(){
  [[ -n "$NISD_LOG" && -f "$NISD_LOG" ]] && cp --preserve=timestamps "$NISD_LOG" "$ITER_DIR/nisd.log" || true
  [[ -n "$LIVE_UBLK_LOG" && -f "$LIVE_UBLK_LOG" ]] && cp --preserve=timestamps "$LIVE_UBLK_LOG" "$ITER_DIR/live-niova-ublk.log" || true
  ps -eo pid,ppid,lstart,stat,args >"$ITER_DIR/processes.txt" || true
}

fio_write_verify(){
  local seed=$1 logf="$ITER_DIR/fio-live-write.log"
  run_bounded "$FIO_TIMEOUT" "$logf" sudo fio --name="generation-$seed" --filename="$LIVE_DEVICE" \
    --rw=randwrite --bs="$FIO_BS" --size="$FIO_SIZE" --offset="$FIO_OFFSET" --ioengine=io_uring \
    --iodepth="$FIO_IODEPTH" --direct=1 --verify=crc32c --do_verify=1 --verify_fatal=1 \
    --randseed="$seed" --group_reporting || die "live fio write/CRC verification failed"
  grep -Eq 'err= *0' "$logf" || die "live fio did not report err=0"
}

fio_snapshot_verify(){
  local seed=$1 logf="$ITER_DIR/fio-snapshot-verify.log"
  run_bounded "$FIO_TIMEOUT" "$logf" sudo fio --name="generation-$seed" --filename="$RO_DEVICE" \
    --rw=read --bs="$FIO_BS" --size="$FIO_SIZE" --offset="$FIO_OFFSET" --ioengine=io_uring \
    --iodepth="$FIO_IODEPTH" --direct=1 --verify=crc32c --verify_only --verify_fatal=1 \
    --randseed="$seed" --group_reporting || die "snapshot fio CRC verification failed"
  grep -Eq 'err= *0' "$logf" || die "snapshot fio did not report err=0"
}

region_hash(){ timeout "${FIO_TIMEOUT}s" sudo dd if="$1" bs=4M skip=256 count=32 iflag=direct status=none | sha256sum | awk '{print $1}'; }

mount_snapshot(){
  local before="$ITER_DIR/ublk-before.txt" after="$ITER_DIR/ublk-after.txt" new deadline uuid
  list_ublk | sort >"$before"; uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  setsid sudo -E "$NIOVA_UBLK" -t cp -v "$VDEV_UUID" -u "$uuid" -k "snapshot_name=$SNAPSHOT_NAME" -q 128 -b 1048576 >"$ITER_DIR/snapshot-ro-mount.log" 2>&1 & RO_PID=$!
  deadline=$((SECONDS+UBLK_TIMEOUT))
  while ((SECONDS<deadline)); do
    list_ublk | sort >"$after"; new="$(comm -13 "$before" "$after" | sed '/^$/d')"
    if [[ "$(printf '%s\n' "$new" | sed '/^$/d' | wc -l)" -eq 1 ]]; then RO_DEVICE="$new"; [[ -b "$RO_DEVICE" ]] || die "new ublk path is not a block device"; return; fi
    is_alive "$RO_PID" || die "snapshot niova-ublk exited before device creation"
    sleep .2
  done
  die "snapshot mount timed out"
}

stop_ro(){
  local old="$RO_DEVICE" deadline
  if is_alive "$RO_PID"; then sudo kill -TERM -- "-$RO_PID" 2>/dev/null || kill -TERM "$RO_PID" 2>/dev/null || true; fi
  deadline=$((SECONDS+STOP_TIMEOUT)); while is_alive "$RO_PID" && ((SECONDS<deadline)); do sleep .2; done
  is_alive "$RO_PID" && { sudo kill -KILL -- "-$RO_PID" 2>/dev/null || true; }
  [[ -z "$old" ]] || wait_device_gone "$old"
  RO_PID=""; RO_DEVICE=""
}

on_exit(){
  local rc=$?; set +e
  trap - EXIT INT TERM
  [[ -z "$ITER_DIR" ]] || copy_service_logs
  # Stop only this test's RO client. Never restart/stop NISD or the live client.
  stop_ro
  if ((rc!=0)); then
    printf '%s\n' "failure_iteration=${ITER_DIR##*-}" "snapshot_name=$SNAPSHOT_NAME" "snapshot_id=$SNAPSHOT_ID" >"$TEST_LOGDIR/FAILURE"
    log "terminated; snapshot/service state and artifacts retained in $ITER_DIR"
  fi
  exit "$rc"
}
trap on_exit EXIT INT TERM

for c in timeout curl python3 fio sha256sum dd comm; do require "$c"; done
[[ -b "$LIVE_DEVICE" ]] || die "not a block device: $LIVE_DEVICE"
cp_login

for ((iteration=1; iteration<=MAX_ITERATIONS; iteration++)); do
  ITERATION_OK=0; SNAPSHOT_CREATED=0; SNAPSHOT_ID=""; RO_PID=""; RO_DEVICE=""
  seed=$((BASE_SEED+iteration)); SNAPSHOT_NAME="snapshot-regression-$(date +%s)-${iteration}"
  ITER_DIR="$TEST_LOGDIR/iteration-$(printf '%06d' "$iteration")"; mkdir -p "$ITER_DIR"
  printf 'iteration=%s\nseed=%s\nvdev_uuid=%s\nsnapshot_name=%s\n' "$iteration" "$seed" "$VDEV_UUID" "$SNAPSHOT_NAME" >"$ITER_DIR/metadata.env"
  log "iteration=$iteration seed=$seed snapshot=$SNAPSHOT_NAME"
  health_check
  fio_write_verify "$seed"
  expected_hash="$(region_hash "$LIVE_DEVICE")" || die "live generation hash read failed"
  run_bounded "$OP_TIMEOUT" "$ITER_DIR/snapshot-create.log" "$NIOVA_SNAPSHOT" --vdev "$VDEV_UUID" --op create --snapshot-name "$SNAPSHOT_NAME" --keep || die "snapshot create failed"
  SNAPSHOT_CREATED=1; wait_applied
  printf 'snapshot_id=%s\nexpected_sha256=%s\n' "$SNAPSHOT_ID" "$expected_hash" >>"$ITER_DIR/metadata.env"
  mount_snapshot; health_check
  fio_snapshot_verify "$seed"
  actual_hash="$(region_hash "$RO_DEVICE")" || die "snapshot generation hash read failed"
  [[ "$actual_hash" == "$expected_hash" ]] || die "snapshot content mismatch: expected=$expected_hash actual=$actual_hash"
  printf 'snapshot_sha256=%s\nsnapshot_device=%s\n' "$actual_hash" "$RO_DEVICE" >>"$ITER_DIR/metadata.env"
  stop_ro
  run_bounded "$OP_TIMEOUT" "$ITER_DIR/snapshot-delete.log" "$NIOVA_SNAPSHOT" --vdev "$VDEV_UUID" --op delete --snapshot-name "$SNAPSHOT_NAME" || die "snapshot delete failed"
  wait_deleted; SNAPSHOT_CREATED=0
  health_check; copy_service_logs
  touch "$ITER_DIR/PASS"; ITERATION_OK=1
done

trap - EXIT INT TERM
log "PASS: completed $MAX_ITERATIONS snapshot lifecycle iterations"
