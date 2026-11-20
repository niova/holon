---
branch: async_events_recipes
pr: 430
repo: niova/holon
reviewed_commit: 0719df461a6f38db46a12e5fc8eaf502b5d36974
base_commit: 64ac7b106537ffbf52dcda4a19a94a9039026cad
reviewed_at: 2026-11-13T04:34:40Z
reviewer: Tejas-ChandraShekarRaju
model: Claude Opus 5.5
review_mode: independent-first
review_focus: test correctness (does each recipe test what it claims)
verdict: request-changes
---

# Code Review: `async_events_recipes` (holon PR #430)

Branch `async_events_recipes` @ `0719df4`, 15 commits on `master` (`64ac7b1`).
16 files, +5,578 / -17. Eleven new recipes, three lookup plugin changes, one empty helper file.

| Area | Files |
|---|---|
| NISD async events | `tidb_nisd_async_event_{basic,eventlimit,multiple_order,restart_persistence,cp_recovery,cluster_uuid,auth_recovery,callback_idempotency,vdev_cleanup}.yml` |
| Cleanup worker | `tidb_cleanup_single_server.yml`, `tidb_cleanup_multiple_app_servers.yml`, `cleanup_worker_verify.py` |
| Plugins | `lookup_plugin/tidb_api.py`, `tidb_setup.py`, `nisd_handler.py` |

This is a tests PR, so the review covers whether the tests are correct. Performance is out of scope.
Everything was checked against mdsvc-tidb `main` and niova-block `master` (`60ed593`) on groot1, 2026-11-13.

## Verdict

**Request changes.** None of the eleven recipes can pass against current mdsvc-tidb and niova-block. Two of
them pass without testing anything. The test designs are mostly sound, but they were not run end to end.

- The cleanup-worker recipes call `cleanup_worker_verify.py`, which is empty. Python exits 0, so the recipe
  passes.
- Every NISD recipe adds events through `POST /api/test/event` with `EntityID`. mdsvc serves
  `POST /api/events` with `nisd_id`. No branch on origin has the first route.
- Seven of the nine NISD recipes read the lreg keys `/ncp_event/0/...`. The author fixed this to
  `/ncp_event/...` in `basic` only.
- Three recipes read `/tmp/mdsvc-heartbeat-capture.jsonl`. Nothing in mdsvc writes it.
- Two recipes test NISD features that do not exist yet: CREATE_SNAPSHOT and DeleteVdev event dispatch.
- Two recipe files do not parse.

Good parts worth keeping:
- `eventlimit` drives `/api/heartbeat` directly, so each response is one heartbeat boundary.
- `vdev_cleanup` builds the expected (chunk, NISD) set from placement and compares exactly.
- `multiple_order` and `restart_persistence` start from a fresh device and assert `event_sno == 0` first.

## Issues by priority

P0 blocks merge. P1 should be fixed in this PR. P2 can be a tracked follow-up. P3 is a nit.

| Pri | Category | Issue | Where | Finding |
|---|---|---|---|---|
| P0 | False pass | Both cleanup-worker recipes delegate all checks to an empty script. It exits 0, so they pass with no verification. | `cleanup_worker_verify.py` (0 bytes) | 1 |
| P0 | Broken | Two recipes do not parse. One file is truncated at the top. | `tidb_cleanup_single_server.yml:1`, `tidb_nisd_async_event_auth_recovery.yml:668` | 2 |
| P0 | Broken | AddEvent uses the wrong path and body in all 9 NISD recipes | every `async_event_add_path` | 3 |
| P0 | Broken | 7 of 9 NISD recipes read `/ncp_event/0/...`. Only `basic` has the fixed `/ncp_event/...`. | all except `basic` and `eventlimit` | 4 |
| P0 | Broken | `cluster_uuid`, `auth_recovery`, `vdev_cleanup` need a heartbeat capture file that nothing produces | `heartbeat_capture_file` | 5 |
| P0 | Wrong contract | `callback_idempotency` needs a CREATE_SNAPSHOT event and a crash hook that do not exist. `vdev_cleanup` needs NISD DeleteVdev dispatch, and expects per-chunk events but CP sends one event per NISD. | `callback_idempotency`, `vdev_cleanup` | 6 |
| P1 | False pass | Assertions that pass when nothing happened: auth wrong-secret phase with no queued event, `basic` "ACK" step repeating the event_sno check, `default(x) == x` and `is not defined or` identity checks, `retry\|Retry` as retry evidence | `auth_recovery`, `basic`, `callback_idempotency`, `cp_recovery` | 7 |
| P1 | Isolation | NISD PID found with host-wide `pgrep -x nisd \| head -n1`. Cleanup can kill another user's NISD. An empty PID counts as "crashed". | 5 recipes | 8 |
| P1 | Broken | `snapshot_name` changes on every reference (lazy `lookup('pipe','date')` in play vars) | `callback_idempotency:42` | 9 |
| P1 | Broken | `server_env` passed to `docker_setup` is dropped. CP never gets the non-default tenant. | `cluster_uuid:83-92` | 10 |
| P1 | Broken | SQL checks query database `mdsvc`. `nisd_event` lives in the tenant schema `mdsvc_tenant_<uuid>`. | `auth_recovery`, `vdev_cleanup` | 11 |
| P1 | Flaky | `cp_recovery` queues the event before blocking CP, so the NISD can consume it before the outage | `cp_recovery:215-261` | 12 |
| P2 | Hygiene | 8 of 9 NISD recipes have no `rescue` / `terminate_recipe`, unlike holon's convention. A failure skips `post_run.yml` cleanup. | all except `basic` | 13 |
| P2 | Coverage | `restart_persistence` "no replay" sends its own heartbeat to CP. It does not check that the NISD skipped callbacks after restart. | `restart_persistence:409-441` | 14 |
| P3 | Diagnosability | AddEvent and other API responses are not asserted. mdsvc returns some app errors with HTTP 200, so a failure surfaces later as an `event_sno` timeout. | all NISD recipes | 15 |
| P3 | Cleanup | Dead regex text in `basic` grep. `wait_server_log` is added but no recipe calls it. It also matches the pattern anywhere in the log, including earlier lines. | `lookup_plugin/tidb_setup.py` | 16 |
| P3 | Hygiene | Fixed ports 13000-13201, device files at `/`, `iptables` on the host, `become: true` | several | 16 |

## Findings

### 1. P0 (false pass): the cleanup-worker recipes verify nothing

`tidb_recipes/cleanup_worker_verify.py` is committed with 0 bytes (`0719df4`). Both cleanup recipes:

1. check the file exists (`Require all fixtures`),
2. run `python3 cleanup_worker_verify.py --mode single|multi ...`,
3. print its stdout.

There is no other assertion after login. On groot1:
`python3 cleanup_worker_verify.py --mode multi --api http://x --wait-seconds 900` exits **0**.
In the intended setup the fixtures exist (they come from mdsvc-tidb `examples/`). Both recipes would report
success for any cleanup-worker behaviour.

Fix: commit the verifier, or remove both recipes from this PR. The verifier should exit non-zero on any
mismatch. The recipe should also assert on its output, for example a final `PASS` line with the counts checked.

### 2. P0 (broken): two recipe files do not parse

`ansible-playbook --syntax-check` on all eleven recipes:

- `tidb_cleanup_single_server.yml` starts at line 1 with `method: GET` inside a task. The play header, vars and
  the first tasks are missing. YAML load fails with `did not find expected <document start>`.
- `tidb_nisd_async_event_auth_recovery.yml:668` fails with `failed at splitting arguments, either an
  unbalanced jinja2 block or quotes`. The cause is `replace("'", "'\\''")` inside a `shell:` string. Write the
  log section to a file and grep the file instead.

The other nine pass the syntax check.

### 3. P0 (broken): AddEvent path and body do not match mdsvc

All nine NISD recipes add events with:

```yaml
async_event_add_path: "/api/test/event"
body: { 'EntityID': nisd_uuid, 'Action': 250, 'Params': {...} }
```

mdsvc-tidb `main` (`cmd/server/test_routes.go:21`, `internal/models/dto/events.go`):

```
POST /api/events    body: {"nisd_id": "...", "action": 250, "params": {...}}
```

`/api/test/event` exists on no mdsvc-tidb branch on origin (searched every branch). With the defaults, the
request gets a 404. With the path overridden, `EntityID` does not map to `nisd_id`, and the handler returns
400 `nisd_id is required`. The `custom_request` action is fine. The `endpoint_map` already has
`add_event: POST /api/events`, so use it with `body={'nisd_id': ..., 'action': ..., 'params': ...}`.
The recipe comments say the path is "branch-dependent". Pin it to the real route instead.

### 4. P0 (broken): seven recipes read the wrong lreg key

niova-block registers `ncp_event` as a single root object (`LREG_ROOT_ENTRY_GENERATE_OBJECT(ncp_event, ...)`,
`src/ncp_event_handler.c:78`). There is no `/0/` index. The author found this while debugging `basic`:

- `c260b85` "Debug ncp_events structure"
- `0d4a42e` "Correct the ncp_event key format": `/ncp_event/0/ping_enabled` to `/ncp_event/ping_enabled`
- `0c1fa2d` "Correct the path of the ncp_event keys": result key `['/0/ping_enabled']` to
  `['/ncp_event/ping_enabled']`

Both commits touch `basic` only. `multiple_order`, `restart_persistence`, `cp_recovery`, `cluster_uuid`,
`auth_recovery`, `callback_idempotency` and `vdev_cleanup` still use `/ncp_event/0/...` and `['/0/...']`.
The first ping wait in each times out. Put the two keys in one shared vars file so this cannot drift again.

### 5. P0 (broken): three recipes depend on a heartbeat capture that does not exist

`cluster_uuid`, `auth_recovery` and `vdev_cleanup` read `MDSVC_HEARTBEAT_CAPTURE_FILE`, default
`/tmp/mdsvc-heartbeat-capture.jsonl`, as "each received heartbeat JSON object". No mdsvc-tidb branch on origin
writes such a file (searched for `heartbeat-capture`, `HEARTBEAT_CAPTURE`, `HeartbeatCapture`). These steps wait
30 s and fail.

If a capture is added later, the recipes must also truncate the file at the start. Today they read the
first matching line. With fixed NISD UUIDs, a line from an earlier run would satisfy the check.

Options: add a test-only capture to mdsvc's `testendpoints` build first, then land these recipes. Or check
the CP side through `GET /api/events` (`expire_at` is set when an ACK covers the row), which already exists.

### 6. P0 (wrong contract): two recipes test NISD features that are not implemented

niova-block `master` decodes only `NCP_EVENT_TYPE_TEST = 250` on the wire. Every other action returns `-ENOENT`
(`src/ncp_event_handler.c:250-262`, `src/include/ncp_event_handler.h:28`).

- **`callback_idempotency`** needs `-e snapshot_event_action=<CREATE_SNAPSHOT>` and
  `-e async_event_crash_after_callback_cmd=<hook>`. Neither exists. mdsvc has `DeleteSnapshot = 6` and no
  create-snapshot event. The recipe's own comment says "Do not guess these values".
- **`vdev_cleanup`** expects the NISD to process DeleteVdev events and restore capacity. The NISD cannot
  dispatch action 1. The recipe also models the CP wrongly. It expects one `nisd_event` row per
  (chunk, NISD) with a `chunk_idx` key. mdsvc's `EnqueueDeleteVdev` writes **one row per NISD** with params
  `{"vdev_id": ..., "chunks": [...]}` (`internal/service/event/event.go`, `models.VDevRemoveParams`). The
  normalizer finds no `chunk_idx`, so it collects 0 targets, waits 480 × 2 s = 16 min, and fails.

Fix: drop both recipes from this PR, or keep them behind a clear skip with the reason and the issue that
tracks the feature. When `vdev_cleanup` comes back, expand each row's `chunks` list into
(chunk, NISD) pairs before comparing.

### 7. P1 (false pass): assertions that pass when nothing happened

| Recipe | Check | Why it proves nothing |
|---|---|---|
| `auth_recovery` | "local durable ACK did not advance" (`event_sno == 0`) and "CP event retention unchanged" (unexpired count equal) during the wrong-secret phase | No event is queued until line 523, after this phase. Both are `0 == 0` whether auth works or not. Queue a TEST event **before** starting the wrong-secret NISD. |
| `basic` | Step 11 "Verify NISD AckEventSerialNo value" | It is the same assertion as step 9 (`final_event_sno == expected_event_sno`). The comment below it says it cannot prove the CP got the ACK. The "Test passed" message still says it did. Check with `GET /api/events?nisd_id=...` that the event has `expire_at` set. |
| `callback_idempotency` | `x.name \| default(snapshot_name) == snapshot_name` | Passes when `name` is missing. |
| `callback_idempotency` | `first.snapshot_id is not defined or replay.snapshot_id is not defined or ... ==` | Passes when either id is missing. |
| `callback_idempotency` | Verification D (`data.snapshots` count) | `get_snapshot_name` returns one snapshot, not a list, so the `is not defined` branch always passes. |
| `cp_recovery` | `retry_log_regex: ENOTCONN\|ETIMEDOUT\|ECONNRESET\|retry\|Retry` | Any log line with "retry" in the 45 s window passes. Match the NISD's CP-client retry message specifically. |

### 8. P1 (isolation): NISD processes found by host-wide `pgrep`

`basic` (cleanup), `callback_idempotency`, `restart_persistence` and `auth_recovery` pick the PID to watch or
kill with `pgrep -x nisd | head -n1`. `multiple_order` waits on `pgrep -x nisd`.

- On a shared host this can kill or watch another user's NISD. `basic` runs it in `always`, so it kills one
  even when the test never started its own.
- `callback_idempotency` reads the PID **after** adding the event. If the NISD has already exited, the PID is
  empty, `kill -0 ""` fails, and "Wait for deterministic NISD crash" passes immediately.

`run_nisd` already records the PID in the recipe JSON (`recipe_conf['nisd_process']['process_pid']` in
`nisd_handler.py`). Use that, and read it before any step that can make the NISD exit.

### 9. P1 (broken): `snapshot_name` changes on every reference

`callback_idempotency.yml:42`:
`snapshot_name: "async-idempotency-{{ lookup('pipe', 'date +%s%N') }}"`.
Play vars are templated lazily, so every use runs `date` again. Reproduced on groot1 (ansible-core 2.16.3)
with two `debug` tasks: `async-idempotency-1794544309431849365`, then `async-idempotency-1794544309454982933`.
The AddEvent body and every later lookup use different names. Pin it once with `set_fact` at the start.

### 10. P1 (broken): `cluster_uuid` never configures the CP tenant

The recipe calls `lookup('tidb_setup', 'docker_setup', server_env=cp_server_env)` to start mdsvc with
`MDSVC_TENANT_MANAGER_ID` set to the non-default UUID. `tidb_setup`'s `LookupModule.run` reads only
`variables["ClusterParams"]` and ignores kwargs. `docker_setup` also never reads `server_env`. This PR wires
`server_env` into `_manual_setup_external` and `playground_setup` only. The CP starts with the compose default
tenant, so the test does not set up the case it claims to cover. Either set `ClusterParams.server_env` and
pass it to `docker compose` as environment, or use the playground backend.

### 11. P1 (broken): SQL checks query a database that does not exist

`auth_recovery` and `vdev_cleanup` run `mysql --database=mdsvc ... FROM nisd_event`. `nisd_event` is a
tenant table (`schema_tables.sql`). The schema is `mdsvc_tenant_<tenant manager id with _>`
(`internal/tenant/schema.go:14,37`). With the compose default that is
`mdsvc_tenant_85d85e86_212d_47f5_b97b_4222d378b7b3`. Derive the name from the tenant id, or use
`GET /api/events`, which needs no SQL access.

### 12. P1 (flaky): `cp_recovery` can lose its event before the outage

The recipe adds the TEST event while CP is healthy, then inserts the iptables rule. The NISD heartbeats every
10 s. If a heartbeat lands between the two steps, the NISD processes the event before the outage. Then
"event_sno unchanged during outage" fails, and the recovery path is not exercised. The race window depends on
timing, so the result varies run to run.

Fix: block first, then add the event. The rule blocks all local traffic to `127.0.0.1:8081`, including
Ansible's own API calls. So scope the rule to the NISD (`-m owner --uid-owner` for a dedicated user, or a
cgroup match), or point the NISD at a separate CP port. Also confirm the NISD reaches CP at `cp_host:cp_port`.
It uses `mdsvc-tidb/configs/gossipNodes`, not `api_base_url`.

### 13. P2 (hygiene): no `rescue` in eight NISD recipes

Holon's convention (`holon.yml`, `post_run.yml:195`) is `rescue` sets `terminate_recipe: true`, and
`post_run.yml` fails the run after cleanup. Only `basic` does this. In the other eight, a failed task fails
the play, and `post_run.yml` (process kill, device cleanup) is skipped for that host. NISDs and device files
from a failed run stay behind. Follow the `basic` pattern.


### 14. P2 (coverage): `restart_persistence` replay check tests the CP, not the NISD

After restart the recipe sends its own heartbeat with `AckEventSerialNo=3` and asserts 0 events come back.
That checks CP filtering. The claim is that the restarted NISD does not re-run callbacks 1-3. Check that too:
count each event marker in the NISD log after the restart line. Each should appear once in the whole log,
and zero times after the restart.


### 15. P3 (diagnosability): API responses are not checked

The `tidb_api` lookup raises only on HTTP status not in {200, 201}. mdsvc reports some errors with HTTP 200
and an app status in the body. AddEvent returns HTTP 200 with `StatusNotFound` for an unknown NISD. No recipe
asserts on `add_event_result`. The test still fails, but later, in a wait that times out with a message about `event_sno`. That hides the cause.
After every AddEvent, assert the app status is OK and `event_ptr` is set. Then use `event_ptr` as the expected
serial instead of `initial + 1`.


### 16. P3 nits

- `basic` step 10 greps `"ncp_event_test|test.\*event|event.\*callback"`. In `grep -E`, `\*` is a literal `*`, so the
  last two patterns are dead text. The check still works through `ncp_event_test`. Drop the dead patterns.
- `wait_server_log` (`tidb_setup.py`) has no caller. It also scans the whole log, so a line written before the
  action under test satisfies it. Remove it, or record the log offset first and scan from there.
- Fixed ports (13000-13201), device files at `/`, `become: true` and host `iptables` make the recipes need root
  and prevent parallel runs. Put devices under `ClusterParams.base_dir` and take ports from holon's port range.
- `make_headers(...)` and `run()` lines changed in `tidb_api.py` drop spaces after commas
  (`make_headers(token=None,service_secret=None,...)`). Run `scripts/lint.sh` before merge.

## What was run

All on groot1, in `/local/tejus/reviews-holon` at `0719df4`, ansible-core 2.16.3.

| Check | Result |
|---|---|
| `ansible-playbook --syntax-check` on the 11 new recipes | 9 pass. `tidb_cleanup_single_server.yml` (YAML load error) and `auth_recovery.yml:668` (argument split error) fail. |
| `python3 cleanup_worker_verify.py --mode multi ...` | File is 0 bytes. Exit 0. |
| Lazy `lookup('pipe','date +%s%N')` play var, two references | Two different values |
| mdsvc-tidb `main` routes and DTOs | `POST /api/events` with `nisd_id`/`action`/`params`. `POST /api/heartbeat` with `EntityID`/`AckEventSerialNo`/`EventLimit`/`TenantUUID`. `DELETE /api/vdev/{id}`. |
| All mdsvc-tidb branches on origin | No `/api/test/event` and no heartbeat capture |
| niova-block `master` `60ed593` | lreg `ncp_event` is a root object with `event_sno`, `ping_enabled`. Only TEST (250) dispatchable. Env var names in `nisd_handler.py` match. |
| holon `post_run.yml` | Fails the run on `terminate_recipe` after cleanup |

Not run: the recipes end to end. With findings 2-6 they cannot pass. Also, the cleanup step in `basic` kills
the first `nisd` on the host. `scripts/lint.sh` was not run because flake8 is not installed on groot1.
