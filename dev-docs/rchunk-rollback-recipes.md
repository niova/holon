# R-chunk ECE / ECRE / mixed-write rollback tests

## Files

- `holon/tidb_recipes/tidb_rchunk_rollback_setup.yml` — starts mdsvc TiDB, creates infrastructure, five NISD backing files, initializes and starts NISDs, and creates the configured E-chunk VDEV.
- `holon/tidb_recipes/tidb_rchunk_ece_rollback.yml`
- `holon/tidb_recipes/tidb_rchunk_ecre_rollback.yml`
- `holon/tidb_recipes/tidb_rchunk_mixed_rollback.yml`
- `holon/scripts/rchunk_rollback.py` — crash, restart and post-crash I/O tests.

## E-chunk VDEV

```json
{"size_bytes":17179869184,"redundancy":1,"data_blk_cnt":3,"parity_blk_cnt":2}
```

The default setup provisions five NISDs with distinct UUIDs/ports on one host. **Check cluster placement and infra topology**: distinct NISDs do not necessarily provide five distinct failure domains. Change `rchunk_nisds`, backing sizes, and `rchunk_infra_payload` to match the actual E-chunk allocation requirements. The example infrastructure payload must exist in the repository.

## Workloads

| Recipe | In-flight workload | Recovery verification |
| --- | --- | --- |
| ECE | 512 KiB random writes | 4 KiB deterministic write, CRC read-back |
| ECRE | 4 KiB random writes | 4 KiB deterministic write, CRC read-back |
| Mixed | `fio --bssplit` distribution: 4, 16, 64, 128, 256 KiB (20% each) | 4 KiB deterministic write, CRC read-back |

Workload writes use offset 0 and 256 MiB length. Post-crash verification uses a **separate** 64 MiB region at 512 MiB, so a crash does not invalidate expected verification metadata. The test validates continued I/O after recovery, not correctness of partially completed writes.

Each attempt starts `niova-ublk`, waits for exactly one new `/dev/ublkbN`, starts `fio` in the background, verifies it is still active, kills client with `SIGKILL` (no graceful termination), waits for device disappearance, restarts client for the same VDEV, checks the counter when configured, and runs new write/read verification. Repeated attempts are bounded (`rchunk_max_attempts`, default 6) **when the counter is configured**.

## Counter integration (deferred)

Set `rchunk_rollback_counter_command` in a child playbook to a command querying the **restarted** client's ctl-interface, and adjust `rchunk_rollback_counter_regex` if needed. The command must print an integer counter value for the current VDEV/client. Confirm whether the counter is persistent or cumulative; if cumulative, implement a before/after delta instead of checking merely `> 0`.

Without this command the helper runs a single crash/recovery attempt and prints `RECOVERY_ONLY`, which means rollback itself is **not verified**. This is an intentionally partial test. With a configured command the script retries until the counter is positive, then prints `ROLLBACK_PASS`. Set `rchunk_require_rollback_counter: true` once the query is ready to require that success marker. The Python helper also supports `--require-counter` directly.

## Run

From your normal Holon Ansible invocation, run a child playbook (which imports the common setup), e.g.:

```bash
ansible-playbook holon/tidb_recipes/tidb_rchunk_ece_rollback.yml -e app_type=tidb
ansible-playbook holon/tidb_recipes/tidb_rchunk_ecre_rollback.yml -e app_type=tidb
ansible-playbook holon/tidb_recipes/tidb_rchunk_mixed_rollback.yml -e app_type=tidb
```

Supply your normal `ClusterParams`, lookup-plugin configuration and environment. Set `NIOVA_WORKSPACE`, `NIOVA_BIN_PATH`, and control-plane auth credentials as required by existing recipes. Run on an **isolated test machine** with working ublk support, root permissions, `fio`, and enough free storage. Existing `/dev/ublkb*` devices cause an immediate refusal rather than risking unrelated devices.

## Caveats / required integration verification

1. The VDEV layout requires allocator-compatible infrastructure; the supplied `create_sample_infra.json` was not included here, and may need replacement.
2. The default five NISD UUIDs and ports are examples. Confirm the required NISD count, failure-domain topology, and whether each NISD backing file can be sparse on the runner.
3. Ublk devices must disappear after `SIGKILL` for restart. The script deliberately refuses global `ublk del -a`. On a stale device, isolate and clean it up manually before rerun.
4. The helper starts a new `niova-ublk` process with the same CP-mode CLI switches used by `nisd_handler.py`; it does not update the handler's recipe JSON process registry. This separation is intentional because the helper owns the crash loop.
5. These files are **not integration-tested** here. Run against Niova and adjust client/device-lifecycle behavior or ioctl/ctl paths as needed.
6. The test does not tear down the mdsvc TiDB stack or NISDs automatically. Use your existing environment teardown after collecting logs; be aware that `docker_teardown` deletes persisted TiDB data.