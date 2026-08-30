"""
Ansible Lookup Plugin: tidb_setup
=====================================
Manages the lifecycle of an mdsvc-tidb test environment using Docker,
manual/pre-existing TiDB, or TiUP Playground.

Backends
--------

Docker:
  - `docker compose up -d --build` starts TiDB + mdsvc-api.

Manual:
  - Assumes TiDB/MySQL is already running.
  - This plugin verifies reachability and starts mdsvc-api only.

TiUP Playground:
  - Starts a local multi-node TiDB cluster with `tiup playground`.
  - Default playground shape is 1 PD, 3 TiKV, 1 TiDB, 0 TiFlash.
  - Playground is launched as a detached supervisor process and its PID is
    recorded so the lookup plugin can tear it down later.
  - Per-node lifecycle actions are implemented by locating the component PID
    inside the playground process tree. Before a stop/kill, the component's
    original command line is saved. `node_start` restarts that exact command,
    preserving the TiKV data directory/store identity for stale-replica tests.

Useful playground ClusterParams
-------------------------------

  tidb_backend: playground
  tidb_version: v8.5.0
  playground_host: 127.0.0.1
  playground_pd: 1
  playground_kv: 3
  playground_db: 1
  playground_tiflash: 0
  playground_without_monitor: true

Optional component config files:

  playground_pd_config: /path/to/pd.toml
  playground_tikv_config: /path/to/tikv.toml
  playground_tidb_config: /path/to/tidb.toml

Optional persistent playground tag:

  playground_tag: my-test-cluster

If a tag is omitted, normal TiUP Playground cleanup semantics apply when the
playground supervisor exits. A tag should only be used when persistence across
full playground restarts is explicitly required.
"""

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import json
import os
import signal
import subprocess
import requests
import time

# =========================================================
# Shared logging helpers
# =========================================================

def _get_log_file(params):
    """Resolve the log file to write status/health-check output to."""
    if "log_file" in params:
        return params["log_file"]

    base_dir = params.get("base_dir")
    raft_uuid = params.get("raft_uuid")
    app_name = params.get("app_type", "mdsvc")

    if base_dir and raft_uuid:
        return "%s/%s/%s_health_log.txt" % (base_dir, raft_uuid, app_name)

    return "/tmp/mdsvc_cluster_health_log.txt"

def _write_log_header(logf, message):
    logf.write("\n==== %s ====\n" % message)

def docker_logs(params):
    """Capture recent Docker logs for mdsvc-tidb."""
    log_file = _get_log_file(params)
    container_name = params.get("container_name", "mdsvc-tidb")

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    with open(log_file, "a") as logf:
        _write_log_header(logf, "CAPTURING DOCKER LOGS (%s)" % container_name)
        try:
            proc = subprocess.Popen(
                ["sudo", "docker", "logs", "--tail", "200", container_name],
                stdout=logf,
                stderr=logf,
            )
            proc.wait()
        except Exception as exc:
            logf.write("Unable to capture docker logs: %s\n" % str(exc))

# =========================================================
# Backend selection
# =========================================================

def _tidb_backend(cluster_params):
    backend = str(cluster_params.get("tidb_backend", "docker")).strip().lower()

    if backend not in ("docker", "playground"):
        raise AnsibleError(
            "Unsupported tidb_backend=%r; expected 'docker' or 'playground'"
            % backend
        )
    return backend

# =========================================================
# Docker Setup
# =========================================================

def docker_setup(cluster_params):
    """Tear down existing stack, start Docker stack, and wait for API health."""

    workspace_dir = os.getenv("NIOVA_WORKSPACE")
    repo_path = "%s/mdsvc-tidb" % workspace_dir

    base_dir = cluster_params["base_dir"]
    app_name = cluster_params["app_type"]
    raft_uuid = cluster_params["raft_uuid"]

    log_file = "%s/%s/%s_docker_log.txt" % (base_dir, raft_uuid, app_name)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    container_name = "mdsvc-tidb"
    base_url = cluster_params.get("api_base_url", "http://localhost:8081")
    server_timeout = int(cluster_params.get("server_timeout", 120))

    with open(log_file, "a") as logf:
        logf.write("\nSTARTING MDSVC-TIDB DOCKER SETUP\n")

        down_proc = subprocess.Popen(
            ["sudo", "docker", "compose", "down", "-v"],
            cwd=repo_path,
            stdout=logf,
            stderr=logf,
        )
        down_rc = down_proc.wait()
        if down_rc != 0:
            raise AnsibleError(
                "docker compose down failed. Check log: %s" % log_file
            )

        logf.write("\nDOCKER STACK CLEANED UP\n")

        up_proc = subprocess.Popen(
            ["sudo", "docker", "compose", "up", "-d", "--build"],
            cwd=repo_path,
            stdout=logf,
            stderr=logf,
        )
        up_rc = up_proc.wait()
        if up_rc != 0:
            raise AnsibleError(
                "docker compose up failed. Check log: %s" % log_file
            )

        logf.write("\nDOCKER STACK STARTED\n")

    wait_for_server({
        "base_url": base_url,
        "server_timeout": server_timeout,
        "log_file": log_file,
        "container_name": container_name,
    })

    with open(log_file, "a") as logf:
        logs_proc = subprocess.Popen(
            ["sudo", "docker", "logs", "-f", container_name],
            stdout=logf,
            stderr=logf,
            start_new_session=True,
        )
        logf.write(
            "\nBACKGROUND LOG STREAM STARTED for %s pid=%d\n"
            % (container_name, logs_proc.pid)
        )

    return {
        "status": "docker_setup_done",
        "container_name": container_name,
        "log_pid": logs_proc.pid,
        "log_file": log_file,
        "base_url": base_url,
    }

def docker_teardown(cluster_params):
    """Stop and remove the mdsvc-tidb Docker stack and its volumes."""

    workspace_dir = os.getenv("NIOVA_WORKSPACE")
    repo_path = "%s/mdsvc-tidb" % workspace_dir

    base_dir = cluster_params["base_dir"]
    app_name = cluster_params["app_type"]
    raft_uuid = cluster_params["raft_uuid"]

    log_file = "%s/%s/%s_docker_log.txt" % (base_dir, raft_uuid, app_name)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    with open(log_file, "a") as logf:
        logf.write("\nSTARTING MDSVC-TIDB DOCKER TEARDOWN\n")

        down_proc = subprocess.Popen(
            ["sudo", "docker", "compose", "down", "-v"],
            cwd=repo_path,
            stdout=logf,
            stderr=logf,
        )
        down_rc = down_proc.wait()
        if down_rc != 0:
            raise AnsibleError(
                "docker compose down failed. Check log: %s" % log_file
            )

        logf.write("\nDOCKER STACK STOPPED AND REMOVED\n")

    return {
        "status": "docker_teardown_done",
        "log_file": log_file,
    }

# =========================================================
# Manual Setup Helpers
# =========================================================

def manual_setup(cluster_params):
    """Verify pre-existing TiDB and start mdsvc-api against it."""

    workspace_dir = os.getenv("NIOVA_WORKSPACE")
    repo_path = "%s/mdsvc-tidb" % workspace_dir

    base_dir = cluster_params["base_dir"]
    app_name = cluster_params["app_type"]
    raft_uuid = cluster_params["raft_uuid"]

    log_file = "%s/%s/%s_manual_log.txt" % (base_dir, raft_uuid, app_name)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    mysql_host = cluster_params.get("mysql_host", "127.0.0.1")
    mysql_port = str(cluster_params.get("mysql_port", "4000"))
    mysql_user = cluster_params.get("mysql_user", "root")
    mysql_password = cluster_params.get("mysql_password", "")

    with open(log_file, "a") as logf:
        logf.write("\nSTARTING MANUAL MDSVC-TIDB SETUP\n")
        logf.write(
            "Assuming TiDB/MySQL is already running at %s:%s.\n"
            % (mysql_host, mysql_port)
        )
        logf.write("Verifying TiDB is reachable...\n")

        tidb_ready = False
        for i in range(1, 31):
            mysql_cmd = [
                "mysql", "-h", mysql_host, "-P", mysql_port, "-u", mysql_user,
            ]
            if mysql_password:
                mysql_cmd.append("-p%s" % mysql_password)
            mysql_cmd += ["-e", "SHOW DATABASES;"]

            check_rc = subprocess.Popen(
                mysql_cmd,
                stdout=logf,
                stderr=logf,
            ).wait()

            if check_rc == 0:
                logf.write("TiDB is reachable!\n")
                tidb_ready = True
                break

            logf.write("Still waiting for TiDB... (%d)\n" % i)
            time.sleep(10)

        if not tidb_ready:
            raise AnsibleError(
                "Could not reach TiDB/MySQL at %s:%s within timeout. "
                "Check log: %s" % (mysql_host, mysql_port, log_file)
            )

        logf.write("\nMANUAL MDSVC-TIDB PRE-CHECK COMPLETED\n")

    base_url = cluster_params.get("api_base_url", "http://localhost:8081")
    server_timeout = int(cluster_params.get("server_timeout", 120))

    server_result = start_server({
        "server_path": repo_path,
        "pid_file": "%s/%s/mdsvc_server.pid" % (base_dir, raft_uuid),
        "mysql_host": mysql_host,
        "mysql_port": mysql_port,
        "mysql_user": mysql_user,
        "mysql_password": mysql_password,
        "base_url": base_url,
        "disable_auth": cluster_params.get("disable_auth", False),
        "jwt_secret": cluster_params.get("jwt_secret"),
        "tenant_admin_username": cluster_params.get("tenant_admin_username"),
        "tenant_admin_password": cluster_params.get("tenant_admin_password"),
        "admin_default_username": cluster_params.get("admin_default_username"),
        "admin_default_password": cluster_params.get("admin_default_password"),
    })

    wait_for_server({
        "base_url": base_url,
        "server_timeout": server_timeout,
        "log_file": log_file,
        "server_pid": server_result["pid"],
        "server_log_file": server_result["log_file"],
    })

    return {
        "status": "manual_setup_done",
        "server_pid": server_result["pid"],
        "log_file": log_file,
        "server_log_file": server_result["log_file"],
        "base_url": base_url,
    }

def manual_teardown(cluster_params):
    """Stop mdsvc-api only; manual mode never started TiDB."""
    base_dir = cluster_params["base_dir"]
    raft_uuid = cluster_params["raft_uuid"]
    pid_dir = "%s/%s" % (base_dir, raft_uuid)

    server_status = stop_server({"pid_file": "%s/mdsvc_server.pid" % pid_dir})

    return {
        "status": "manual_teardown_done",
        "server": server_status,
    }

# =========================================================
# TiUP Playground lifecycle
# =========================================================

def _tiup_bin(cluster_params):
    return cluster_params.get("tiup_bin", os.path.expanduser("~/.tiup/bin/tiup"))

def _playground_pid_file(cluster_params):
    return "%s/%s/tiup_playground.pid" % (
        cluster_params["base_dir"],
        cluster_params["raft_uuid"],
    )

def _playground_state_file(cluster_params):
    return "%s/%s/tiup_playground_nodes.json" % (
        cluster_params["base_dir"],
        cluster_params["raft_uuid"],
    )

def _playground_log_file(cluster_params):
    return "%s/%s/%s_playground_log.txt" % (
        cluster_params["base_dir"],
        cluster_params["raft_uuid"],
        cluster_params["app_type"],
    )

def _read_pid_file(pid_file):
    if not os.path.exists(pid_file):
        return None
    try:
        with open(pid_file, "r") as fp:
            value = fp.read().strip()
        return int(value) if value else None
    except Exception:
        return None

def _pid_exists(pid):
    if not pid:
        return False
    pid = int(pid)
    try:
        # Treat zombies as exited. os.kill(pid, 0) succeeds for a zombie,
        # which would otherwise make stop/kill waits run until timeout.
        with open("/proc/%d/stat" % pid, "r") as fp:
            fields = fp.read().split()
        if len(fields) > 2 and fields[2] == "Z":
            return False
    except FileNotFoundError:
        return False
    except Exception:
        pass

    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False

def _wait_pid_exit(pid, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.2)
    return not _pid_exists(pid)

def _proc_cmdline(pid):
    try:
        with open("/proc/%d/cmdline" % int(pid), "rb") as fp:
            raw = fp.read()
        return [
            token.decode("utf-8", errors="replace")
            for token in raw.split(b"\0")
            if token
        ]
    except Exception:
        return []

def _proc_cwd(pid):
    try:
        return os.readlink("/proc/%d/cwd" % int(pid))
    except Exception:
        return None

def _load_playground_state(cluster_params):
    state_file = _playground_state_file(cluster_params)
    if not os.path.exists(state_file):
        return {"nodes": {}}
    try:
        with open(state_file, "r") as fp:
            data = json.load(fp)
        if not isinstance(data, dict):
            return {"nodes": {}}
        data.setdefault("nodes", {})
        return data
    except Exception:
        return {"nodes": {}}

def _save_playground_state(cluster_params, state):
    state_file = _playground_state_file(cluster_params)
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    tmp_file = state_file + ".tmp"
    with open(tmp_file, "w") as fp:
        json.dump(state, fp, indent=2, sort_keys=True)
    os.replace(tmp_file, state_file)

def _component_role_from_cmd(cmd):
    joined = " ".join(cmd)
    if "tikv-server" in joined:
        return "tikv"
    if "pd-server" in joined:
        return "pd"
    if "tidb-server" in joined:
        return "tidb"
    return None

def _get_playground_pgid(cluster_params):
    launcher_pid = _read_pid_file(_playground_pid_file(cluster_params))
    if not launcher_pid or not _pid_exists(launcher_pid):
        raise AnsibleError("TiUP playground is not running")
    try:
        return os.getpgid(launcher_pid)
    except Exception as exc:
        raise AnsibleError(
            "Could not determine TiUP playground process group: %s" % exc
        )

def _ps_processes():
    proc = subprocess.Popen(
        ["ps", "-eo", "pid=,ppid=,pgid=,stat=,args="],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise AnsibleError("ps failed while locating playground nodes: %s" % stderr)

    result = []
    for line in stdout.splitlines():
        parts = line.strip().split(None, 4)
        if len(parts) < 5:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
            pgid = int(parts[2])
        except ValueError:
            continue
        result.append({
            "pid": pid,
            "ppid": ppid,
            "pgid": pgid,
            "stat": parts[3],
            "args": parts[4],
        })
    return result

def _playground_component_processes(cluster_params):
    """Return live PD/TiKV/TiDB processes belonging to this playground.

    Original playground children are identified by process group. Components
    manually restarted by node_start are also included using the saved state.
    """
    pgid = _get_playground_pgid(cluster_params)
    state = _load_playground_state(cluster_params)
    processes = []
    seen = set()

    ps_items = _ps_processes()
    by_pid = {item["pid"]: item for item in ps_items}
    launcher_pid = _read_pid_file(_playground_pid_file(cluster_params))

    def _is_descendant_of_launcher(item):
        current = item
        visited = set()
        while current and current.get("pid") not in visited:
            visited.add(current.get("pid"))
            parent_pid = current.get("ppid")
            if parent_pid == launcher_pid:
                return True
            current = by_pid.get(parent_pid)
        return False

    for item in ps_items:
        # Most playground children inherit the supervisor process group. Use
        # parent ancestry as an additional match in case TiUP gives a child its
        # own process group.
        if item["pgid"] != pgid and not _is_descendant_of_launcher(item):
            continue
        cmd = _proc_cmdline(item["pid"])
        role = _component_role_from_cmd(cmd)
        if not role:
            continue
        item = dict(item)
        item["cmd"] = cmd
        item["role"] = role
        processes.append(item)
        seen.add(item["pid"])

    for node_id, saved in state.get("nodes", {}).items():
        pid = saved.get("pid")
        if not pid or pid in seen or not _pid_exists(pid):
            continue
        cmd = _proc_cmdline(pid)
        role = _component_role_from_cmd(cmd)
        if not role:
            continue
        processes.append({
            "pid": int(pid),
            "ppid": None,
            "pgid": os.getpgid(int(pid)),
            "stat": "",
            "args": " ".join(cmd),
            "cmd": cmd,
            "role": role,
            "saved_node_id": node_id,
        })
        seen.add(int(pid))

    return processes

def _require_node_id(cluster_params):
    node_id = cluster_params.get("node_id")
    if node_id is None or str(node_id).strip() == "":
        raise AnsibleError(
            "cluster_params['node_id'] is required. For TiKV, pass the "
            "PD-reported store address such as '127.0.0.1:20160'."
        )
    return str(node_id).strip()

def _resolve_playground_node(cluster_params, require_live=True):
    """Resolve a node_id/address/PID to a playground component process."""
    node_id = _require_node_id(cluster_params)

    # Direct PID is useful for diagnostics and explicit recipe control.
    if node_id.isdigit():
        pid = int(node_id)
        if _pid_exists(pid):
            cmd = _proc_cmdline(pid)
            role = _component_role_from_cmd(cmd)
            if role:
                return {
                    "pid": pid,
                    "cmd": cmd,
                    "args": " ".join(cmd),
                    "role": role,
                    "node_id": node_id,
                }

    matches = []
    for proc in _playground_component_processes(cluster_params):
        joined = " ".join(proc.get("cmd") or [])
        saved_node_id = str(proc.get("saved_node_id") or "")
        if node_id == saved_node_id or node_id in joined:
            matches.append(proc)

    if len(matches) == 1:
        result = dict(matches[0])
        result["node_id"] = node_id
        return result

    if len(matches) > 1:
        raise AnsibleError(
            "node_id=%r matched multiple playground processes: %s"
            % (node_id, [(m["pid"], m["role"]) for m in matches])
        )

    if not require_live:
        state = _load_playground_state(cluster_params)
        saved = state.get("nodes", {}).get(node_id)
        if saved:
            result = dict(saved)
            result["node_id"] = node_id
            return result

    raise AnsibleError(
        "Could not find playground node %r. Use list_playground_nodes or "
        "list_tikv_stores to inspect current processes/addresses." % node_id
    )

def _save_node_launch_state(cluster_params, node_id, proc):
    cmd = proc.get("cmd") or _proc_cmdline(proc["pid"])
    if not cmd:
        raise AnsibleError(
            "Could not capture command line for playground node %s (pid=%s)"
            % (node_id, proc["pid"])
        )

    state = _load_playground_state(cluster_params)
    state.setdefault("nodes", {})[node_id] = {
        "pid": int(proc["pid"]),
        "role": proc.get("role") or _component_role_from_cmd(cmd),
        "cmd": cmd,
        "cwd": _proc_cwd(proc["pid"]),
        "last_action": "captured",
    }
    _save_playground_state(cluster_params, state)
    return state["nodes"][node_id]

def _set_saved_node_status(cluster_params, node_id, **updates):
    state = _load_playground_state(cluster_params)
    state.setdefault("nodes", {})
    state["nodes"].setdefault(node_id, {})
    state["nodes"][node_id].update(updates)
    _save_playground_state(cluster_params, state)

def _build_playground_command(cluster_params):
    version = str(cluster_params.get("tidb_version", "v8.5.0"))
    host = str(cluster_params.get("playground_host", "127.0.0.1"))
    pd_count = int(cluster_params.get("playground_pd", 1))
    kv_count = int(cluster_params.get("playground_kv", 3))
    db_count = int(cluster_params.get("playground_db", 1))
    tiflash_count = int(cluster_params.get("playground_tiflash", 0))

    cmd = [
        _tiup_bin(cluster_params),
        "playground",
        version,
        "--host", host,
        "--pd", str(pd_count),
        "--kv", str(kv_count),
        "--db", str(db_count),
        "--tiflash", str(tiflash_count),
    ]

    if cluster_params.get("playground_without_monitor", True):
        cmd.append("--without-monitor")

    tag = cluster_params.get("playground_tag")
    if tag:
        cmd += ["--tag", str(tag)]

    pd_config = cluster_params.get("playground_pd_config")
    if pd_config:
        cmd += ["--pd.config", str(pd_config)]

    tikv_config = cluster_params.get("playground_tikv_config")
    if tikv_config:
        cmd += ["--kv.config", str(tikv_config)]

    tidb_config = cluster_params.get("playground_tidb_config")
    if tidb_config:
        cmd += ["--db.config", str(tidb_config)]

    return cmd

def _wait_for_pd_ready(cluster_params, logf):
    """Wait until PD is answering HTTP requests."""
    pd_url = _get_pd_client_url(cluster_params)
    timeout = int(cluster_params.get("pd_ready_timeout", 90))
    deadline = time.time() + timeout
    last_err = None

    while time.time() < deadline:
        try:
            resp = requests.get("%s/pd/api/v1/health" % pd_url, timeout=5)
            if resp.ok:
                logf.write("PD is ready at %s\n" % pd_url)
                logf.flush()
                return
            last_err = "HTTP %s" % resp.status_code
        except Exception as exc:
            last_err = str(exc)
        time.sleep(2)

    raise AnsibleError(
        "PD did not become ready within %ss at %s. Last error: %s"
        % (timeout, pd_url, last_err)
    )

def _wait_for_tikv_quorum_ready(cluster_params, logf):
    """Wait for all expected TiKV stores and full Region replication."""
    pd_url = _get_pd_client_url(cluster_params)
    expected_stores = int(
        cluster_params.get(
            "quorum_expected_stores",
            cluster_params.get("playground_kv", 3),
        )
    )
    expected_replicas = int(cluster_params.get("quorum_expected_replicas", 3))
    timeout = int(cluster_params.get("quorum_ready_timeout", 240))
    deadline = time.time() + timeout
    last_summary = "not checked"

    while time.time() < deadline:
        try:
            stores = _pd_get_stores(pd_url)
            up_stores = [s for s in stores if s.get("state_name") == "Up"]

            resp = requests.get("%s/pd/api/v1/regions" % pd_url, timeout=10)
            resp.raise_for_status()
            regions = resp.json().get("regions", [])
            under_replicated = [
                region for region in regions
                if len(region.get("peers", [])) < expected_replicas
            ]

            last_summary = (
                "stores_up=%d/%d regions=%d under_replicated=%d"
                % (
                    len(up_stores), expected_stores,
                    len(regions), len(under_replicated),
                )
            )
            logf.write("TiKV quorum readiness: %s\n" % last_summary)
            logf.flush()

            if (
                len(up_stores) == expected_stores
                and len(regions) > 0
                and not under_replicated
            ):
                logf.write(
                    "TiKV quorum ready: %d stores Up; every Region has at least "
                    "%d peers.\n" % (expected_stores, expected_replicas)
                )
                logf.flush()
                return
        except Exception as exc:
            last_summary = "readiness query failed: %s" % str(exc)
            logf.write(last_summary + "\n")
            logf.flush()
        time.sleep(3)

    raise AnsibleError(
        "TiKV quorum did not become ready within %ss: %s"
        % (timeout, last_summary)
    )

def _wait_for_mysql_ready(cluster_params, logf):
    mysql_host = cluster_params.get("mysql_host", "127.0.0.1")
    mysql_port = str(cluster_params.get("mysql_port", "4000"))
    mysql_user = cluster_params.get("mysql_user", "root")
    mysql_password = cluster_params.get("mysql_password", "")
    attempts = int(cluster_params.get("mysql_ready_attempts", 30))
    interval = int(cluster_params.get("mysql_ready_interval", 5))

    logf.write(
        "Waiting for TiDB at %s:%s to accept connections...\n"
        % (mysql_host, mysql_port)
    )
    logf.flush()

    for i in range(1, attempts + 1):
        mysql_cmd = [
            "mysql", "-h", mysql_host, "-P", mysql_port, "-u", mysql_user,
        ]
        if mysql_password:
            mysql_cmd.append("-p%s" % mysql_password)
        mysql_cmd += ["-e", "SHOW DATABASES;"]

        check_rc = subprocess.Popen(
            mysql_cmd,
            stdout=logf,
            stderr=logf,
        ).wait()
        if check_rc == 0:
            logf.write("TiDB is up!\n")
            logf.flush()
            return

        logf.write("Still waiting for TiDB... (%d/%d)\n" % (i, attempts))
        logf.flush()
        time.sleep(interval)

    raise AnsibleError(
        "TiDB did not become reachable at %s:%s. Check log: %s"
        % (mysql_host, mysql_port, _playground_log_file(cluster_params))
    )

def playground_setup(cluster_params):
    """Start a local multi-node cluster with `tiup playground`."""
    base_dir = cluster_params["base_dir"]
    raft_uuid = cluster_params["raft_uuid"]
    log_file = _playground_log_file(cluster_params)
    pid_file = _playground_pid_file(cluster_params)
    state_file = _playground_state_file(cluster_params)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Clean up a playground left by a previous failed recipe invocation.
    old_pid = _read_pid_file(pid_file)
    if old_pid and _pid_exists(old_pid):
        raise AnsibleError(
            "A TiUP playground is already running for this recipe context "
            "(pid=%d). Run teardown first. PID file: %s" % (old_pid, pid_file)
        )
    if os.path.exists(pid_file):
        os.remove(pid_file)
    if os.path.exists(state_file):
        os.remove(state_file)

    cmd = _build_playground_command(cluster_params)

    with open(log_file, "a") as logf:
        logf.write("\nSTARTING TIUP PLAYGROUND\n")
        logf.write("$ %s\n" % " ".join(cmd))
        if cluster_params.get("topo_file"):
            logf.write(
                "NOTE: topo_file=%s is ignored by TiUP Playground. Use "
                "playground_pd/playground_kv/playground_db and component "
                "*.config parameters instead.\n" % cluster_params.get("topo_file")
            )
        logf.flush()

        playground_proc = subprocess.Popen(
            cmd,
            stdout=logf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        with open(pid_file, "w") as fp:
            fp.write(str(playground_proc.pid))

        logf.write("TiUP Playground launcher pid=%d\n" % playground_proc.pid)
        logf.flush()

    try:
        # If TiUP exits immediately, fail early with the playground log path.
        time.sleep(2)
        if playground_proc.poll() is not None:
            raise AnsibleError(
                "tiup playground exited early (rc=%s). Check log: %s"
                % (playground_proc.returncode, log_file)
            )

        with open(log_file, "a") as logf:
            _wait_for_pd_ready(cluster_params, logf)
            _wait_for_tikv_quorum_ready(cluster_params, logf)
            _wait_for_mysql_ready(cluster_params, logf)

        workspace_dir = os.getenv("NIOVA_WORKSPACE")
        repo_path = "%s/mdsvc-tidb" % workspace_dir
        base_url = cluster_params.get("api_base_url", "http://localhost:8081")
        server_timeout = int(cluster_params.get("server_timeout", 120))

        mysql_host = cluster_params.get("mysql_host", "127.0.0.1")
        mysql_port = str(cluster_params.get("mysql_port", "4000"))
        mysql_user = cluster_params.get("mysql_user", "root")
        mysql_password = cluster_params.get("mysql_password", "")

        server_result = start_server({
            "server_path": repo_path,
            "pid_file": "%s/%s/mdsvc_server.pid" % (base_dir, raft_uuid),
            "mysql_host": mysql_host,
            "mysql_port": mysql_port,
            "mysql_user": mysql_user,
            "mysql_password": mysql_password,
            "base_url": base_url,
            "disable_auth": cluster_params.get("disable_auth", False),
            "jwt_secret": cluster_params.get("jwt_secret"),
            "tenant_admin_username": cluster_params.get("tenant_admin_username"),
            "tenant_admin_password": cluster_params.get("tenant_admin_password"),
            "admin_default_username": cluster_params.get("admin_default_username"),
            "admin_default_password": cluster_params.get("admin_default_password"),
        })

        wait_for_server({
            "base_url": base_url,
            "server_timeout": server_timeout,
            "log_file": log_file,
            "server_pid": server_result["pid"],
            "server_log_file": server_result["log_file"],
        })

        return {
            "status": "playground_setup_done",
            "playground_pid": playground_proc.pid,
            "playground_tag": cluster_params.get("playground_tag"),
            "server_pid": server_result["pid"],
            "log_file": log_file,
            "server_log_file": server_result["log_file"],
            "base_url": base_url,
            "pd_count": int(cluster_params.get("playground_pd", 1)),
            "tikv_count": int(cluster_params.get("playground_kv", 3)),
            "tidb_count": int(cluster_params.get("playground_db", 1)),
        }

    except Exception:
        try:
            stop_server({
                "pid_file": "%s/%s/mdsvc_server.pid" % (base_dir, raft_uuid)
            })
        except Exception:
            pass
        try:
            _stop_playground_process(cluster_params, force=True)
        except Exception:
            pass
        raise

def _stop_tracked_restarted_nodes(cluster_params, logf):
    state = _load_playground_state(cluster_params)
    launcher_pid = _read_pid_file(_playground_pid_file(cluster_params))
    launcher_pgid = None
    if launcher_pid and _pid_exists(launcher_pid):
        try:
            launcher_pgid = os.getpgid(launcher_pid)
        except Exception:
            launcher_pgid = None

    for node_id, node in state.get("nodes", {}).items():
        pid = node.get("pid")
        if not pid or not _pid_exists(pid):
            continue
        try:
            pid_pgid = os.getpgid(int(pid))
        except Exception:
            pid_pgid = None

        # Original children will be terminated with the playground process
        # group. Only kill independently restarted components here.
        if launcher_pgid is not None and pid_pgid == launcher_pgid:
            continue

        try:
            logf.write(
                "Stopping independently restarted node %s pid=%s\n"
                % (node_id, pid)
            )
            os.kill(int(pid), signal.SIGTERM)
            if not _wait_pid_exit(int(pid), timeout=10):
                os.kill(int(pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception as exc:
            logf.write(
                "Failed stopping tracked node %s pid=%s: %s\n"
                % (node_id, pid, exc)
            )

def _stop_playground_process(cluster_params, force=False):
    pid_file = _playground_pid_file(cluster_params)
    pid = _read_pid_file(pid_file)
    log_file = _playground_log_file(cluster_params)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    if not pid or not _pid_exists(pid):
        if os.path.exists(pid_file):
            os.remove(pid_file)
        return {"status": "playground_not_running"}

    with open(log_file, "a") as logf:
        _stop_tracked_restarted_nodes(cluster_params, logf)

        try:
            pgid = os.getpgid(pid)
            logf.write(
                "Stopping TiUP Playground pid=%d pgid=%d\n" % (pid, pgid)
            )
            os.killpg(pgid, signal.SIGTERM)

            if not _wait_pid_exit(pid, timeout=20):
                if force:
                    logf.write(
                        "Playground did not stop after SIGTERM; sending SIGKILL.\n"
                    )
                    os.killpg(pgid, signal.SIGKILL)
                    _wait_pid_exit(pid, timeout=5)
                else:
                    raise AnsibleError(
                        "TiUP playground pid=%d did not stop within timeout" % pid
                    )
        except ProcessLookupError:
            pass
        finally:
            if os.path.exists(pid_file):
                os.remove(pid_file)

    return {"status": "playground_stopped", "pid": pid}


def playground_teardown(cluster_params):
    """Stop mdsvc-api and the TiUP Playground supervisor/process tree."""
    base_dir = cluster_params["base_dir"]
    raft_uuid = cluster_params["raft_uuid"]
    pid_dir = "%s/%s" % (base_dir, raft_uuid)
    log_file = _playground_log_file(cluster_params)

    server_status = stop_server({"pid_file": "%s/mdsvc_server.pid" % pid_dir})
    playground_status = _stop_playground_process(cluster_params, force=True)

    # Do not silently delete tagged TiUP data. A tag is explicitly a request
    # for persistence. Recipes that need a fresh cluster should omit the tag.
    tag = cluster_params.get("playground_tag")
    if tag:
        with open(log_file, "a") as logf:
            logf.write(
                "Playground tag %s was used; tagged TiUP data is intentionally "
                "left in place.\n" % tag
            )

    return {
        "status": "playground_teardown_done",
        "server": server_status,
        "playground": playground_status,
        "playground_tag": tag,
    }

def list_playground_nodes(cluster_params):
    """Return locally discovered PD/TiKV/TiDB playground processes."""
    nodes = []
    for proc in _playground_component_processes(cluster_params):
        nodes.append({
            "pid": proc["pid"],
            "role": proc["role"],
            "args": " ".join(proc.get("cmd") or []),
            "stat": proc.get("stat"),
        })
    return {"nodes": nodes}

def node_stop(cluster_params):
    """Gracefully stop one playground component while preserving restart data."""
    node_id = _require_node_id(cluster_params)
    proc = _resolve_playground_node(cluster_params, require_live=True)
    saved = _save_node_launch_state(cluster_params, node_id, proc)
    pid = int(proc["pid"])
    timeout = int(cluster_params.get("node_stop_timeout", 20))

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass

    if not _wait_pid_exit(pid, timeout=timeout):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        _wait_pid_exit(pid, timeout=5)

    _set_saved_node_status(
        cluster_params,
        node_id,
        pid=pid,
        last_action="stopped",
    )

    return {
        "status": "node_stopped",
        "node_id": node_id,
        "pid": pid,
        "role": saved.get("role"),
    }

def node_kill(cluster_params):
    """Hard-kill one playground component and save enough state to restart it."""
    node_id = _require_node_id(cluster_params)
    proc = _resolve_playground_node(cluster_params, require_live=True)
    saved = _save_node_launch_state(cluster_params, node_id, proc)
    pid = int(proc["pid"])

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    _wait_pid_exit(pid, timeout=5)

    _set_saved_node_status(
        cluster_params,
        node_id,
        pid=pid,
        last_action="killed",
    )

    return {
        "status": "node_killed",
        "node_id": node_id,
        "pid": pid,
        "role": saved.get("role"),
    }

def node_start(cluster_params):
    """Restart a previously stopped/killed playground component.

    The original component command line is replayed, so a TiKV node comes back
    with the same data directory and therefore the same store identity.
    """
    node_id = _require_node_id(cluster_params)

    # If it is already live, do not launch a duplicate instance.
    try:
        live = _resolve_playground_node(cluster_params, require_live=True)
        return {
            "status": "node_already_running",
            "node_id": node_id,
            "pid": live["pid"],
            "role": live["role"],
        }
    except AnsibleError:
        pass

    state = _load_playground_state(cluster_params)
    saved = state.get("nodes", {}).get(node_id)
    if not saved:
        raise AnsibleError(
            "No saved launch state exists for node %s. node_start can only "
            "restart a node previously stopped/killed by this plugin."
            % node_id
        )

    cmd = saved.get("cmd")
    if not cmd:
        raise AnsibleError("Saved command line is missing for node %s" % node_id)

    cwd = saved.get("cwd")
    if cwd and not os.path.isdir(cwd):
        cwd = None

    log_file = _playground_log_file(cluster_params)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    with open(log_file, "a") as logf:
        logf.write(
            "\nRESTARTING PLAYGROUND NODE %s\n$ %s\n"
            % (node_id, " ".join(cmd))
        )
        logf.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=logf,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
            start_new_session=True,
        )

    # Detect immediate startup failures without claiming success.
    time.sleep(1)
    if proc.poll() is not None:
        raise AnsibleError(
            "Restarted node %s exited immediately (rc=%s). Check log: %s"
            % (node_id, proc.returncode, log_file)
        )

    _set_saved_node_status(
        cluster_params,
        node_id,
        pid=proc.pid,
        last_action="started",
    )

    return {
        "status": "node_started",
        "node_id": node_id,
        "pid": proc.pid,
        "role": saved.get("role"),
    }

def node_restart(cluster_params):
    """Stop and restart the same playground component/data directory."""
    stop_result = node_stop(cluster_params)
    start_result = node_start(cluster_params)
    return {
        "status": "node_restarted",
        "node_id": _require_node_id(cluster_params),
        "old_pid": stop_result.get("pid"),
        "pid": start_result.get("pid"),
        "role": start_result.get("role"),
    }

# =========================================================
# PD-backed leader lookup
# =========================================================

def _get_pd_client_url(cluster_params):
    return cluster_params.get("pd_client_url", "http://127.0.0.1:2379")

def _pd_get_stores(pd_url):
    """GET /pd/api/v1/stores -> store summaries."""
    resp = requests.get("%s/pd/api/v1/stores" % pd_url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    stores = []
    for entry in data.get("stores", []):
        store = entry.get("store", {})
        status = entry.get("status", {})
        stores.append({
            "id": store.get("id"),
            "address": store.get("address"),
            "state_name": store.get("state_name"),
            "leader_count": status.get("leader_count", 0),
            "region_count": status.get("region_count", 0),
        })
    return stores

def get_region_leader_store(pd_url, region_id):
    resp = requests.get(
        "%s/pd/api/v1/region/id/%s" % (pd_url, region_id),
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    leader = data.get("leader", {})
    return leader.get("store_id")

def get_busiest_leader_store(pd_url):
    stores = _pd_get_stores(pd_url)
    if not stores:
        raise AnsibleError("PD returned no stores; is the cluster up?")
    return max(stores, key=lambda s: s["leader_count"])["id"]

def list_tikv_stores(cluster_params):
    return {"stores": _pd_get_stores(_get_pd_client_url(cluster_params))}

def kill_leader(cluster_params):
    """Find a Region leader (or busiest leader store) and SIGKILL that TiKV."""
    pd_url = _get_pd_client_url(cluster_params)
    region_id = cluster_params.get("region_id")

    if region_id is not None:
        store_id = get_region_leader_store(pd_url, region_id)
        if store_id is None:
            raise AnsibleError("PD reported no leader for region %s" % region_id)
    else:
        store_id = get_busiest_leader_store(pd_url)

    stores = {s["id"]: s for s in _pd_get_stores(pd_url)}
    if store_id not in stores:
        raise AnsibleError("PD store %s disappeared during leader lookup" % store_id)

    store_addr = stores[store_id]["address"]
    mapping = cluster_params.get("store_addr_to_node_id", {})
    node_id = mapping.get(store_addr, store_addr)

    result = node_kill({**cluster_params, "node_id": node_id})
    result["store_id"] = store_id
    result["store_address"] = store_addr
    return result

# =========================================================
# Region-targeted stale-replica recovery helpers
# =========================================================

def _mysql_query(cluster_params, sql):
    """Run a small TiDB/MySQL query and return tab-separated result rows."""
    mysql_host = str(cluster_params.get("mysql_host", "127.0.0.1"))
    mysql_port = str(cluster_params.get("mysql_port", "4000"))
    mysql_user = str(cluster_params.get("mysql_user", "root"))
    mysql_password = str(cluster_params.get("mysql_password", ""))

    cmd = [
        "mysql",
        "-h", mysql_host,
        "-P", mysql_port,
        "-u", mysql_user,
        "--batch",
        "--raw",
        "--skip-column-names",
        "-e", sql,
    ]
    if mysql_password:
        cmd.insert(7, "-p%s" % mysql_password)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise AnsibleError(
            "TiDB query failed (rc=%d): %s\n%s"
            % (proc.returncode, sql, stderr.strip())
        )

    return [line.split("\t") for line in stdout.splitlines() if line.strip()]

def _sql_ident(value):
    return "`%s`" % str(value).replace("`", "``")

def _sql_string(value):
    return "'%s'" % str(value).replace("\\", "\\\\").replace("'", "''")

def _get_table_regions(cluster_params, database, table):
    """Return Region IDs for all record/index key ranges of db.table."""
    sql = (
        "SELECT DISTINCT REGION_ID, IS_INDEX "
        "FROM INFORMATION_SCHEMA.TIKV_REGION_STATUS "
        "WHERE DB_NAME=%s AND TABLE_NAME=%s AND REGION_ID IS NOT NULL "
        "ORDER BY REGION_ID, IS_INDEX"
        % (_sql_string(database), _sql_string(table))
    )
    rows = _mysql_query(cluster_params, sql)

    region_ids = set()
    record_region_ids = set()
    index_region_ids = set()
    for row in rows:
        if not row:
            continue
        region_id = int(row[0])
        is_index = int(row[1]) if len(row) > 1 and row[1] not in ("", "NULL") else 0
        region_ids.add(region_id)
        if is_index:
            index_region_ids.add(region_id)
        else:
            record_region_ids.add(region_id)

    return {
        "rows": rows,
        "region_ids": sorted(region_ids),
        "record_region_ids": sorted(record_region_ids),
        "index_region_ids": sorted(index_region_ids),
    }

def locate_row_region(cluster_params):
    """Locate the table containing a resource row and require one Region."""
    row_id = cluster_params.get("region_row_id")
    if row_id is None or str(row_id) == "":
        raise AnsibleError("cluster_params['region_row_id'] is required")

    key_column = str(cluster_params.get("region_key_column", "id"))
    database = cluster_params.get("region_database")
    table = cluster_params.get("region_table")

    if bool(database) != bool(table):
        raise AnsibleError(
            "region_database and region_table must either both be set or both be omitted"
        )

    if not database:
        candidates = _mysql_query(
            cluster_params,
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM information_schema.COLUMNS "
            "WHERE COLUMN_NAME=%s "
            "AND TABLE_SCHEMA NOT IN "
            "('information_schema','mysql','performance_schema','metrics_schema') "
            "ORDER BY TABLE_SCHEMA, TABLE_NAME"
            % _sql_string(key_column),
        )

        matches = []
        for row in candidates:
            if len(row) < 2:
                continue
            candidate_db, candidate_table = row[0], row[1]
            sql = (
                "SELECT COUNT(*) FROM %s.%s WHERE %s=%s"
                % (
                    _sql_ident(candidate_db),
                    _sql_ident(candidate_table),
                    _sql_ident(key_column),
                    _sql_string(row_id),
                )
            )
            try:
                result = _mysql_query(cluster_params, sql)
            except AnsibleError:
                continue
            if result and result[0] and int(result[0][0]) > 0:
                matches.append((candidate_db, candidate_table))

        if len(matches) != 1:
            raise AnsibleError(
                "Unable to uniquely locate row id=%s. Matching tables=%s. "
                "Set ClusterParams.region_database and region_table explicitly."
                % (row_id, matches)
            )
        database, table = matches[0]

    table_regions = _get_table_regions(cluster_params, database, table)
    table_region_ids = table_regions["region_ids"]
    record_region_ids = table_regions["record_region_ids"]
    index_region_ids = table_regions["index_region_ids"]

    if len(table_region_ids) != 1:
        raise AnsibleError(
            "Single-Raft-group precondition failed for %s.%s: "
            "record_region_ids=%s index_region_ids=%s all_region_ids=%s. "
            "Provide a playground TiKV config that disables the Region split "
            "behavior required by this test before mdsvc creates its schema."
            % (
                database, table, record_region_ids,
                index_region_ids, table_region_ids,
            )
        )

    region_id = int(table_region_ids[0])
    region_state = get_region_state({**cluster_params, "region_id": region_id})

    return {
        "database": database,
        "table": table,
        "row_id": str(row_id),
        "region_id": region_id,
        "leader_store_id": region_state.get("leader_store_id"),
        "peer_store_ids": region_state.get("peer_store_ids", []),
        "record_region_count": len(record_region_ids),
        "index_region_count": len(index_region_ids),
        "table_region_ids": table_region_ids,
        "single_raft_group": len(table_region_ids) == 1,
        "table_region_data": table_regions,
    }

def get_region_state(cluster_params):
    """Return PD's current state for one Region."""
    region_id = cluster_params.get("region_id")
    if region_id is None:
        raise AnsibleError("cluster_params['region_id'] is required")

    pd_url = _get_pd_client_url(cluster_params)
    try:
        resp = requests.get(
            "%s/pd/api/v1/region/id/%s" % (pd_url, region_id),
            timeout=10,
        )
        resp.raise_for_status()
        region = resp.json()
    except Exception as exc:
        raise AnsibleError("Failed to read PD Region %s: %s" % (region_id, exc))

    leader = region.get("leader") or {}
    peers = region.get("peers") or []
    pending_peers = region.get("pending_peers") or []
    down_peers = region.get("down_peers") or []

    def _store_id(peer_or_down):
        peer = peer_or_down.get("peer", peer_or_down)
        return peer.get("store_id") if isinstance(peer, dict) else None

    return {
        "region_id": region.get("id", region.get("region_id", region_id)),
        "leader_store_id": leader.get("store_id"),
        "peer_store_ids": [
            p.get("store_id") for p in peers if p.get("store_id") is not None
        ],
        "pending_peer_store_ids": [
            sid for sid in (_store_id(p) for p in pending_peers) if sid is not None
        ],
        "down_peer_store_ids": [
            sid for sid in (_store_id(p) for p in down_peers) if sid is not None
        ],
        "region": region,
    }

def wait_region_store_ready(cluster_params):
    """Wait until a recovered store is Up and no longer pending/down."""
    region_id = cluster_params.get("region_id")
    store_id = cluster_params.get("target_store_id")
    timeout = int(cluster_params.get("region_recovery_timeout", 300))
    if region_id is None or store_id is None:
        raise AnsibleError(
            "region_id and target_store_id are required for wait_region_store_ready"
        )
    store_id = int(store_id)

    pd_url = _get_pd_client_url(cluster_params)
    deadline = time.time() + timeout
    last_state = None

    while time.time() < deadline:
        stores = {
            int(s["id"]): s
            for s in _pd_get_stores(pd_url)
            if s.get("id") is not None
        }
        state = get_region_state({**cluster_params, "region_id": region_id})
        last_state = state
        store = stores.get(store_id)
        if (
            store
            and store.get("state_name") == "Up"
            and store_id in [int(x) for x in state["peer_store_ids"]]
            and store_id not in [int(x) for x in state["pending_peer_store_ids"]]
            and store_id not in [int(x) for x in state["down_peer_store_ids"]]
        ):
            return {
                "status": "region_store_ready",
                "store": store,
                "region": state,
            }
        time.sleep(2)

    raise AnsibleError(
        "Recovered store %s did not become ready for Region %s within %ss. "
        "Last Region state: %s"
        % (store_id, region_id, timeout, last_state)
    )

def _ctl_component_version(cluster_params):
    version = str(cluster_params.get("tidb_version", "v8.5.0")).strip()
    return version if version.startswith("v") else "v" + version

def transfer_region_leader(cluster_params):
    """Transfer one Region leader to target_store_id with PD Control."""
    region_id = cluster_params.get("region_id")
    target_store_id = cluster_params.get("target_store_id")
    timeout = int(cluster_params.get("leader_transfer_timeout", 180))
    if region_id is None or target_store_id is None:
        raise AnsibleError(
            "region_id and target_store_id are required for transfer_region_leader"
        )

    region_id = int(region_id)
    target_store_id = int(target_store_id)
    pd_url = _get_pd_client_url(cluster_params)
    tiup = _tiup_bin(cluster_params)
    component = "ctl:%s" % _ctl_component_version(cluster_params)
    log_file = _get_log_file(cluster_params)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    deadline = time.time() + timeout
    last_output = ""
    last_rc = None

    while time.time() < deadline:
        state = get_region_state({**cluster_params, "region_id": region_id})
        if state.get("leader_store_id") == target_store_id:
            return {
                "status": "leader_transferred",
                "region_id": region_id,
                "leader_store_id": target_store_id,
                "region": state,
            }

        cmd = [
            tiup,
            component,
            "pd",
            "-u", pd_url,
            "operator", "add", "transfer-leader",
            str(region_id), str(target_store_id),
        ]
        with open(log_file, "a") as logf:
            logf.write("\n$ %s\n" % " ".join(cmd))
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            output, _ = proc.communicate()
            last_rc = proc.returncode
            last_output = output.strip()
            logf.write(output)
            logf.flush()

        for _ in range(5):
            if time.time() >= deadline:
                break
            time.sleep(1)
            state = get_region_state({**cluster_params, "region_id": region_id})
            if state.get("leader_store_id") == target_store_id:
                return {
                    "status": "leader_transferred",
                    "region_id": region_id,
                    "leader_store_id": target_store_id,
                    "region": state,
                    "pd_ctl_rc": last_rc,
                    "pd_ctl_output": last_output,
                }
        time.sleep(2)

    raise AnsibleError(
        "Failed to transfer Region %s leader to store %s within %ss. "
        "Last pd-ctl rc=%s output=%s"
        % (region_id, target_store_id, timeout, last_rc, last_output)
    )

# =========================================================
# Shared mdsvc-api start/stop
# =========================================================

def _tail_file(path, max_lines=80):
    """Return the tail of a log file for actionable Ansible errors."""
    if not path or not os.path.exists(path):
        return "<log file not found: %s>" % path
    try:
        with open(path, "r", errors="replace") as fp:
            lines = fp.readlines()
        return "".join(lines[-max_lines:]).rstrip()
    except Exception as exc:
        return "<unable to read %s: %s>" % (path, exc)

def start_server(params):
    """Launch mdsvc-api as a detached background process and detect early exit."""
    server_path = params["server_path"]
    pid_file = params["pid_file"]

    if not os.path.isdir(server_path):
        raise AnsibleError("mdsvc server_path does not exist: %s" % server_path)
    if not os.path.exists(os.path.join(server_path, "go.mod")):
        raise AnsibleError("mdsvc server_path has no go.mod: %s" % server_path)

    log_file = os.path.join(server_path, "mdsvc.log")
    fp = open(log_file, "a")

    env = os.environ.copy()
    env.update({
        "MDSVC_MYSQL_HOST": str(params["mysql_host"]),
        "MDSVC_MYSQL_PORT": str(params["mysql_port"]),
        "MDSVC_MYSQL_USER": str(params["mysql_user"]),
        "MDSVC_MYSQL_PASSWORD": str(params["mysql_password"]),
        "MDSVC_API_URL": str(params["base_url"]),
    })

    if params.get("disable_auth"):
        env["DISABLE_AUTH"] = "true"
    if params.get("jwt_secret"):
        env["JWT_SECRET"] = str(params["jwt_secret"])
    if params.get("tenant_admin_username"):
        env["TENANT_ADMIN_USERNAME"] = str(params["tenant_admin_username"])
    if params.get("tenant_admin_password"):
        env["TENANT_ADMIN_PASSWORD"] = str(params["tenant_admin_password"])
    if params.get("admin_default_username"):
        env["ADMIN_DEFAULT_USERNAME"] = str(params["admin_default_username"])
    if params.get("admin_default_password"):
        env["ADMIN_DEFAULT_PASSWORD"] = str(params["admin_default_password"])

    command = params.get("server_command") or ["go", "run", "./cmd/server"]
    if isinstance(command, str):
        command = command.split()

    fp.write("\n==== STARTING MDSVC-API ====\n")
    fp.write("cwd=%s\n" % server_path)
    fp.write("command=%s\n" % " ".join(command))
    fp.write(
        "mysql=%s:%s user=%s api=%s disable_auth=%s\n"
        % (
            params["mysql_host"],
            params["mysql_port"],
            params["mysql_user"],
            params["base_url"],
            bool(params.get("disable_auth")),
        )
    )
    fp.flush()

    proc = subprocess.Popen(
        command,
        cwd=server_path,
        stdout=fp,
        stderr=fp,
        env=env,
        preexec_fn=os.setsid,
    )

    os.makedirs(os.path.dirname(pid_file), exist_ok=True)
    with open(pid_file, "w") as pidf:
        pidf.write(str(proc.pid))

    fp.write("mdsvc-api launcher pid=%d\n" % proc.pid)
    fp.flush()

    time.sleep(float(params.get("server_early_exit_check", 2)))
    rc = proc.poll()
    if rc is not None:
        fp.close()
        if os.path.exists(pid_file):
            os.remove(pid_file)
        raise AnsibleError(
            "mdsvc-api exited before becoming ready (rc=%s). Log: %s\n"
            "---- mdsvc.log tail ----\n%s"
            % (rc, log_file, _tail_file(log_file))
        )

    fp.close()
    return {
        "status": "server_started",
        "pid": proc.pid,
        "log_file": log_file,
    }

def stop_server(params):
    """Send SIGTERM to the mdsvc-api process group recorded in pid_file."""
    pid_file = params["pid_file"]

    if not os.path.exists(pid_file):
        return {"status": "server_not_running"}

    with open(pid_file, "r") as pidf:
        value = pidf.read().strip()
    if not value:
        os.remove(pid_file)
        return {"status": "server_not_running"}
    pid = int(value)

    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except ProcessLookupError:
        pass
    except Exception as exc:
        raise AnsibleError("Failed to stop server (pid=%s): %s" % (pid, exc))
    finally:
        if os.path.exists(pid_file):
            os.remove(pid_file)

    return {"status": "server_stopped", "pid": pid}

# =========================================================
# Health check helper
# =========================================================

def wait_for_server(params):
    """Poll base_url while detecting an mdsvc process that exits early."""
    base_url = params["base_url"]
    timeout = int(params["server_timeout"])
    log_file = _get_log_file(params)
    server_pid = params.get("server_pid")
    server_log_file = params.get("server_log_file")
    last_err = None

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    for _ in range(timeout):
        if server_pid and not _pid_exists(int(server_pid)):
            raise AnsibleError(
                "mdsvc-api process pid=%s exited before %s became ready. "
                "Server log: %s\n---- mdsvc.log tail ----\n%s"
                % (
                    server_pid,
                    base_url,
                    server_log_file,
                    _tail_file(server_log_file),
                )
            )

        try:
            resp = requests.get(base_url, timeout=5)
            if resp.status_code < 500:
                with open(log_file, "a") as logf:
                    _write_log_header(logf, "SERVER BECAME READY")
                return
            last_err = "HTTP %s" % resp.status_code
        except Exception as exc:
            last_err = str(exc)

        time.sleep(1)

    if params.get("container_name"):
        docker_logs(params)

    server_tail = ""
    if server_log_file:
        server_tail = "\n---- mdsvc.log tail ----\n%s" % _tail_file(server_log_file)

    raise AnsibleError(
        "Server at %s did not become ready within %ss. Last error: %s. "
        "Health log: %s. Server log: %s%s"
        % (base_url, timeout, last_err, log_file, server_log_file, server_tail)
    )

# =========================================================
# Generic backend-aware setup / teardown
# =========================================================

def setup(cluster_params):
    backend = _tidb_backend(cluster_params)
    if backend == "docker":
        return docker_setup(cluster_params)
    return playground_setup(cluster_params)

def teardown(cluster_params):
    backend = _tidb_backend(cluster_params)
    if backend == "docker":
        return docker_teardown(cluster_params)
    return playground_teardown(cluster_params)

# =========================================================
# Lookup Entry Point
# =========================================================

class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):
        action = terms[0]
        cluster_params = variables["ClusterParams"]

        os.environ["NIOVA_THREAD_COUNT"] = cluster_params["nthreads"]

        if action == "setup":
            result = setup(cluster_params)
        elif action == "teardown":
            result = teardown(cluster_params)
        elif action == "docker_setup":
            result = docker_setup(cluster_params)
        elif action == "docker_teardown":
            result = docker_teardown(cluster_params)
        elif action == "manual_setup":
            result = manual_setup(cluster_params)
        elif action == "manual_teardown":
            result = manual_teardown(cluster_params)
        elif action == "playground_setup":
            result = playground_setup(cluster_params)
        elif action == "playground_teardown":
            result = playground_teardown(cluster_params)
        elif action == "list_playground_nodes":
            result = list_playground_nodes(cluster_params)

        elif action == "node_stop":
            result = node_stop(cluster_params)
        elif action == "node_start":
            result = node_start(cluster_params)
        elif action == "node_restart":
            result = node_restart(cluster_params)
        elif action == "node_kill":
            result = node_kill(cluster_params)
        elif action == "kill_leader":
            result = kill_leader(cluster_params)
        elif action == "list_tikv_stores":
            result = list_tikv_stores(cluster_params)
        elif action == "locate_row_region":
            result = locate_row_region(cluster_params)
        elif action == "get_region_state":
            result = get_region_state(cluster_params)
        elif action == "wait_region_store_ready":
            result = wait_region_store_ready(cluster_params)
        elif action == "transfer_region_leader":
            result = transfer_region_leader(cluster_params)
        else:
            raise AnsibleError("Unsupported action: %s" % action)

        return [result]
