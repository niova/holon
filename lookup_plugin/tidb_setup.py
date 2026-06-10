"""
Ansible Lookup Plugin: mdsvc_cluster
=====================================
Manages the lifecycle of an mdsvc-tidb cluster — both the Docker-based
deployment and the manual (bring-your-own-TiDB) deployment.
"""

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import os
import signal
import socket
import subprocess
import requests
import time
from datetime import datetime


# =========================================================
# Parameter Resolution
# =========================================================

def _resolve(kwargs, key, env_var, default):
    """
    Resolve a single parameter.

    Priority: kwarg → environment variable → default.
    """
    if key in kwargs and kwargs[key] is not None:
        return kwargs[key]
    return os.environ.get(env_var, default)


def _build_params(kwargs):
    """
    Build the full cluster_params dict from kwargs + env + defaults.
    All consumers use this dict so nothing is scattered in caller code.
    """
    repo_path = _resolve(
        kwargs, "repo_path", "MDSVC_REPO_PATH",
        os.getcwd()
    )

    # server_path falls back to repo_path when not set independently
    server_path = _resolve(
        kwargs, "server_path", "MDSVC_SERVER_PATH",
        repo_path
    )

    log_dir_raw = _resolve(
        kwargs, "log_dir", "MDSVC_LOG_DIR",
        os.path.join(repo_path, "logs")
    )

    # Allow relative log_dir to be anchored to repo_path
    log_dir = (
        log_dir_raw
        if os.path.isabs(log_dir_raw)
        else os.path.join(repo_path, log_dir_raw)
    )

    return {
        "repo_path": repo_path,
        "server_path": server_path,
        "log_dir": log_dir,

        "base_url": _resolve(
            kwargs, "base_url", "MDSVC_API_URL",
            "http://localhost:8081"
        ),

        "container_name": _resolve(
            kwargs, "container_name", "MDSVC_CONTAINER_NAME",
            "mdsvc-tidb"
        ),

        "pid_file": _resolve(
            kwargs, "pid_file", "MDSVC_PID_FILE",
            "/tmp/mdsvc_server.pid"
        ),

        "server_timeout": int(_resolve(
            kwargs, "server_timeout", "MDSVC_SERVER_TIMEOUT",
            "120"
        )),

        "mysql_host": _resolve(
            kwargs, "mysql_host", "MDSVC_MYSQL_HOST",
            "127.0.0.1"
        ),

        "mysql_port": _resolve(
            kwargs, "mysql_port", "MDSVC_MYSQL_PORT",
            "4000"
        ),

        "mysql_user": _resolve(
            kwargs, "mysql_user", "MDSVC_MYSQL_USER",
            "root"
        ),

        "mysql_password": _resolve(
            kwargs, "mysql_password", "MDSVC_MYSQL_PASSWORD",
            ""
        ),

        "disable_auth": _resolve(
            kwargs, "disable_auth", "DISABLE_AUTH",
            "false"
        ).lower() in ("true", "1", "yes"),
    }


# =========================================================
# Logging Helpers
# =========================================================

def _get_log_file(params):
    log_dir = params["log_dir"]
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "mdsvc_cluster.log")


def _write_log_header(logf, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logf.write("\n")
    logf.write("=" * 80 + "\n")
    logf.write(f"[{timestamp}] {message}\n")
    logf.write("=" * 80 + "\n")
    logf.flush()


# =========================================================
# Command Execution Helper
# =========================================================

def _run(cmd, cwd=None, env=None, logf=None, check=True):
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        stdout=logf if logf else subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        text=True,
        check=check,
    )


# =========================================================
# Docker Helpers
# =========================================================

def _docker_compose_cmd():
    """Return ['docker', 'compose'] or ['docker-compose'] depending on what is available."""
    try:
        subprocess.check_call(
            ["docker", "compose", "version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return ["docker", "compose"]
    except Exception:
        return ["docker-compose"]


def docker_up(params):
    """Tear down any existing stack then bring it up fresh."""
    repo_path = params["repo_path"]
    log_file  = _get_log_file(params)
    compose   = _docker_compose_cmd()

    with open(log_file, "a") as logf:
        _write_log_header(logf, "STARTING MDSVC-TIDB DOCKER STACK")

        _run(
            ["sudo"] + compose + ["down", "-v"],
            cwd=repo_path,
            logf=logf,
            check=False,   # tolerate "nothing to stop"
        )

        _run(
            ["sudo"] + compose + ["up", "-d", "--build"],
            cwd=repo_path,
            logf=logf,
        )

        _write_log_header(logf, "DOCKER STACK STARTED")

    return {"status": "docker_started", "log_file": log_file}


def docker_down(params):
    """Stop the stack and remove volumes."""
    repo_path = params["repo_path"]
    log_file  = _get_log_file(params)
    compose   = _docker_compose_cmd()

    with open(log_file, "a") as logf:
        _write_log_header(logf, "STOPPING MDSVC-TIDB DOCKER STACK")

        _run(
            ["sudo"] + compose + ["down", "-v"],
            cwd=repo_path,
            logf=logf,
            check=False,
        )

        _write_log_header(logf, "DOCKER STACK STOPPED")

    return {"status": "docker_stopped", "log_file": log_file}


def docker_logs(params, follow=False):
    """Dump container logs into the plugin log file."""
    container = params["container_name"]
    log_file  = _get_log_file(params)

    cmd = ["sudo", "docker", "logs"]
    if follow:
        cmd.append("-f")
    cmd.append(container)

    with open(log_file, "a") as logf:
        _write_log_header(logf, f"DOCKER LOGS: {container}")
        _run(cmd, logf=logf, check=False)
        _write_log_header(logf, f"END DOCKER LOGS: {container}")

    return {"status": "docker_logs_collected", "log_file": log_file}


# =========================================================
# Manual Setup Helpers
# =========================================================

def check_db(params):
    """Verify that the TiDB / MySQL port is reachable."""
    host = params["mysql_host"]
    port = int(params["mysql_port"])

    sock = socket.socket()
    try:
        sock.settimeout(3)
        sock.connect((host, port))
    except Exception as e:
        raise AnsibleError(
            f"Database not reachable at {host}:{port} → {e}"
        )
    finally:
        sock.close()


def create_schema(params):
    """Run scripts/run_schema.sh to initialise the mdsvc schema."""
    repo_path   = params["repo_path"]
    script_path = os.path.join(repo_path, "scripts", "run_schema.sh")

    if not os.path.exists(script_path):
        raise AnsibleError(f"Schema script not found: {script_path}")

    env = os.environ.copy()
    env.update({
        "MDSVC_MYSQL_HOST":     str(params["mysql_host"]),
        "MDSVC_MYSQL_PORT":     str(params["mysql_port"]),
        "MDSVC_MYSQL_USER":     str(params["mysql_user"]),
        "MDSVC_MYSQL_PASSWORD": str(params["mysql_password"]),
    })

    _run(["chmod", "+x", script_path])
    _run([script_path], cwd=repo_path, env=env)

    return {"status": "schema_created"}


def start_server(params):
    """
    Launch `go run ./cmd/server` as a detached background process.
    The process group ID is written to pid_file for later cleanup.
    """
    server_path = params["server_path"]
    pid_file    = params["pid_file"]

    log_file = os.path.join(server_path, "mdsvc.log")
    fp = open(log_file, "a")

    # Build server environment: inherit everything, then overlay MDSVC_* vars
    env = os.environ.copy()
    env.update({
        "MDSVC_MYSQL_HOST":     str(params["mysql_host"]),
        "MDSVC_MYSQL_PORT":     str(params["mysql_port"]),
        "MDSVC_MYSQL_USER":     str(params["mysql_user"]),
        "MDSVC_MYSQL_PASSWORD": str(params["mysql_password"]),
        "MDSVC_API_URL":        str(params["base_url"]),
    })

    if params["disable_auth"]:
        env["DISABLE_AUTH"] = "true"

    proc = subprocess.Popen(
        ["go", "run", "./cmd/server"],
        cwd=server_path,
        stdout=fp,
        stderr=fp,
        env=env,
        preexec_fn=os.setsid,
    )

    with open(pid_file, "w") as pidf:
        pidf.write(str(proc.pid))

    fp.flush()

    return {
        "status":   "server_started",
        "pid":      proc.pid,
        "log_file": log_file,
    }


def stop_server(params):
    """Send SIGTERM to the process group recorded in pid_file."""
    pid_file = params["pid_file"]

    if not os.path.exists(pid_file):
        return {"status": "server_not_running"}

    with open(pid_file, "r") as pidf:
        pid = int(pidf.read().strip())

    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except ProcessLookupError:
        pass  # process already gone
    except Exception as e:
        raise AnsibleError(f"Failed to stop server (pid={pid}): {e}")
    finally:
        os.remove(pid_file)

    return {"status": "server_stopped", "pid": pid}


# =========================================================
# Health-Check Helper
# =========================================================

def wait_for_server(params):
    """
    Poll base_url until we get an HTTP status < 500, then return.
    Raises AnsibleError if the server does not become ready within
    server_timeout seconds.
    """
    base_url = params["base_url"]
    timeout  = params["server_timeout"]
    log_file = _get_log_file(params)
    last_err = None

    for _ in range(timeout):
        try:
            resp = requests.get(base_url, timeout=5)
            if resp.status_code < 500:
                with open(log_file, "a") as logf:
                    _write_log_header(logf, "SERVER BECAME READY")
                return
        except Exception as e:
            last_err = str(e)

        time.sleep(1)

    # Capture container logs before raising so the operator has context
    docker_logs(params)

    raise AnsibleError(
        f"Server at {base_url} did not become ready within {timeout}s. "
        f"Last error: {last_err}. "
        f"See logs: {log_file}"
    )


# =========================================================
# Action Dispatch Table
# =========================================================

def _action_docker_up(params):
    docker_up(params)
    wait_for_server(params)
    return {"status": "docker_started", "log_file": _get_log_file(params)}


def _action_docker_down(params):
    return docker_down(params)


def _action_docker_logs(params):
    return docker_logs(params)


def _action_start_db(params):
    check_db(params)
    return {"status": "db_ready"}


def _action_create_schema(params):
    return create_schema(params)


def _action_start_server(params):
    data = start_server(params)
    wait_for_server(params)
    return data


def _action_stop_server(params):
    return stop_server(params)


def _action_wait_for_server(params):
    wait_for_server(params)
    return {"status": "server_ready"}


def _action_setup_docker(params):
    """Full Docker-based setup: bring the stack up and wait for readiness."""
    docker_up(params)
    wait_for_server(params)
    return {"status": "ready", "mode": "docker", "log_file": _get_log_file(params)}


def _action_setup_manual(params):
    """Full manual setup: verify DB → create schema → start server → health check."""
    check_db(params)
    create_schema(params)
    data = start_server(params)
    wait_for_server(params)
    return {"status": "ready", "mode": "manual", "pid": data["pid"]}


def _action_teardown_docker(params):
    return docker_down(params)


def _action_teardown_manual(params):
    return stop_server(params)


_ACTIONS = {
    # Low-level Docker actions
    "docker_up":        _action_docker_up,
    "docker_down":      _action_docker_down,
    "docker_logs":      _action_docker_logs,

    # Low-level manual actions
    "start_db":         _action_start_db,
    "create_schema":    _action_create_schema,
    "start_server":     _action_start_server,
    "stop_server":      _action_stop_server,

    # Health check only
    "wait_for_server":  _action_wait_for_server,

    # Composite helpers
    "setup_docker":     _action_setup_docker,
    "setup_manual":     _action_setup_manual,
    "teardown_docker":  _action_teardown_docker,
    "teardown_manual":  _action_teardown_manual,
}


# =========================================================
# Lookup Entry Point
# =========================================================

class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):

        if not terms:
            raise AnsibleError(
                "mdsvc_cluster requires an action as the first term. "
                f"Valid actions: {sorted(_ACTIONS)}"
            )

        action = terms[0]

        if action not in _ACTIONS:
            raise AnsibleError(
                f"Unknown action '{action}'. "
                f"Valid actions: {sorted(_ACTIONS)}"
            )

        params = _build_params(kwargs)

        result = _ACTIONS[action](params)

        return [result]