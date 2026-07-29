"""
Ansible Lookup Plugin: mdsvc_cluster
=====================================
Manages the lifecycle of an mdsvc-tidb cluster — both the Docker-based deployment and the manual (TiDB) deployment.

This matches the mdsvc-tidb README:
  - `docker compose up -d --build` brings up TiDB + mdsvc-api in one container,
    exposing the API on http://localhost:8081 and TiDB MySQL protocol on 127.0.0.1:4000.
  - Manual mode assumes the operator already has a TiDB/MySQL deployment running
    (see https://docs.pingcap.com/tidb/stable/quick-start-with-tidb/); this plugin
    only verifies reachability, it does not start TiDB itself.
  - The mdsvc-api server auto-provisions the control-plane schema, the default
    tenant's schema, and a default admin user on startup — no manual schema/bootstrap
    script is needed.
  - DISABLE_AUTH=true bypasses auth entirely (dev/test only). JWT_SECRET,
    TENANT_ADMIN_USERNAME/PASSWORD, and ADMIN_DEFAULT_USERNAME/PASSWORD are
    forwarded through when supplied.
"""

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

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

    # Fallback so we never crash purely because of missing log context
    return "/tmp/mdsvc_cluster_health_log.txt"


def _write_log_header(logf, message):
    logf.write("\n==== %s ====\n" % message)


def docker_logs(params):
    """
    Capture `docker logs` for the mdsvc-tidb container and append them to the
    relevant log file. Used to give the operator context when a health check
    times out.
    """
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
        except Exception as e:
            logf.write("Unable to capture docker logs: %s\n" % str(e))


# =========================================================
# Docker Setup
# =========================================================

def docker_setup(cluster_params):
    """Tear down existing stack, start Docker stack, wait for health, and stream logs."""

    workspace_dir = os.getenv('NIOVA_WORKSPACE')
    repo_path = "%s/mdsvc-tidb" % workspace_dir

    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    log_file = "%s/%s/%s_docker_log.txt" % (base_dir, raft_uuid, app_name)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    container_name = "mdsvc-tidb"
    base_url = cluster_params.get('api_base_url', 'http://localhost:8081')
    server_timeout = int(cluster_params.get('server_timeout', 120))

    with open(log_file, "a") as logf:
        logf.write("\nSTARTING MDSVC-TIDB DOCKER SETUP\n")

        down_proc = subprocess.Popen(
            ["sudo", "docker", "compose", "down", "-v"],
            cwd=repo_path,
            stdout=logf,
            stderr=logf
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
            stderr=logf
        )

        up_rc = up_proc.wait()

        if up_rc != 0:
            raise AnsibleError(
                "docker compose up failed. Check log: %s" % log_file
            )

        logf.write("\nDOCKER STACK STARTED\n")

    # Wait for the API to actually answer before declaring success, per README
    # (mdsvc-api HTTP on http://localhost:8081). Auto-provisioning of schema
    # and default admin user happens inside the container on startup.
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
            start_new_session=True
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

# =========================================================
# Manual Setup Helpers
# =========================================================

def manual_setup(cluster_params):
    """
    Verify an existing TiDB/MySQL deployment is reachable and start the
    mdsvc-api server against it.

    Per the README, manual mode assumes TiDB/MySQL is already running
    externally (see the PingCAP quick-start link) — this plugin does not
    launch `tiup playground` itself. The server auto-provisions its own
    schema and default admin user on startup, so no schema script is run
    here either.
    """

    workspace_dir = os.getenv('NIOVA_WORKSPACE')
    repo_path = "%s/mdsvc-tidb" % workspace_dir

    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    log_file = "%s/%s/%s_manual_log.txt" % (base_dir, raft_uuid, app_name)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    mysql_host = cluster_params.get('mysql_host', '127.0.0.1')
    mysql_port = str(cluster_params.get('mysql_port', '4000'))
    mysql_user = cluster_params.get('mysql_user', 'root')
    mysql_password = cluster_params.get('mysql_password', '')

    with open(log_file, "a") as logf:
        logf.write("\nSTARTING MANUAL MDSVC-TIDB SETUP\n")
        logf.write(
            "Assuming TiDB/MySQL is already running at %s:%s "
            "(manual mode does not start TiDB itself; see README)\n"
            % (mysql_host, mysql_port)
        )
        logf.write("Verifying TiDB is reachable...\n")

        tidb_ready = False

        for i in range(1, 31):
            mysql_cmd = [
                "mysql",
                "-h", mysql_host,
                "-P", mysql_port,
                "-u", mysql_user,
            ]
            if mysql_password:
                mysql_cmd.append("-p%s" % mysql_password)
            mysql_cmd += ["-e", "SHOW DATABASES;"]

            check_proc = subprocess.Popen(
                mysql_cmd,
                stdout=logf,
                stderr=logf
            )

            check_rc = check_proc.wait()

            if check_rc == 0:
                logf.write("TiDB is reachable!\n")
                tidb_ready = True
                break

            logf.write("Still waiting for TiDB... (%d)\n" % i)
            time.sleep(10)

        if not tidb_ready:
            raise AnsibleError(
                "Could not reach TiDB/MySQL at %s:%s within timeout. "
                "Manual mode requires an already-running TiDB deployment "
                "(see README). Check log: %s"
                % (mysql_host, mysql_port, log_file)
            )

        logf.write("\nMANUAL MDSVC-TIDB PRE-CHECK COMPLETED\n")

    base_url = cluster_params.get('api_base_url', 'http://localhost:8081')
    server_timeout = int(cluster_params.get('server_timeout', 120))

    server_result = start_server({
        "server_path":            repo_path,
        "pid_file":               "%s/%s/mdsvc_server.pid" % (base_dir, raft_uuid),
        "mysql_host":             mysql_host,
        "mysql_port":             mysql_port,
        "mysql_user":             mysql_user,
        "mysql_password":         mysql_password,
        "base_url":               base_url,
        "disable_auth":           cluster_params.get('disable_auth', False),
        "jwt_secret":             cluster_params.get('jwt_secret'),
        "tenant_admin_username":  cluster_params.get('tenant_admin_username'),
        "tenant_admin_password":  cluster_params.get('tenant_admin_password'),
        "admin_default_username": cluster_params.get('admin_default_username'),
        "admin_default_password": cluster_params.get('admin_default_password'),
    })

    # The server auto-provisions its schema and default admin user on startup
    # (per README), so we just wait for it to answer rather than running any
    # schema script.
    wait_for_server({
        "base_url": base_url,
        "server_timeout": server_timeout,
        "log_file": log_file,
    })

    return {
        "status":          "manual_setup_done",
        "server_pid":      server_result["pid"],
        "log_file":        log_file,
        "server_log_file": server_result["log_file"],
        "base_url":        base_url,
    }

def manual_teardown(cluster_params):
    """Stop the mdsvc-api server and remove its pid file.

    TiDB itself is not touched — manual mode never started it, so it's not
    this plugin's responsibility to stop it either.
    """
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    pid_dir = "%s/%s" % (base_dir, raft_uuid)

    server_status = stop_server({"pid_file": "%s/mdsvc_server.pid" % pid_dir})

    return {
        "status": "manual_teardown_done",
        "server": server_status,
    }

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

    if params.get("disable_auth"):
        # Dev/test only — bypasses auth entirely, treats all requests as
        # dev_admin (role=admin), and JWT_SECRET is not required in this mode.
        env["DISABLE_AUTH"] = "true"

    # Optional auth-related overrides documented in .env_example
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
    server_timeout seconds. On timeout, captures docker logs (if a
    container_name is present) for operator context.
    """
    base_url = params["base_url"]
    timeout  = params["server_timeout"]
    log_file = _get_log_file(params)
    last_err = None

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

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
    # (only meaningful for the docker path; harmless no-op otherwise)
    if params.get("container_name"):
        docker_logs(params)

    raise AnsibleError(
        f"Server at {base_url} did not become ready within {timeout}s. "
        f"Last error: {last_err}. "
        f"See logs: {log_file}"
    )

# =========================================================
# Lookup Entry Point
# =========================================================

class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):

        action = terms[0]

        cluster_params = variables['ClusterParams']

        #export NIOVA_THREAD_COUNT
        os.environ['NIOVA_THREAD_COUNT'] = cluster_params['nthreads']

        if action == "docker_setup":
            result = docker_setup(cluster_params)
        elif action == "manual_setup":
            result = manual_setup(cluster_params)
        elif action == "manual_teardown":
            result = manual_teardown(cluster_params)
        else:
            raise AnsibleError("Unsupported action: %s" % action)

        return [result]