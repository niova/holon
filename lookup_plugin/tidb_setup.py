"""
Ansible Lookup Plugin: mdsvc_cluster
=====================================
Manages the lifecycle of an mdsvc-tidb cluster — both the Docker-based deployment and the manual (TiDB) deployment.
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
# Docker Setup
# =========================================================

def docker_setup(cluster_params):
    """Tear down existing stack, start Docker stack, and collect logs."""

    repo_path = "/home/himani/mdsvc-tidb"

    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    log_file = "%s/%s/%s_docker_log.txt" % (base_dir, raft_uuid, app_name)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    container_name = "mdsvc-tidb"

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
        "log_file": log_file
    }

# =========================================================
# Manual Setup Helpers
# =========================================================

def manual_setup(cluster_params):
    """Start TiDB manually using tiup playground and setup mdsvc schema."""

    repo_path = "/home/himani/mdsvc-tidb"

    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    log_file = "%s/%s/%s_manual_log.txt" % (base_dir, raft_uuid, app_name)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    tidb_log_file = "%s/%s/tidb.log" % (base_dir, raft_uuid)

    env = os.environ.copy()
    env["MDSVC_MYSQL_HOST"] = "127.0.0.1"
    env["MDSVC_MYSQL_PORT"] = "4000"
    env["MDSVC_MYSQL_USER"] = "root"
    env["MDSVC_MYSQL_PASSWORD"] = ""

    with open(log_file, "a") as logf:
        logf.write("\nSTARTING MANUAL MDSVC-TIDB SETUP\n")

        tidb_proc = subprocess.Popen(
            [
                os.path.expanduser("~/.tiup/bin/tiup"),
                "playground",
                "v8.5.0",
                "--db", "1",
                "--kv", "1",
                "--pd", "1",
                "--tiflash", "0",
                "--monitor", "false"
            ],
            stdout=open(tidb_log_file, "a"),
            stderr=subprocess.STDOUT,
            start_new_session=True
        )

        logf.write("TiDB playground started in background pid=%d\n" % tidb_proc.pid)
        logf.write("Waiting for TiDB to start...\n")

        tidb_ready = False

        for i in range(1, 31):
            check_proc = subprocess.Popen(
                [
                    "mysql",
                    "-h", "127.0.0.1",
                    "-P", "4000",
                    "-u", "root",
                    "-e", "SHOW DATABASES;"
                ],
                stdout=logf,
                stderr=logf
            )

            check_rc = check_proc.wait()

            if check_rc == 0:
                logf.write("TiDB is up!\n")
                tidb_ready = True
                break

            logf.write("Still waiting... (%d)\n" % i)
            time.sleep(10)

        if not tidb_ready:
            raise AnsibleError(
                "TiDB did not start within timeout. Check log: %s" % tidb_log_file
            )

        logf.write("\nRunning final TiDB database check\n")

        final_check = subprocess.Popen(
            [
                "mysql",
                "-h", "127.0.0.1",
                "-P", "4000",
                "-u", "root",
                "-e", "SHOW DATABASES;"
            ],
            stdout=logf,
            stderr=logf
        )

        final_rc = final_check.wait()

        if final_rc != 0:
            raise AnsibleError(
                "Final TiDB SHOW DATABASES check failed. Check log: %s" % log_file
            )

        schema_script = "%s/scripts/run_schema.sh" % repo_path

        chmod_proc = subprocess.Popen(
            ["chmod", "+x", schema_script],
            stdout=logf,
            stderr=logf
        )

        chmod_rc = chmod_proc.wait()

        if chmod_rc != 0:
            raise AnsibleError(
                "chmod failed for schema script. Check log: %s" % log_file
            )

        schema_success = False

        for i in range(1, 6):
            logf.write("\nSchema attempt %d\n" % i)

            schema_proc = subprocess.Popen(
                [schema_script],
                cwd=repo_path,
                stdout=logf,
                stderr=logf,
                env=env
            )

            schema_rc = schema_proc.wait()

            if schema_rc == 0:
                logf.write("Schema setup succeeded\n")
                schema_success = True
                break

            logf.write("Schema setup failed, retrying...\n")
            time.sleep(15)

        if not schema_success:
            logf.write("Schema setup failed permanently\n")

            try:
                with open(tidb_log_file, "r") as tidbf:
                    logf.write("\n==== tidb.log ====\n")
                    logf.write(tidbf.read())
                    logf.write("\n==== end tidb.log ====\n")
            except Exception as e:
                logf.write("Unable to read tidb log: %s\n" % str(e))

            raise AnsibleError(
                "Schema setup failed permanently. Check log: %s" % log_file
            )

        logf.write("\nMANUAL MDSVC-TIDB SETUP COMPLETED\n")

    return {
        "status": "manual_setup_done",
        "tidb_pid": tidb_proc.pid,
        "log_file": log_file,
        "tidb_log_file": tidb_log_file
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
# Lookup Entry Point
# =========================================================

class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):

        action = terms[0]
        input_values = terms[1]

        cluster_params = kwargs['variables']['ClusterParams']

        #export NIOVA_THREAD_COUNT
        os.environ['NIOVA_THREAD_COUNT'] = cluster_params['nthreads']

        if action == "docker_setup":
            result = docker_setup(cluster_params)
        elif action == "manual_setup":
            result = manual_setup(input_values, cluster_params)
        else:
            raise AnsibleError("Unsupported action: %s" % action)

        return [result]