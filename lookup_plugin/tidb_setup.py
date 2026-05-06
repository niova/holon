from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import os
import json
import signal
import socket
import subprocess
import requests
import time
from datetime import datetime

# =========================================================
# Generic Helpers
# =========================================================

def write_log_header(logf, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logf.write("\n")
    logf.write("=" * 80 + "\n")
    logf.write(f"[{timestamp}] {message}\n")
    logf.write("=" * 80 + "\n")
    logf.flush()

def run_command(cmd,
                cwd=None,
                env=None,
                logf=None,
                check=True):

    return subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        stdout=logf if logf else subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        text=True,
        check=check
    )

def get_log_file(repo_path):

    log_dir = os.path.join(repo_path, "logs")
    os.makedirs(log_dir, exist_ok=True)

    return os.path.join(log_dir, "mdsvc_docker.log")

# =========================================================
# Docker Helpers
# =========================================================

def get_docker_compose_cmd():

    try:
        subprocess.check_call(
            ["docker", "compose", "version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        return ["docker", "compose"]

    except Exception:
        return ["docker-compose"]

def docker_up(cluster_params):

    repo_path = cluster_params['repo_path']

    cmd = get_docker_compose_cmd()
    log_file = get_log_file(repo_path)

    with open(log_file, "a") as logf:

        write_log_header(logf, "STARTING MDSVC-TIDB DOCKER STACK")

        run_command(
            ["sudo"] + cmd + ["down", "-v"],
            cwd=repo_path,
            logf=logf
        )

        run_command(
            ["sudo"] + cmd + ["up", "-d", "--build"],
            cwd=repo_path,
            logf=logf
        )

        write_log_header(logf, "DOCKER STACK STARTED")

    return {
        "status": "docker_started",
        "log_file": log_file
    }

def docker_down(cluster_params):

    repo_path = cluster_params['repo_path']

    cmd = get_docker_compose_cmd()
    log_file = get_log_file(repo_path)

    with open(log_file, "a") as logf:

        write_log_header(logf, "STOPPING MDSVC-TIDB DOCKER STACK")

        run_command(
            ["sudo"] + cmd + ["down", "-v"],
            cwd=repo_path,
            logf=logf
        )

        write_log_header(logf, "DOCKER STACK STOPPED")

    return {
        "status": "docker_stopped",
        "log_file": log_file
    }

def docker_logs(cluster_params,
                container_name="mdsvc-tidb",
                follow=False):

    repo_path = cluster_params['repo_path']
    log_file = get_log_file(repo_path)

    cmd = ["sudo", "docker", "logs"]

    if follow:
        cmd.append("-f")

    cmd.append(container_name)

    with open(log_file, "a") as logf:

        write_log_header(
            logf,
            f"DOCKER LOGS: {container_name}"
        )

        run_command(
            cmd,
            logf=logf
        )

        write_log_header(
            logf,
            f"END DOCKER LOGS: {container_name}"
        )

    return {
        "status": "docker_logs_collected",
        "log_file": log_file
    }

# =========================================================
# Manual Setup Helpers
# =========================================================

def check_db(cluster_params):

    host = cluster_params['mysql_host']
    port = cluster_params['mysql_port']

    sock = socket.socket()

    try:
        sock.settimeout(3)
        sock.connect((host, int(port)))

    except Exception as e:
        raise AnsibleError(
            f"Database not reachable at {host}:{port} -> {e}"
        )

    finally:
        sock.close()

def create_schema(cluster_params):

    repo_path = cluster_params['repo_path']

    script_path = os.path.join(
        repo_path,
        "scripts/run_schema.sh"
    )

    if not os.path.exists(script_path):
        raise AnsibleError(
            f"Schema script not found: {script_path}"
        )

    env = os.environ.copy()

    env.update({
        "MDSVC_MYSQL_HOST": str(cluster_params['mysql_host']),
        "MDSVC_MYSQL_PORT": str(cluster_params['mysql_port']),
        "MDSVC_MYSQL_USER": str(cluster_params['mysql_user']),
        "MDSVC_MYSQL_PASSWORD": str(cluster_params['mysql_password']),
    })

    run_command(
        ["chmod", "+x", script_path]
    )

    run_command(
        [script_path],
        cwd=repo_path,
        env=env
    )

    return {
        "status": "schema_created"
    }

def start_server(cluster_params):

    server_path = cluster_params['server_path']

    log_file = os.path.join(server_path, "mdsvc.log")

    fp = open(log_file, "a")

    process_popen = subprocess.Popen(
        ["go", "run", "./cmd/server"],
        cwd=server_path,
        stdout=fp,
        stderr=fp,
        preexec_fn=os.setsid
    )

    pid_file = "/tmp/mdsvc_server.pid"

    with open(pid_file, "w") as pidf:
        pidf.write(str(process_popen.pid))

    os.fsync(fp)

    return {
        "status": "server_started",
        "pid": process_popen.pid,
        "log_file": log_file
    }

def stop_server():

    pid_file = "/tmp/mdsvc_server.pid"

    if not os.path.exists(pid_file):
        return {
            "status": "server_not_running"
        }

    with open(pid_file, "r") as pidf:
        pid = int(pidf.read().strip())

    try:
        os.killpg(
            os.getpgid(pid),
            signal.SIGTERM
        )

    except Exception:
        pass

    os.remove(pid_file)

    return {
        "status": "server_stopped"
    }

# =========================================================
# Health Check Helpers
# =========================================================

def wait_for_server(cluster_params,
                    timeout=120):

    repo_path = cluster_params['repo_path']
    base_url = cluster_params['base_url']

    log_file = get_log_file(repo_path)

    last_error = None

    for _ in range(timeout):

        try:
            response = requests.get(
                base_url,
                timeout=5
            )

            if response.status_code < 500:

                with open(log_file, "a") as logf:
                    write_log_header(
                        logf,
                        "SERVER BECAME READY"
                    )

                return

        except Exception as e:
            last_error = str(e)

        time.sleep(1)

    docker_logs(cluster_params)

    raise AnsibleError(
        f"Server did not become ready in time. "
        f"Last error: {last_error}. "
        f"Check logs: {log_file}"
    )

# =========================================================
# Main Lookup Module
# =========================================================

class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):

        action = terms[0]

        cluster_params = {
            "repo_path": kwargs.get(
                "repo_path",
                "/home/himani/mdsvc-tidb"
            ),

            "server_path": kwargs.get(
                "server_path",
                "/home/himani/mdsvc-tidb"
            ),

            "base_url": kwargs.get(
                "base_url",
                "http://localhost:8081"
            ),

            "mysql_host": kwargs.get(
                "mysql_host",
                os.getenv("MDSVC_MYSQL_HOST", "127.0.0.1")
            ),

            "mysql_port": kwargs.get(
                "mysql_port",
                os.getenv("MDSVC_MYSQL_PORT", "4000")
            ),

            "mysql_user": kwargs.get(
                "mysql_user",
                os.getenv("MDSVC_MYSQL_USER", "root")
            ),

            "mysql_password": kwargs.get(
                "mysql_password",
                os.getenv("MDSVC_MYSQL_PASSWORD", "")
            )
        }

        if action == "docker_up":

            docker_up(cluster_params)
            wait_for_server(cluster_params)

            return [{
                "status": "docker_started",
                "log_file": get_log_file(cluster_params['repo_path'])
            }]

        elif action == "docker_down":

            return [docker_down(cluster_params)]

        elif action == "docker_logs":

            return [docker_logs(cluster_params)]

        elif action == "start_db":

            check_db(cluster_params)

            return [{
                "status": "db_ready"
            }]

        elif action == "create_schema":

            return [create_schema(cluster_params)]

        elif action == "start_server":

            server_data = start_server(cluster_params)

            wait_for_server(cluster_params)

            return [server_data]

        elif action == "stop_server":

            return [stop_server()]

        elif action == "setup_all":

            check_db(cluster_params)
            create_schema(cluster_params)

            server_data = start_server(cluster_params)

            wait_for_server(cluster_params)

            return [{
                "status": "ready",
                "pid": server_data['pid']
            }]

        raise AnsibleError(
            f"Unsupported action: {action}"
        )
