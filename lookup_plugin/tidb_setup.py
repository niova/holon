from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import os
import subprocess
import time
import signal
import requests

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError


class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):
        if not terms:
            raise AnsibleError("Action is required")

        action = terms[0]

        repo_path = kwargs.get("repo_path", "/work/niova/tidb-mdsvc")
        server_path = kwargs.get("server_path", os.path.join(repo_path, "mdsvc-tidb"))

        db_config = kwargs.get("db_config", {})
        host = db_config.get("host", os.getenv("MDSVC_MYSQL_HOST", "127.0.0.1"))
        port = db_config.get("port", os.getenv("MDSVC_MYSQL_PORT", "4000"))
        user = db_config.get("user", os.getenv("MDSVC_MYSQL_USER", "root"))
        password = db_config.get("password", os.getenv("MDSVC_MYSQL_PASSWORD", ""))

        base_url = kwargs.get("base_url", "http://localhost:8081")

        if action == "setup_all":
            self._check_db(host, port)
            self._create_schema(repo_path, host, port, user, password)
            pid = self._start_server(server_path)
            self._wait_for_server(base_url)
            return [{"status": "ready", "pid": pid}]

        elif action == "start_db":
            self._check_db(host, port)
            return [{"status": "db_ready"}]

        elif action == "create_schema":
            self._create_schema(repo_path, host, port, user, password)
            return [{"status": "schema_created"}]

        elif action == "start_server":
            pid = self._start_server(server_path)
            self._wait_for_server(base_url)
            return [{"status": "server_started", "pid": pid}]

        elif action == "stop_server":
            self._stop_server()
            return [{"status": "server_stopped"}]

        elif action == "docker_up":
            self._docker_up(repo_path)
            self._wait_for_server(base_url)
            return [{"status": "docker_started"}]

        elif action == "docker_down":
            self._docker_down(repo_path)
            return [{"status": "docker_stopped"}]

        elif action == "docker_logs":
            logs = self._docker_logs(kwargs.get("container", "mdsvc-tidb"),
                                    follow=kwargs.get("follow", False))
            return [{"logs": logs}]

        else:
            raise AnsibleError(f"Unknown action: {action}")

    # -------------------------
    # Internal Helpers
    # -------------------------

    def check_db(self, host, port):
        import socket
        s = socket.socket()
        try:
            s.settimeout(3)
            s.connect((host, int(port)))
        except Exception as e:
            raise AnsibleError(f"Database not reachable at {host}:{port} → {e}")
        finally:
            s.close()

    def create_schema(self, repo_path, host, port, user, password):
        script_path = os.path.join(repo_path, "scripts/run_schema.sh")

        if not os.path.exists(script_path):
            raise AnsibleError(f"Schema script not found: {script_path}")

        env = os.environ.copy()
        env.update({
            "MDSVC_MYSQL_HOST": str(host),
            "MDSVC_MYSQL_PORT": str(port),
            "MDSVC_MYSQL_USER": str(user),
            "MDSVC_MYSQL_PASSWORD": str(password),
        })

        try:
            subprocess.check_call(["chmod", "+x", script_path])
            subprocess.check_call([script_path], cwd=repo_path, env=env)
        except subprocess.CalledProcessError as e:
            raise AnsibleError(f"Schema creation failed: {e}")

    def start_server(self, server_path):
        log_file = os.path.join(server_path, "mdsvc.log")

        with open(log_file, "w") as f:
            proc = subprocess.Popen(
                ["go", "run", "./cmd/server"],
                cwd=server_path,
                stdout=f,
                stderr=f,
                preexec_fn=os.setsid
            )

        pid_file = "/tmp/mdsvc_server.pid"
        with open(pid_file, "w") as f:
            f.write(str(proc.pid))

        return proc.pid

    def stop_server(self):
        pid_file = "/tmp/mdsvc_server.pid"

        if not os.path.exists(pid_file):
            return

        with open(pid_file, "r") as f:
            pid = int(f.read().strip())

        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except Exception:
            pass

        os.remove(pid_file)

    def wait_for_server(self, base_url, timeout=20):
        for _ in range(timeout):
            try:
                r = requests.get(base_url + "/infra", timeout=2)
                if r.status_code in [200, 404]:
                    return
            except Exception:
                pass
            time.sleep(1)

        raise AnsibleError("Server did not become ready in time")

    def get_docker_compose_cmd(self):
        """
        Detect whether to use `docker compose` or `docker-compose`
        """
        try:
            subprocess.check_call(
                ["docker", "compose", "version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return ["docker", "compose"]
        except Exception:
            return ["docker-compose"]


    def docker_up(self, repo_path):
        cmd = self._get_docker_compose_cmd()

        try:
            # docker compose down -v
            subprocess.check_call(cmd + ["down", "-v"], cwd=repo_path)

            # docker compose up -d --build
            subprocess.check_call(cmd + ["up", "-d", "--build"], cwd=repo_path)

        except subprocess.CalledProcessError as e:
            raise AnsibleError(f"Docker up failed: {e}")


    def docker_down(self, repo_path):
        cmd = self._get_docker_compose_cmd()

        try:
            subprocess.check_call(cmd + ["down", "-v"], cwd=repo_path)
        except subprocess.CalledProcessError as e:
            raise AnsibleError(f"Docker down failed: {e}")


    def docker_logs(self, container_name, follow=False):
        cmd = ["docker", "logs"]

        if follow:
            cmd.append("-f")

        cmd.append(container_name)

        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            return output.decode()
        except subprocess.CalledProcessError as e:
            raise AnsibleError(f"Failed to fetch logs: {e.output.decode()}")