from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import json
import os
import requests
import subprocess
from datetime import datetime

def load_payload(file_path=None, body=None):
    if file_path:
        if not os.path.exists(file_path):
            raise AnsibleError(f"JSON file not found: {file_path}")
        with open(file_path, "r") as fp:
            return json.load(fp)

    if body:
        if isinstance(body, dict):
            return body
        if isinstance(body, str):
            return json.loads(body)

    return None

def get_log_file(variables, log_dir=None):
    """
    Build the API log path using the directory:

        <base_dir>/<raft_uuid>/<app_name>_api_log.txt
    """

    cluster_params = variables.get("ClusterParams", {})

    base_dir = cluster_params.get("base_dir")
    raft_uuid = cluster_params.get("raft_uuid")
    app_name = cluster_params.get("app_type", "tidb")

    if log_dir:
        resolved_log_dir = os.path.abspath(os.path.expanduser(log_dir))
    elif base_dir and raft_uuid:
        resolved_log_dir = os.path.join(
            os.path.expanduser(str(base_dir)),
            str(raft_uuid),
        )
    else:
        resolved_log_dir = os.path.abspath("./logs")

    os.makedirs(resolved_log_dir, exist_ok=True)

    return os.path.join(
        resolved_log_dir,
        f"{app_name}_api_log.txt",
    )

def write_log(log_file, method, url, payload, status, response_data):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(log_file, "a") as logf:
        logf.write("\n" + "=" * 80 + "\n")
        logf.write(f"[{timestamp}] {method} {url}\n")
        logf.write("=" * 80 + "\n")

        if payload:
            logf.write("REQUEST BODY:\n")
            logf.write(json.dumps(payload, indent=2))
            logf.write("\n")

        logf.write(f"STATUS CODE: {status}\n")
        logf.write("RESPONSE:\n")
        logf.write(json.dumps(response_data, indent=2))
        logf.write("\n")

def parse_response(response, log_file=None, method=None, url=None, payload=None):
    try:
        data = response.json()
    except ValueError:
        data = {
            "raw_response": response.text,
        }

        if log_file:
            write_log(
                log_file,
                method or response.request.method,
                url or response.url,
                payload,
                response.status_code,
                data,
            )

        raise AnsibleError(
            f"Invalid JSON response from {response.url}. "
            f"Status: {response.status_code}. "
            f"Response: {response.text}. "
            f"Check log: {log_file}"
        )

    if response.status_code not in [200, 201]:
        if log_file:
            write_log(
                log_file,
                method or response.request.method,
                url or response.url,
                payload,
                response.status_code,
                data,
            )

        raise AnsibleError(
            f"API failed [{response.status_code}]: {data}. "
            f"Check log: {log_file}"
        )

    return data

def login(base_url, username, password, log_file, timeout):
    payload = {
        "username": username,
        "password": password,
    }

    url = f"{base_url}/users/login"

    response = perform_request(
        method="POST",
        url=url,
        payload=payload,
        params=None,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
        log_file=log_file,
    )

    data = parse_response(
        response,
        log_file=log_file,
        method="POST",
        url=url,
        payload=payload,
    )

    write_log(
        log_file,
        "POST",
        url,
        payload,
        response.status_code,
        data,
    )

    token = data.get("access_token")

    if not token:
        raise AnsibleError(f"Login succeeded but access_token missing: {data}")

    return token

def make_headers(token=None, extra_headers=None):
    headers = {
        "Content-Type": "application/json",
    }

    if token:
        headers["Authorization"] = f"Bearer {token}"

    if extra_headers:
        headers.update(extra_headers)

    return headers

def perform_request(
    method,
    url,
    payload,
    params,
    headers,
    timeout,
    log_file,
):
    try:
        response = requests.request(
            method=method,
            url=url,
            json=payload if method in ["POST", "PUT", "DELETE"] else None,
            params=params,
            headers=headers,
            timeout=timeout,
        )

        return response

    except requests.exceptions.RequestException as exc:
        write_log(
            log_file=log_file,
            method=method,
            url=url,
            payload=payload,
            status="REQUEST_FAILED",
            response_data={
                "error": str(exc),
                "exception_type": type(exc).__name__,
            },
        )

        raise AnsibleError(
            f"{method} request to {url} failed: {exc}. "
            f"Check log: {log_file}"
        )

def extract_fields(data):
    extracted = {}

    keys = [
        "uuid",
        "vdev_id",
        "infra_id",
        "chunk_uuid",
        "nisd_uuid",
        "resource_id",
        "access_token",
    ]

    if isinstance(data, dict):
        for key in keys:
            if key in data:
                extracted[key] = data[key]

        inner = data.get("data")
        if isinstance(inner, dict):
            for key in keys:
                if key in inner:
                    extracted[key] = inner[key]

    return extracted

def paginated_chunks(api_params):
    all_chunks = []
    page_count = 0

    params = dict(api_params.get("params") or {})

    while True:
        page_count += 1

        response = perform_request(
            api_params["method"],
            api_params["url"],
            api_params["payload"],
            params,
            api_params["headers"],
            api_params["timeout"],
            api_params["log_file"],
        )

        data = parse_response(
            response,
            log_file=api_params["log_file"],
            method=api_params["method"],
            url=api_params["url"],
            payload=api_params["payload"],
        )

        write_log(
            api_params["log_file"],
            api_params["method"],
            api_params["url"],
            api_params["payload"],
            response.status_code,
            data,
        )

        chunks = data.get("chunks", [])
        all_chunks.extend(chunks)

        if not data.get("has_more", False):
            break

        next_start = data.get("next_start_chunk_idx")

        if next_start is None:
            raise AnsibleError(
                "Pagination error: has_more=true but "
                "next_start_chunk_idx missing. "
                f"Check log: {api_params['log_file']}"
            )

        params["start_chunk_idx"] = next_start

    result = {
        "success": True,
        "vdev_id": data.get("vdev_id"),
        "chunks": all_chunks,
        "total_chunks_fetched": len(all_chunks),
        "expected_total_chunks": data.get("total_chunks"),
        "pages_fetched": page_count,
        "log_file": api_params["log_file"],
    }

    result.update(extract_fields(data))
    return result

def run_schema(repo_path, log_file, mysql_env):
    schema_script = os.path.join(repo_path, "scripts", "run_schema.sh")

    if not os.path.exists(schema_script):
        raise AnsibleError(f"Schema script not found: {schema_script}")

    env = os.environ.copy()
    env.update(mysql_env)

    with open(log_file, "a") as logf:
        chmod_proc = subprocess.Popen(
            ["chmod", "+x", schema_script],
            stdout=logf,
            stderr=logf,
        )
        chmod_rc = chmod_proc.wait()

        if chmod_rc != 0:
            raise AnsibleError(f"chmod failed for {schema_script}")

        proc = subprocess.Popen(
            [schema_script],
            cwd=repo_path,
            stdout=logf,
            stderr=logf,
            env=env,
        )

        rc = proc.wait()

    if rc != 0:
        raise AnsibleError(f"Schema setup failed. Check log: {log_file}")

    return {
        "status": "schema_created",
        "schema_script": schema_script,
        "log_file": log_file,
    }

class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):
        if variables is None:
            variables = {}

        if len(terms) < 1:
            raise AnsibleError(
                "Usage: lookup('tidb_api', ACTION, ...)"
            )

        action = terms[0]

        base_url = kwargs.get(
            "base_url",
            os.getenv("MDSVC_API_URL", "http://localhost:8081"),
        )

        log_dir = kwargs.get("log_dir")
        log_file = get_log_file(
            variables=variables,
            log_dir=log_dir,
        )

        timeout = kwargs.get("timeout", 10)

        username = kwargs.get(
            "username",
            os.getenv("INTEGRATION_ADMIN_USERNAME", "admin"),
        )

        password = kwargs.get(
            "password",
            os.getenv("INTEGRATION_ADMIN_PASSWORD", "admin"),
        )

        disable_auth = str(
            kwargs.get("disable_auth", os.getenv("DISABLE_AUTH", "false"))
        ).lower() == "true"

        token = kwargs.get("token")

        workspace_dir = os.getenv("NIOVA_WORKSPACE")

        if workspace_dir:
            default_repo_path = os.path.join(
                workspace_dir,
                "mdsvc-tidb",
            )
        else:
            default_repo_path = os.path.expanduser(
                "~/mdsvc-tidb"
            )

        if action == "create_schema":
            repo_path = kwargs.get(
                "repo_path",
                default_repo_path,
            )

            if not os.path.isdir(repo_path):
                raise AnsibleError(
                    f"mdsvc-tidb repository directory not found: {repo_path}"
                )

            mysql_env = {
                "MDSVC_MYSQL_HOST": str(
                    kwargs.get("mysql_host", "127.0.0.1")
                ),
                "MDSVC_MYSQL_PORT": str(
                    kwargs.get("mysql_port", "4000")
                ),
                "MDSVC_MYSQL_USER": str(
                    kwargs.get("mysql_user", "root")
                ),
                "MDSVC_MYSQL_PASSWORD": str(
                    kwargs.get("mysql_password", "")
                ),
            }

            return [run_schema(repo_path, log_file, mysql_env)]

        if action == "login":
            token = login(base_url, username, password, log_file, timeout)
            return [{
                "status": "login_success",
                "access_token": token,
                "log_file": log_file,
            }]

        endpoint_map = {
            "create_infra": ("POST", "/api/infra"),
            "get_infra": ("GET", "/api/infra"),

            "create_vdev": ("POST", "/api/vdev"),
            "get_vdev": ("GET", "/api/vdev"),

            "get_chunk": ("GET", "/api/chunk"),
            "get_chunks": ("GET", "/api/chunks"),

            "get_nisd": ("GET", "/api/nisd"),

            "get_resource": ("GET", "/api/resource"),
            "put_resource": ("PUT", "/api/resource"),

            "get_users": ("GET", "/api/users"),

            "get_rbac": ("GET", "/api/authz/rbac"),
            "create_rbac": ("POST", "/api/authz/rbac"),
            "delete_rbac": ("DELETE", "/api/authz/rbac"),

            "get_abac": ("GET", "/api/authz/abac"),
            "create_abac": ("POST", "/api/authz/abac"),
            "delete_abac": ("DELETE", "/api/authz/abac"),

            "proxy_func": ("GET", "/func"),
        }

        if action not in endpoint_map:
            raise AnsibleError(f"Unsupported action: {action}")

        method, path = endpoint_map[action]

        if len(terms) >= 2:
            path = terms[1]

        if not disable_auth and not token and path.startswith("/api/"):
            token = login(base_url, username, password, log_file, timeout)

        payload = load_payload(
            kwargs.get("file"),
            kwargs.get("body"),
        )

        headers = make_headers(
            token=token,
            extra_headers=kwargs.get("headers"),
        )

        api_params = {
            "method": method,
            "url": f"{base_url}{path}",
            "payload": payload,
            "params": kwargs.get("params"),
            "headers": headers,
            "timeout": timeout,
            "log_file": log_file,
        }

        if action == "get_chunks":
            return [paginated_chunks(api_params)]

        response = perform_request(
            api_params["method"],
            api_params["url"],
            api_params["payload"],
            api_params["params"],
            api_params["headers"],
            api_params["timeout"],
            api_params["log_file"],
        )

        data = parse_response(
            response,
            log_file=log_file,
            method=method,
            url=api_params["url"],
            payload=payload,
        )

        write_log(
            log_file,
            method,
            api_params["url"],
            payload,
            response.status_code,
            data,
        )

        result = {
            "status_code": response.status_code,
            "data": data,
            "log_file": log_file,
        }

        result.update(extract_fields(data))

        return [result]