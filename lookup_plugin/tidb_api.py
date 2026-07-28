from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import json
import os
import requests
import subprocess
from datetime import datetime
from collections import defaultdict

def compute_expected_allocations(nisd_resources, replica_count, vdev_size):
    if replica_count <= 0 or vdev_size <= 0:
        raise AnsibleError(
            f"Invalid inputs: replica_count={replica_count}, vdev_size={vdev_size}"
        )

    rack_capacity = defaultdict(int)
    for nisd in nisd_resources:
        rack_id = nisd.get("fd_rack_id")
        rack_capacity[rack_id] += int(nisd["available_size"])

    domains_available = len(rack_capacity)

    if domains_available < replica_count:
        return {
            "expected_allocations": 0,
            "domains_available": domains_available,
            "domains_required": replica_count,
            "reason": "insufficient failure domains for replica_count",
        }

    chosen_capacities = sorted(rack_capacity.values(), reverse=True)[:replica_count]
    bottleneck_capacity = min(chosen_capacities)
    expected_allocations = bottleneck_capacity // vdev_size

    return {
        "expected_allocations": expected_allocations,
        "domains_available": domains_available,
        "domains_required": replica_count,
        "bottleneck_capacity": bottleneck_capacity,
        "chosen_rack_capacities": chosen_capacities,
    }

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

    response_payload = data.get("payload", {})
    token = response_payload.get("access_token")

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
        "mount_counter",
        "last_mounted_at",
        "ncp_status_code",
        "parity_blk_cnt",
        "redundancy",
        "chunk_cnt",
        "total_chunks",
        "status",
        "message",
    ]

    def collect(obj):
        if not isinstance(obj, dict):
            return

        for key in keys:
            if key in obj:
                extracted[key] = obj[key]

    collect(data)

    inner = data.get("data")
    collect(inner)

    payload = data.get("payload")
    collect(payload)

    if isinstance(inner, dict):
        collect(inner.get("payload"))

    return extracted

def paginated_chunks(api_params):
    all_chunks = []
    pages = []
    page_count = 0

    params = dict(api_params.get("params") or {})

    initial_start_chunk_idx = int(
        params.get("start_chunk_idx", 0)
    )

    requested_limit = int(
        params.get("limit", 100)
    )

    current_start_chunk_idx = initial_start_chunk_idx

    while True:
        page_count += 1

        params["start_chunk_idx"] = current_start_chunk_idx
        params["limit"] = requested_limit

        response = perform_request(
            method=api_params["method"],
            url=api_params["url"],
            payload=api_params["payload"],
            params=params,
            headers=api_params["headers"],
            timeout=api_params["timeout"],
            log_file=api_params["log_file"],
        )

        response_data = parse_response(
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
            response_data,
        )

        if not isinstance(response_data, dict):
            raise AnsibleError(
                "Pagination response must be a JSON object. "
                f"Received: {type(response_data).__name__}. "
                f"Check log: {api_params['log_file']}"
            )
            
        page_data = response_data

        inner_data = response_data.get("data")
        if isinstance(inner_data, dict):
            page_data = inner_data

        payload = page_data.get("payload")
        if isinstance(payload, dict):
            page_data = payload

        if not isinstance(page_data, dict):
            raise AnsibleError(
                "Unable to locate pagination data in API response. "
                f"Response: {response_data}. "
                f"Check log: {api_params['log_file']}"
            )

        chunks = page_data.get("chunks", [])

        if chunks is None:
            chunks = []

        if not isinstance(chunks, list):
            raise AnsibleError(
                "Pagination field 'chunks' must be a list. "
                f"Received: {type(chunks).__name__}. "
                f"Page data: {page_data}. "
                f"Check log: {api_params['log_file']}"
            )

        response_start_chunk_idx = int(
            page_data.get(
                "start_chunk_idx",
                current_start_chunk_idx,
            )
        )

        response_limit = int(
            page_data.get(
                "limit",
                requested_limit,
            )
        )

        has_more = bool(
            page_data.get("has_more", False)
        )

        next_start_chunk_idx = page_data.get(
            "next_start_chunk_idx"
        )

        total_chunks = page_data.get("total_chunks")

        vdev_id = (
            page_data.get("vdev_id")
            or params.get("vdev_id")
        )

        if response_start_chunk_idx != current_start_chunk_idx:
            raise AnsibleError(
                "Pagination continuity error: requested "
                f"start_chunk_idx={current_start_chunk_idx}, but API "
                f"returned start_chunk_idx={response_start_chunk_idx}. "
                f"Check log: {api_params['log_file']}"
            )

        if response_limit <= 0:
            raise AnsibleError(
                "Pagination response contains an invalid limit: "
                f"{response_limit}. "
                f"Check log: {api_params['log_file']}"
            )

        if len(chunks) > response_limit:
            raise AnsibleError(
                "Pagination response exceeded the page limit: "
                f"received {len(chunks)} chunks with limit "
                f"{response_limit}. "
                f"Check log: {api_params['log_file']}"
            )

        if has_more and next_start_chunk_idx is None:
            raise AnsibleError(
                "Pagination error: has_more=true but "
                "next_start_chunk_idx is missing. "
                f"Page data: {page_data}. "
                f"Check log: {api_params['log_file']}"
            )

        if next_start_chunk_idx is not None:
            next_start_chunk_idx = int(
                next_start_chunk_idx
            )

        if (
            has_more
            and next_start_chunk_idx
            <= current_start_chunk_idx
        ):
            raise AnsibleError(
                "Pagination did not progress: "
                f"current start_chunk_idx={current_start_chunk_idx}, "
                f"next_start_chunk_idx={next_start_chunk_idx}. "
                f"Check log: {api_params['log_file']}"
            )

        page_entry = {
            "page_number": page_count,
            "start_chunk_idx": response_start_chunk_idx,
            "next_start_chunk_idx": next_start_chunk_idx,
            "has_more": has_more,
            "limit": response_limit,
            "chunk_count": len(chunks),
            "chunks": chunks,
        }

        pages.append(page_entry)
        all_chunks.extend(chunks)

        if not has_more:
            final_page_data = page_data
            break

        current_start_chunk_idx = next_start_chunk_idx

    expected_total_chunks = final_page_data.get(
        "total_chunks"
    )

    if expected_total_chunks is not None:
        expected_total_chunks = int(
            expected_total_chunks
        )

        if len(all_chunks) != expected_total_chunks:
            raise AnsibleError(
                "Pagination reconstruction failed: "
                f"fetched {len(all_chunks)} chunks, but API reported "
                f"total_chunks={expected_total_chunks}. "
                f"Check log: {api_params['log_file']}"
            )

    result = {
        "success": True,
        "status_code": response.status_code,
        "vdev_id": (
            final_page_data.get("vdev_id")
            or params.get("vdev_id")
        ),
        "chunks": all_chunks,
        "pages": pages,
        "pages_fetched": page_count,
        "total_chunks_fetched": len(all_chunks),
        "expected_total_chunks": expected_total_chunks,
        "initial_start_chunk_idx": initial_start_chunk_idx,
        "requested_limit": requested_limit,
        "pagination_complete": True,
        "log_file": api_params["log_file"],
    }

    result.update(extract_fields(final_page_data))

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

        if action == "expected_allocations":
            nisd_resources = kwargs.get("nisd_resources")
            replica_count = int(kwargs.get("replica_count"))
            vdev_size = int(kwargs.get("vdev_size"))

            if not nisd_resources:
                raise AnsibleError(
                    "expected_allocations requires 'nisd_resources' kwarg"
                )

            return [compute_expected_allocations(nisd_resources, replica_count, vdev_size)]

        endpoint_map = {
            "create_infra": ("POST", "/api/infra"),
            "get_infra": ("GET", "/api/infra"),

            "create_vdev": ("POST", "/api/vdev"),
            "get_vdev": ("GET", "/api/vdev"),
            "mount_vdev": ("POST", "/api/vdev/mount"),

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