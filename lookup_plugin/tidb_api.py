from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import json
import os
import requests
from datetime import datetime

# =========================================================
# Generic Helpers
# =========================================================

def load_payload(file_path=None, body=None):

    if file_path:

        if not os.path.exists(file_path):
            raise AnsibleError(
                f"JSON file not found: {file_path}"
            )

        with open(file_path, "r") as fp:

            try:
                return json.load(fp)

            except Exception as e:
                raise AnsibleError(
                    f"Invalid JSON in file "
                    f"{file_path}: {e}"
                )

    if body:

        if isinstance(body, dict):
            return body

        if isinstance(body, str):

            try:
                return json.loads(body)

            except Exception:
                raise AnsibleError(
                    "Body must be valid JSON string"
                )

        raise AnsibleError(
            "Unsupported body format"
        )

    return None

def get_log_file(log_dir):

    os.makedirs(log_dir, exist_ok=True)

    return os.path.join(
        log_dir,
        "mdsvc_api.log"
    )

def write_log(log_file,
              method,
              url,
              payload,
              response_status,
              response_data):

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    with open(log_file, "a") as logf:

        logf.write("\n")
        logf.write("=" * 80 + "\n")
        logf.write(f"[{timestamp}] API REQUEST\n")
        logf.write("=" * 80 + "\n")

        logf.write(f"METHOD: {method}\n")
        logf.write(f"URL: {url}\n")

        if payload:
            logf.write(
                "REQUEST BODY:\n"
            )

            logf.write(
                json.dumps(payload, indent=2)
            )

            logf.write("\n")

        logf.write(
            f"STATUS CODE: {response_status}\n"
        )

        logf.write(
            "RESPONSE:\n"
        )

        logf.write(
            json.dumps(response_data, indent=2)
        )

        logf.write("\n")

# =========================================================
# Request Helpers
# =========================================================

def perform_request(api_params):

    method = api_params['method']
    url = api_params['url']
    payload = api_params['payload']
    headers = api_params['headers']
    timeout = api_params['timeout']

    try:

        if method == "POST":

            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=timeout
            )

        elif method == "GET":

            response = requests.get(
                url,
                headers=headers,
                timeout=timeout
            )

        else:
            raise AnsibleError(
                f"Unsupported method: {method}"
            )

    except Exception as e:
        raise AnsibleError(
            f"HTTP request failed: {e}"
        )

    return response

def parse_response(response):

    try:
        response_data = response.json()

    except Exception:
        raise AnsibleError(
            f"Invalid JSON response: "
            f"{response.text}"
        )

    if response.status_code not in [200, 201]:

        raise AnsibleError(
            f"API failed "
            f"[{response.status_code}]: "
            f"{response_data}"
        )

    return response_data

# =========================================================
# Response Helpers
# =========================================================

def extract_fields(response_data):

    extracted = {}

    if not isinstance(response_data, dict):
        return extracted

    common_keys = [
        "uuid",
        "vdev_id",
        "infra_id",
        "chunk_uuid",
        "nisd_uuid"
    ]

    for key in common_keys:

        if key in response_data:
            extracted[key] = response_data[key]

    if (
        "data" in response_data and
        isinstance(response_data["data"], dict)
    ):

        inner = response_data["data"]

        for key in common_keys:

            if key in inner:
                extracted[key] = inner[key]

    return extracted

# =========================================================
# API Operation Helpers
# =========================================================

def perform_api_operation(api_params):

    response = perform_request(api_params)

    response_data = parse_response(response)

    write_log(
        api_params['log_file'],
        api_params['method'],
        api_params['url'],
        api_params['payload'],
        response.status_code,
        response_data
    )

    result = {
        "status_code": response.status_code,
        "data": response_data,
        "log_file": api_params['log_file']
    }

    extracted_fields = extract_fields(
        response_data
    )

    if extracted_fields:
        result.update(extracted_fields)

    return result

# =========================================================
# Main Lookup Module
# =========================================================

class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):

        if len(terms) < 2:

            raise AnsibleError(
                "Usage: "
                "lookup("
                "'mdsvc_tidb_api', "
                "METHOD, "
                "PATH)"
            )

        method = terms[0].upper()
        path = terms[1]

        base_url = kwargs.get(
            "base_url",
            os.getenv(
                "MDSVC_API_URL",
                "http://localhost:8081"
            )
        )

        payload = load_payload(
            kwargs.get("file"),
            kwargs.get("body")
        )

        log_dir = kwargs.get(
            "log_dir",
            "./logs"
        )

        api_params = {
            "method": method,
            "url": f"{base_url}{path}",
            "payload": payload,
            "headers": kwargs.get(
                "headers",
                {
                    "Content-Type": "application/json"
                }
            ),
            "timeout": kwargs.get(
                "timeout",
                10
            ),
            "log_file": get_log_file(log_dir)
        }

        result = perform_api_operation(
            api_params
        )

        return [result]
