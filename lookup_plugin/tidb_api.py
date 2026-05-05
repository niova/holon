from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import os
import json
import requests

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError


class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):
        if len(terms) < 2:
            raise AnsibleError("Usage: lookup('mdsvc_tidb_api', METHOD, PATH, ...)")

        method = terms[0].upper()
        path = terms[1]

        base_url = kwargs.get("base_url", os.getenv("MDSVC_API_URL", "http://localhost:8081"))
        file_path = kwargs.get("file", None)
        body = kwargs.get("body", None)
        headers = kwargs.get("headers", {"Content-Type": "application/json"})
        timeout = kwargs.get("timeout", 10)

        url = f"{base_url}{path}"

        # -------------------------
        # Prepare payload
        # -------------------------
        payload = None

        if file_path:
            if not os.path.exists(file_path):
                raise AnsibleError(f"JSON file not found: {file_path}")

            with open(file_path, "r") as f:
                try:
                    payload = json.load(f)
                except Exception as e:
                    raise AnsibleError(f"Invalid JSON in file {file_path}: {e}")

        elif body:
            if isinstance(body, str):
                try:
                    payload = json.loads(body)
                except Exception:
                    raise AnsibleError("Body must be valid JSON string")
            elif isinstance(body, dict):
                payload = body
            else:
                raise AnsibleError("Unsupported body format")

        # -------------------------
        # Perform request
        # -------------------------
        try:
            if method == "POST":
                resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            elif method == "GET":
                resp = requests.get(url, headers=headers, timeout=timeout)
            else:
                raise AnsibleError(f"Unsupported method: {method}")
        except Exception as e:
            raise AnsibleError(f"HTTP request failed: {e}")

        # -------------------------
        # Handle response
        # -------------------------
        try:
            data = resp.json()
        except Exception:
            raise AnsibleError(f"Invalid JSON response: {resp.text}")

        if resp.status_code not in [200, 201]:
            raise AnsibleError(f"API failed [{resp.status_code}]: {data}")

        result = {
            "status_code": resp.status_code,
            "data": data
        }

        # -------------------------
        # Extract useful fields
        # -------------------------
        extracted = self._extract_fields(data)
        if extracted:
            result.update(extracted)

        return [result]

    # -------------------------
    # Helpers
    # -------------------------

    def extract_fields(self, data):
        """
        Try to extract commonly used values like UUIDs
        """
        extracted = {}

        # Common patterns
        if isinstance(data, dict):
            if "uuid" in data:
                extracted["uuid"] = data["uuid"]

            if "vdev_uuid" in data:
                extracted["vdev_uuid"] = data["vdev_uuid"]

            if "infra_id" in data:
                extracted["infra_id"] = data["infra_id"]

            # Nested patterns (common in APIs)
            if "data" in data and isinstance(data["data"], dict):
                inner = data["data"]
                for key in ["uuid", "vdev_uuid", "infra_id"]:
                    if key in inner:
                        extracted[key] = inner[key]

        return extracted