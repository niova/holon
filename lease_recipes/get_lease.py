#!/usr/bin/env python3

import os
import subprocess
import uuid
import json
import time
import argparse
import sys

def generate_uuid_pairs(count):
    clients = []
    resources = []

    for _ in range(count):
        clients.append(str(uuid.uuid4()))
        resources.append(str(uuid.uuid4()))

    return clients, resources


def run_get(bin_path, raft_uuid, client, resource, base_dir, index):
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = '%s/leaseClient' % binary_dir
    outfile = f"{base_dir}/lease_{index}_{uuid.uuid4()}"
    logfile = f"{base_dir}/lease_{index}.log"

    cmd = [
        bin_path,
        "-o", "GET",
        "-u", client,
        "-v", resource,
        "-ru", raft_uuid,
        "-n", "1",
        "-j", outfile,
        "-l", logfile
    ]

    subprocess.run(cmd, check=True)

    json_file = outfile + ".json"
    timeout = 120
    start = time.time()

    while not os.path.exists(json_file):
        if time.time() - start > timeout:
            raise TimeoutError("Timeout waiting for lease JSON")
        time.sleep(0.5)

    with open(json_file, "r") as f:
        return json.load(f)

def bulk_get(args):
    clients, resources = generate_uuid_pairs(args.total)

    success_count = 0
    failures = 0

    for i in range(args.total):
        try:
            data = run_get(
                args.raft,
                clients[i],
                resources[i],
                args.output,
                i
            )

            lease_res = data[0]["LeaseRes"]

            if (
                lease_res["Status"] == 0 and
                lease_res["Client"] == clients[i] and
                lease_res["Resource"] == resources[i]
            ):
                success_count += 1
            else:
                failures += 1

        except Exception as e:
            print(f"Lease {i} failed: {e}")
            failures += 1

    summary = {
        "total_requested": args.total,
        "success_count": success_count,
        "failures": failures
    }

    return summary

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raft", required=True)
    parser.add_argument("--total", type=int, required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    result = bulk_get(args)

    print(json.dumps(result, indent=2))