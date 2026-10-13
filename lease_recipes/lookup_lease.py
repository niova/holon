#!/usr/bin/env python3

import os
import json
import time
import subprocess
import uuid
import argparse


# -------------------------------------------------
# Wait for JSON output file
# -------------------------------------------------
def get_the_output(outfilePath):
    outfile = outfilePath + '.json'
    timeout = 100
    start = time.time()

    while not os.path.exists(outfile):
        if time.time() - start > timeout:
            return {'outfile_status': -1}
        time.sleep(0.1)

    with open(outfile, "r", encoding="utf-8") as json_file:
        json_data = json.load(json_file)

    return {
        'outfile_status': 0,
        'output_data': json_data
    }


# -------------------------------------------------
# Set environment variables (same as plugin)
# -------------------------------------------------
def set_environment_variables(cluster_params):
    ctl_interface_path = "%s/%s/ctl-interface" % (
        cluster_params['base_dir'],
        cluster_params['raft_uuid']
    )

    config_path = "%s/%s/configs" % (
        cluster_params['base_dir'],
        cluster_params['raft_uuid']
    )

    os.makedirs(ctl_interface_path, exist_ok=True)

    os.environ['NIOVA_INOTIFY_BASE_PATH'] = ctl_interface_path
    os.environ['NIOVA_LOCAL_CTL_SVC_DIR'] = config_path

    return ctl_interface_path


# -------------------------------------------------
# Lease Operation (copied logic from plugin)
# -------------------------------------------------
def lease_operation(cluster_params, operation, client,
                    resource, numOfLeases,
                    getLeaseOutfile, outFileName):

    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']

    binary_dir = os.getenv('NIOVA_BIN_PATH')
    if not binary_dir:
        raise Exception("NIOVA_BIN_PATH not set")

    bin_path = f"{binary_dir}/leaseClient"

    leaseLogFile = f"{base_dir}/{raft_uuid}/leaseClient_{operation}.log"

    outfilePath = "%s/%s/%s_%s" % (
        base_dir,
        raft_uuid,
        outFileName,
        uuid.uuid1()
    )

    set_environment_variables(cluster_params)

    cmd = [
        bin_path,
        "-o", operation,
        "-ru", raft_uuid,
        "-n", str(numOfLeases),
        "-f", getLeaseOutfile,
        "-j", outfilePath,
        "-l", leaseLogFile
    ]

    # Add client/resource if provided
    if resource:
        cmd.extend(["-v", resource])
    if client:
        cmd.extend(["-u", client])

    process = subprocess.Popen(cmd)
    process.wait()

    return outfilePath


# -------------------------------------------------
# Extracting dictionary (same behavior as lookup)
# -------------------------------------------------
def extracting_dictionary(cluster_params, operation, input_values):

    outfile = lease_operation(
        cluster_params,
        operation,
        input_values.get('client', ''),
        input_values.get('resource', ''),
        input_values.get('numOfLeases', '1'),
        input_values.get('getLeaseOutfile', ''),
        input_values.get('outFileName', 'lease_out')
    )

    output_data = get_the_output(outfile)

    # Match lookup plugin structure
    output_data['detailedJsonPath'] = outfile
    output_data['singleResponseJson'] = "%s_summary" % outfile

    return output_data


# -------------------------------------------------
# Main CLI
# -------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--operation", required=True)
    parser.add_argument("--base_dir", required=True)
    parser.add_argument("--raft_uuid", required=True)
    parser.add_argument("--num", default="1")
    parser.add_argument("--client", default="")
    parser.add_argument("--resource", default="")
    parser.add_argument("--outfile", default="lease_out")
    parser.add_argument("--getLeaseOutfile", default="")

    args = parser.parse_args()

    cluster_params = {
        "base_dir": args.base_dir,
        "raft_uuid": args.raft_uuid,
        "app_type": "lease"
    }

    input_values = {
        "client": args.client,
        "resource": args.resource,
        "numOfLeases": args.num,
        "getLeaseOutfile": args.getLeaseOutfile,
        "outFileName": args.outfile
    }

    result = extracting_dictionary(cluster_params, args.operation, input_values)

    print(json.dumps(result, indent=2))