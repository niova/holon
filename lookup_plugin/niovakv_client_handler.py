from ansible.plugins.lookup import LookupBase
import fcntl
import sys
import json
import termios
import os
import shutil, os
import time
import subprocess
from constants import NIOVA_LIBEXEC_DIR

def start_niovakv_subprocess(cluster_params, Operation, Key, Value, OutfileName,
                     NumRequest, MultiKey, Sequential):
    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "nkvc")

    # Prepare path for log file.
    log_file = "%s/%s/%s_log.txt" % (base_dir, raft_uuid, app_name)

    # Open the log file to pass the fd to subprocess.Popen
    fp = open(log_file, "w")
    logfile = "%s/%s/niovakvclientlogfile.log" % (base_dir, raft_uuid)

    # Prepare config file path for niovakv_client
    config_path = "%s/%s/gossipNodes" % (base_dir, raft_uuid)

    outfilePath = "%s/%s/%s" % (base_dir, raft_uuid, OutfileName)

    if MultiKey == False:
        if Operation == "write":
            process_popen = subprocess.Popen([bin_path,'-c', config_path,
                                             '-l', logfile, '-o', Operation, '-k', Key,
                                             '-v', Value, '-r', outfilePath , '-n', NumRequest, '-ru', raft_uuid],
                                             stdout = fp, stderr = fp)
        elif Operation == "read":
            process_popen = subprocess.Popen([bin_path,'-c', config_path, '-o', Operation,
                                              '-l', logfile, '-k', Key, '-v', Value,
                                              '-r', outfilePath, '-n', NumRequest, '-ru', raft_uuid],
                                              stdout = fp, stderr = fp)

        elif Operation == "leader":         # operation for get leader
            process_popen = subprocess.Popen([bin_path,'-c', config_path, '-o', Operation,
                                              '-r', outfilePath, '-ru', raft_uuid],
                                              stdout = fp, stderr = fp)
        elif Operation == "membership":
            process_popen = subprocess.Popen([bin_path,'-c', config_path, '-o', Operation,
                                              '-r', outfilePath, '-ru', raft_uuid],
                                              stdout = fp, stderr = fp)
        else:
           logging.error("Invalid Operation passed to start niovakv_client: %s", Operation)
           os.fsync(fp)
           exit(1)

    elif (( MultiKey == True ) and ( Sequential == True )):

        logfile = "%s/%s/niovakvclientlogfile_multi_sequential.log" % (base_dir, raft_uuid)
        outfilePath = "%s/%s/%s" % (base_dir, raft_uuid, OutfileName)
        process_popen = subprocess.Popen([bin_path,'-c', config_path, '-l' , logfile,
                                          '-s', "y", '-n', NumRequest, '-k', Key,
                                          '-v', Value, '-r', outfilePath, '-ru', raft_uuid],
                                          stdout = fp, stderr = fp)
    else:
        logfile = "%s/%s/niovakvclientlogfile_multi_concurrent.log" % (base_dir, raft_uuid)
        outfilePath = "%s/%s/%s" % (base_dir, raft_uuid , OutfileName)
        process_popen = subprocess.Popen([bin_path,'-c', config_path, '-l', logfile,
                                          '-n', NumRequest, '-k', Key, '-v', Value,
                                          '-r' , outfilePath, '-ru', raft_uuid],
                                          stdout = fp, stderr = fp)

    # Sync the log file so all the logs from niovakv client gets written to log file.
    os.fsync(fp)
    return process_popen, outfilePath

def start_lkvt_subprocess(cluster_params, database_type, size_of_key,
                        key_prefix, seed_random_generator, size_of_value,
                        no_of_operations, precent_put_get, no_of_concurrent_req,
                        choose_algorithm, specific_server_name, outfileName):
    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "lkvt")

    # Prepare path for log file.
    log_file = "%s/%s/%s_LKVT_log.txt" % (base_dir, raft_uuid, app_name)

    # Open the log file to pass the fd to subprocess.Popen
    fp = open(log_file, "w")
    logfile = "%s/%s/lkvtclientlogfile.log" % (base_dir, raft_uuid)

    # Prepare config file path for niovakv_client
    config_path = "%s/%s/gossipNodes" % (base_dir, raft_uuid)

    outfilePath = "%s/%s/%s" % (base_dir, raft_uuid, outfileName)

    process_popen = subprocess.Popen([bin_path, '-d', database_type, '-ks', size_of_key,
                                         '-kp', key_prefix, '-s', seed_random_generator,
                                         '-vs', size_of_value, '-n', no_of_operations,
                                         '-pp', precent_put_get, '-c', no_of_concurrent_req,
                                         '-jp', outfilePath ,'-cp', config_path,
                                         '-ca', choose_algorithm, '-ss', specific_server_name,
                                         '-ru', raft_uuid],
                                         stdout = fp, stderr = fp)

    # Sync the log file so all the logs from niovakv client gets written to log file.
    os.fsync(fp)
    return process_popen, outfilePath

def get_the_output(outfilePath, client_type):
    outfile = outfilePath + '.json'
    counter = 0
    timeout = 100

    # Wait till the output json file gets created.
    while True:
        if not os.path.exists(outfile):
            counter += 1
            time.sleep(1)
            if counter == timeout:
                return {'outfile_status':-1}
        else:
            break

    output_data = {}

    if client_type == "lkvt":
        json_data = {}
        with open(outfile, "r+", encoding="utf-8") as json_file:
            output_data = json.load(json_file)
        json_data['outfile_status'] = 0
        json_data['output_data'] = output_data
        return json_data
    else:
        with open(outfile, "r+", encoding="utf-8") as json_file:
            output_data = json.load(json_file)
        return output_data

def extract_niovakv_client_values_perform_operation(cluster_params, client_type, input_values):
        Operation = input_values['Operation']
        Key = input_values['Key']
        Value = input_values['Value']
        OutfileName = input_values['OutfileName']
        NumRequest = str(input_values['NumRequest'])
        MultiKey = input_values['MultiKey']
        Sequential = input_values['Sequential']

        # Start the niovakv_client and perform the specified operation e.g write/read/getLeader.
        process,outfile = start_niovakv_subprocess(cluster_params, Operation, Key,
                                           Value, OutfileName, NumRequest,
                                           MultiKey, Sequential)
        output_data = get_the_output(outfile, client_type)

        if Operation == "write":
                return {"write":output_data['write']}
        elif Operation == "read":
                return {"read":output_data['read']}
        elif Operation == "getLeader":
                return {"getLeader":output_data['getLeader']}
        elif Operation == "membership":
                return {"membership":output_data}
        else:
                return output_data

def extract_lkvt_client_values_perform_operation(cluster_params, client_type, input_values):
        wait_for_outfile = input_values["wait_for_outfile"]
        if wait_for_outfile == False:
            database_type = str(input_values["database_type"])
            size_of_key = str(input_values["size_of_key"])
            key_prefix = str(input_values["key_prefix"])
            seed_random_generator = str(input_values["seed_random_generator"])
            size_of_value  = str(input_values["size_of_value"])
            no_of_operations = str(input_values["no_of_operations"])
            precent_put_get = str(input_values["precent_put_get"])
            no_of_concurrent_req = str(input_values["no_of_concurrent_req"])
            choose_algorithm = str(input_values["choose_algorithm"])
            specific_server_name = str(input_values["specific_server_name"])
            outfileName = str(input_values["outfileName"])

            # Start the niovakv_client and perform the specified operation e.g write/read/getLeader.
            process,outfile = start_lkvt_subprocess(cluster_params, database_type, size_of_key, key_prefix,
                                                         seed_random_generator, size_of_value,
                                                         no_of_operations, precent_put_get, no_of_concurrent_req,
                                                         choose_algorithm, specific_server_name, outfileName)
            return outfile
        else:
            outfile_path = str(input_values['outfile_path'])
            wait_for_outfile = input_values['wait_for_outfile']

            if wait_for_outfile == True:
                output_data = get_the_output(outfile_path, client_type)
                return output_data

class LookupModule(LookupBase):
    def run(self,terms,**kwargs):
        #Get lookup parameter values
        cluster_params = kwargs['variables']['ClusterParams']

        #export NIOVA_THREAD_COUNT
        os.environ['NIOVA_THREAD_COUNT'] = cluster_params['nthreads']

        client_type = terms[0]
        input_values = terms[1]

        if client_type == "niovakv":
                result = extract_niovakv_client_values_perform_operation(cluster_params, client_type, input_values)

        elif client_type == "lkvt":
                result = extract_lkvt_client_values_perform_operation(cluster_params, client_type, input_values)

        return [result]
