from ansible.plugins.lookup import LookupBase
import fcntl, psutil, uuid
import sys
import json
import termios
import os
import re
import json
import time
import shutil
import subprocess
from genericcmd import *
from func_timeout import func_timeout, FunctionTimedOut
import time as time_global
from inotifypath import *
from constants import NIOVA_LIBEXEC_DIR

def start_ncpc(cluster_params, Key, Value, Operation,
                                     OutfileName, IP_addr, Port, NumWrites, seqNo, lookout_uuid, nisd_uuid, cmd):
    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "ncpc")

    # Prepare path for log file.
    log_file = "%s/%s/%s_ncpc_log.txt" % (base_dir, raft_uuid, app_name)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_file, "w")
    logfile = "%s/%s/ncpclogfile.log" % (base_dir, raft_uuid)

    serviceRetry = "3"

    # Prepare config file path for ncpc
    ConfigPath = "%s/%s/gossipNodes" % (base_dir, raft_uuid)

    outfilePath = "%s/%s/%s_%s" % (base_dir, raft_uuid, OutfileName, uuid.uuid1())

    if Operation == "read":
        if seqNo != "" and NumWrites != "":
            process_popen = subprocess.Popen([bin_path, '-c', ConfigPath,
                                             '-l', logfile, '-o', Operation, '-j', outfilePath,
                                             '-k', Key, '-n', NumWrites, '-S', seqNo,'-sr', serviceRetry, '-ru', raft_uuid],
                                             stdout = fp, stderr = fp)
        elif NumWrites != "":
            process_popen = subprocess.Popen([bin_path, '-c', ConfigPath,
                                             '-l', logfile, '-o', Operation, '-j', outfilePath,
                                             '-k', Key, '-n', NumWrites, '-sr', serviceRetry, '-ru', raft_uuid],
                                             stdout = fp, stderr = fp)
        else:
            process_popen = subprocess.Popen([bin_path, '-c', ConfigPath,
                                             '-l', logfile, '-o', Operation, '-j', outfilePath,
                                             '-k', Key, '-sr', serviceRetry, '-ru', raft_uuid],
                                             stdout = fp, stderr = fp)
    elif Operation == "write":
        if NumWrites == "":
            process_popen = subprocess.Popen([bin_path, '-k', Key, '-v', Value,'-c', ConfigPath,
                                             '-l', logfile, '-o', Operation, '-j', outfilePath,
                                             '-a' , IP_addr, '-p', Port, '-sr', serviceRetry, '-ru', raft_uuid],
                                             stdout = fp, stderr = fp)
        else:
            process_popen = subprocess.Popen([bin_path, '-k', Key, '-v', Value,'-c', ConfigPath,
                                             '-l', logfile, '-o', Operation, '-j', outfilePath,
                                             '-a' , IP_addr, '-p', Port, '-n', NumWrites, '-sr', serviceRetry, '-ru', raft_uuid],
                                             stdout = fp, stderr = fp)
    elif Operation == "LookoutInfo":
        process_popen = subprocess.Popen([bin_path, '-c', ConfigPath, '-o', Operation, '-u',
                                            lookout_uuid, '-k', nisd_uuid, '-v', cmd,
                                            '-l', logfile, '-j', outfilePath, '-ru', raft_uuid],
                                            stdout = fp, stderr = fp)
    else:
        process_popen = subprocess.Popen([bin_path, '-k', Key,
                                             '-v', Value, '-c', ConfigPath,
                                             '-l', logfile, '-o', Operation, '-j', outfilePath, '-ru', raft_uuid],
                                             stdout = fp, stderr = fp)
    # Sync the log file so all the logs from ncpc gets written to log file.
    os.fsync(fp)
    return process_popen, outfilePath

def extracting_dictionary(cluster_params, input_values):
    Key = ""
    Value = ""
    IP_addr = ""
    Port = ""
    NumWrites = ""
    seqNo = ""
    lookout_uuid = ""
    nisd_uuid = ""
    cmd = ""


    if input_values['Operation'] == "write" and input_values['NoofWrites'] == "":
        # Start the ncpc_client and perform the specified operation e.g write/read/config.
        process,outfile = start_ncpc(cluster_params, input_values['Key'], input_values['Value'],
                                           input_values['Operation'], input_values['OutfileName'],
                                           input_values['IP_addr'], input_values['Port'], NumWrites,
                                           seqNo, lookout_uuid, nisd_uuid, cmd)
        if input_values['wait_for_outfile']:
            output_data = get_the_output(outfile)
            return output_data
        else:
            return outfile

    elif input_values['Operation'] == "write" and input_values['NoofWrites'] != "":
        # Start the ncpc_client and perform the specified operation e.g write/read/config.
        process,outfile = start_ncpc(cluster_params, Key, Value,
                                           input_values['Operation'], input_values['OutfileName'],
                                           IP_addr, Port, input_values['NoofWrites'], seqNo,
                                           lookout_uuid, nisd_uuid, cmd)
        if input_values['wait_for_outfile']:
            output_data = get_the_output(outfile)
            return output_data
        else:
            return outfile

    elif input_values['Operation'] == "read" and input_values['NoofWrites'] == "":
        # Start the ncpc_client and perform the specified operation e.g write/read/config.
        process,outfile = start_ncpc(cluster_params, input_values['Key'], Value,
                                           input_values['Operation'], input_values['OutfileName'],
                                           IP_addr, Port, NumWrites, seqNo, lookout_uuid, nisd_uuid, cmd)
        if input_values['wait_for_outfile']:
            output_data = get_the_output(outfile)
            return output_data
        else:
            return outfile

    elif input_values['Operation'] == "read" and input_values['NoofWrites'] != "":
        # Start the ncpc_client and perform the specified operation e.g write/read/config.
        process,outfile = start_ncpc(cluster_params, input_values['Key'], Value,
                                           input_values['Operation'], input_values['OutfileName'],
                                           IP_addr, Port, input_values['NoofWrites'], input_values['seqNo'],
                                           lookout_uuid, nisd_uuid, cmd)
        if input_values['wait_for_outfile']:
            output_data = get_the_output(outfile)
            return output_data
        else:
            return outfile

    elif input_values['Operation'] == "membership":
        # Start the ncpc_client and perform the specified operation e.g write/read/config.
        process,outfile = start_ncpc(cluster_params, Key, Value,
                                           input_values['Operation'], input_values['OutfileName'],
                                           IP_addr, Port, NumWrites, seqNo, lookout_uuid, nisd_uuid, cmd)
        output_data = get_the_output(outfile)
        return {"membership":output_data}

    elif input_values['Operation'] == "NISDGossip":
        # Start the ncpc_client and perform the specified operation e.g write/read/config.
        process,outfile = start_ncpc(cluster_params, Key, Value,
                                           input_values['Operation'], input_values['OutfileName'],
                                           IP_addr, Port, NumWrites, seqNo, lookout_uuid, nisd_uuid, cmd)
        output_data = get_the_output(outfile)
        return {"NISDGossip":output_data}

    elif input_values['Operation'] == "config":
        # Start the ncpc_client and perform the specified operation e.g write/read/config.
        process,outfile = start_ncpc(cluster_params, Key, Value,
                                           input_values['Operation'], input_values['OutfileName'],
                                           IP_addr, Port, NumWrites, seqNo, lookout_uuid, nisd_uuid, cmd)
        output_data = get_the_output(outfile)
        return {"config":output_data}

    elif input_values['Operation'] == "LookoutInfo":
        process,outfile = start_ncpc(cluster_params, Key, Value, input_values['Operation'],
                                                     input_values['OutfileName'], IP_addr, Port, NumWrites, seqNo,
                                                     input_values['lookout_uuid'], input_values['nisd_uuid'], input_values['cmd'])
        output_data = get_the_output(outfile)
        return {"LookoutInfo":output_data}


def get_the_output(outfilePath):
    outfile = outfilePath + '.json'
    counter = 0
    timeout = 300

    # Wait till the output json file gets created.
    while True:
        if not os.path.exists(outfile):
            counter += 1
            time.sleep(1)
            if counter == timeout:
                return {'outfile_status': -1}
        else:
            break

    # File exists, but may still be mid-write. Retry parsing until it's
    # valid JSON or we exhaust the same timeout budget.
    output_data = {}
    parse_counter = 0
    parse_timeout = 30
    while True:
        try:
            with open(outfile, "r", encoding="utf-8") as json_file:
                content = json_file.read()
                if not content.strip():
                    raise json.JSONDecodeError("empty file", content, 0)
                output_data = json.loads(content)
            break
        except (json.JSONDecodeError, ValueError):
            parse_counter += 1
            time.sleep(0.5)
            if parse_counter >= parse_timeout:
                return {'outfile_status': -1}

    json_data = {}
    json_data['outfile_status'] = 0
    json_data['output_data'] = output_data
    return json_data

def set_environment_variables(cluster_params,lookout_uuid):
    niova_lookout_ctl_interface_path = "%s/%s/niova_lookout/%s" % (cluster_params['base_dir'],
                                                           cluster_params['raft_uuid'], lookout_uuid)

    if os.path.exists(niova_lookout_ctl_interface_path):
        logging.info("file already exist")
    else:
        os.mkdir(niova_lookout_ctl_interface_path)

    #set environment variables
    os.environ['NIOVA_INOTIFY_BASE_PATH'] = niova_lookout_ctl_interface_path
    os.environ['NIOVA_LOCAL_CTL_SVC_DIR'] = niova_lookout_ctl_interface_path

    return niova_lookout_ctl_interface_path

def start_niova_lookout_process(cluster_params, uport):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    app_name = cluster_params['app_type']

    genericcmdobj = GenericCmds()
    lookout_uuid = genericcmdobj.generate_uuid()
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    ctl_interface_path = set_environment_variables(cluster_params, lookout_uuid)

    # Prepare path for log file.
    log_file = "%s/%s/%s_niova-lookout_%s_log.txt" % (base_dir, raft_uuid, app_name, lookout_uuid)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_file, "w")
    gossipNodes = "%s/%s/gossipNodes" % (base_dir, raft_uuid)
    
    #start niova block test process
    bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "nisdLookout")

      #writing the information of lookout uuids dict into raft_uuid.json
    recipe_conf = load_recipe_op_config(cluster_params)

    if not "lookout_uuid_dict" in recipe_conf:
        recipe_conf['lookout_uuid_dict'] = {}

    recipe_conf['lookout_uuid_dict'][lookout_uuid] = {}

    genericcmdobj.recipe_json_dump(recipe_conf)
    logging.info("starting niova-lookout process")
    http_port_path = "%s/%s/http_port.json" % (base_dir, raft_uuid)
    if app_name == "controlplane":
        process_popen = subprocess.Popen([bin_path, '-dir', str(ctl_interface_path), '-c', gossipNodes,
                                            '-n', lookout_uuid, '-r', raft_uuid,
                                            '-u', uport], stdout = fp, stderr = fp)
    else:
        process_popen = subprocess.Popen([bin_path, '-dir', str(ctl_interface_path), '-c', gossipNodes,
                                            '-n', lookout_uuid, '-r', raft_uuid,
                                            '-u', uport, '-pr', http_port_path], stdout = fp, stderr = fp)

    #Check if niova-lookout process exited with error
    if process_popen.poll() is None:
        logging.info("niova-lookout process started successfully")
    else:
        logging.info("niova-lookout failed to start")
        raise subprocess.SubprocessError(process_popen.returncode)

    
    #writing the information of lookout uuids dict into raft_uuid.json
    recipe_conf = load_recipe_op_config(cluster_params)

    #writing the information of lookout uuid in raft_process into raft_uuid.json
    pid = process_popen.pid
    ps = psutil.Process(pid)

    if not "raft_process" in recipe_conf:
        recipe_conf['raft_process'] = {}

    recipe_conf['raft_process'][lookout_uuid] = {}

    recipe_conf['raft_process'][lookout_uuid]['process_raft_uuid'] = lookout_uuid
    recipe_conf['raft_process'][lookout_uuid]['process_pid'] = pid
    recipe_conf['raft_process'][lookout_uuid]['process_uuid'] = lookout_uuid
    recipe_conf['raft_process'][lookout_uuid]['process_type'] = "lookout"
    recipe_conf['raft_process'][lookout_uuid]['process_app_type'] = app_name
    recipe_conf['raft_process'][lookout_uuid]['process_status'] = ps.status()

    genericcmdobj = GenericCmds()
    genericcmdobj.recipe_json_dump(recipe_conf)

    # Sync the log file so all the logs from niova-block-test gets written to log file.
    os.fsync(fp)

    return lookout_uuid

def load_recipe_op_config(cluster_params):
    recipe_conf = {}
    raft_json_fpath = "%s/%s/%s.json" % (cluster_params['base_dir'],
                                         cluster_params['raft_uuid'],
                                         cluster_params['raft_uuid'])
    if os.path.exists(raft_json_fpath):
        with open(raft_json_fpath, "r+", encoding="utf-8") as json_file:
            recipe_conf = json.load(json_file)

    return recipe_conf

def start_testApp(cluster_params, input_values):
    base_dir = cluster_params['base_dir']
    port = int(cluster_params['srv_port'])

    #Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "testApp")
    start_range = port
    end_range = port+50
    Port_range = str(start_range)+"-"+str(end_range)
    process_popen = subprocess.Popen([bin_path, '-p' , Port_range])

    #writing the information of testApp in raft_process into raft_uuid.json
    recipe_conf = load_recipe_op_config(cluster_params)
    pid = process_popen.pid
    ps = psutil.Process(pid)

    if not "testApp" in recipe_conf:
        recipe_conf['testApp'] = {}

    recipe_conf['testApp']['process_pid'] = pid

    genericcmdobj = GenericCmds()
    genericcmdobj.recipe_json_dump(recipe_conf)

def lease(cluster_params, Key, Value, Operation, OutfileName, Port):
    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "ncpc")

    # Prepare path for log file.
    log_file = "%s/%s/%s_ncpc_lease_log.txt" % (base_dir, raft_uuid, app_name)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_file, "w")
    logfile = "%s/%s/ncpclogfile.log" % (base_dir, raft_uuid)

    serviceRetry = "3"

    # Prepare config file path for ncpc
    ConfigPath = "%s/%s/gossipNodes" % (base_dir, raft_uuid)

    outfilePath = "%s/%s/%s" % (base_dir, raft_uuid, OutfileName)

    process_popen = subprocess.Popen([bin_path, '-c', ConfigPath, '-o', Operation, '-k', Key, '-v', Value,
                                            '-l', logfile, '-j', outfilePath, '-ru', raft_uuid, '-p', Port],
                                            stdout = fp, stderr = fp)

    os.fsync(fp)
    return process_popen, outfilePath

def extracting_dictionary_for_lease(cluster_params, input_values):
    process,outfile = lease(cluster_params, input_values['Key'], input_values['Value'],
                                           input_values['Operation'], input_values['OutfileName'],
                                           input_values['Port'])

    if input_values['wait_for_outfile']:
        output_data = get_the_output(outfile)
        return output_data
    else:
        return outfile

def extracting_values_for_gotest(log_file_path):
    """
    Reads the go_test_output.log file and extracts structured test data
    (e.g. test names, results, info messages). Returns a JSON-compatible dict.
    """
    if not os.path.exists(log_file_path):
        raise FileNotFoundError(f"Log file not found: {log_file_path}")

    with open(log_file_path, "r") as f:
        log_text = f.read()

    # Extract each Go test block between === RUN and --- PASS/FAIL
    test_blocks = re.findall(
        r"=== RUN\s+(.*?)\n(.*?)(?=--- (?:PASS|FAIL):|\Z)",
        log_text,
        re.DOTALL
    )

    results = []
    for test_name, block in test_blocks:
        # Extract "msg=" log lines
        info_msgs = re.findall(r'level=info msg="(.*?)"', block)
        # Extract Go [ERR] lines
        err_msgs = re.findall(r'\[ERR\].*', block)
        # Check test result
        passed = bool(re.search(rf"--- PASS: {test_name}", log_text))
        failed = bool(re.search(rf"--- FAIL: {test_name}", log_text))

        # Special parsing for structured key/value lines like "GetNisdCfg: [...]"
        structured_data = {}
        for msg in info_msgs:
            m = re.match(r"([A-Za-z0-9]+):\s*(\[.*\])", msg)
            if m:
                structured_data[m.group(1)] = m.group(2)

        results.append({
            "test_name": test_name.strip(),
            "passed": passed,
            "failed": failed,
            "info_messages": info_msgs,
            "error_messages": err_msgs,
            "structured_data": structured_data
        })

    summary = {
        "total_tests": len(results),
        "passed": sum(1 for t in results if t["passed"]),
        "failed": sum(1 for t in results if t["failed"]),
        "tests": results
    }

    # Save structured JSON
    json_path = log_file_path.replace(".log", ".json")
    with open(json_path, "w") as jf:
        json.dump(summary, jf, indent=2)

    return summary

def run_go_test(cluster_params, input_values):
    """
    Run Go test for controlplane client with exported RAFT_ID and GOSSIP_NODES_PATH,
    then parse and return structured test results.
    """
    raft_id = cluster_params['raft_uuid']
    base_dir = cluster_params['base_dir']
    raft_dir = os.path.join(base_dir, raft_id)

    # Resolve gossipNodes file path
    gossip_nodes_path = os.path.join(raft_dir, "gossipNodes.json")
    if not os.path.exists(gossip_nodes_path):
        gossip_nodes_path = os.path.join(raft_dir, "gossipNodes")

    # Validate Go test path
    go_test_path = input_values.get('test_path')
    go_test_name = input_values.get('test_name')
    go_test_output_file = input_values.get('output_file')
    if not go_test_path or not os.path.exists(go_test_path):
        raise FileNotFoundError(f"Go test path does not exist: {go_test_path}")

    if not go_test_name:
        raise ValueError("test_name must be provided")

    log_file = os.path.join(base_dir, raft_id, f"{go_test_output_file}.log")

    # Build regex exactly like Go expects
    test_regex = f"^{go_test_name}$"

    # Run the Go tests
    with open(log_file, "w") as fp:
        env = os.environ.copy()
        env["RAFT_ID"] = raft_id
        env["GOSSIP_NODES_PATH"] = gossip_nodes_path

        cmd = ["./client.test", "-test.v", "-test.run", go_test_name]

        process = subprocess.Popen(cmd, cwd=go_test_path, env=env,
                                   stdout=fp, stderr=subprocess.STDOUT)
        ret = process.wait(timeout=300)

    # Extract structured test information
    test_summary = extracting_values_for_gotest(log_file)

    # Combine run metadata + parsed data
    result = {
        "raft_id": raft_id,
        "gossip_nodes_path": gossip_nodes_path,
        "test_path": go_test_path,
        "test_name": go_test_name,
        "test_file": input_values.get("test_file"),
        "return_code": ret,
        "log_file": log_file,
        "parsed_results": test_summary
    }

    return result

class LookupModule(LookupBase):
    def run(self,terms,**kwargs):
        #Get lookup parameter values
        process_type = terms[0]
        input_values = terms[1]

        cluster_params = kwargs['variables']['ClusterParams']

        #export NIOVA_THREAD_COUNT
        os.environ['NIOVA_THREAD_COUNT'] = cluster_params['nthreads']

        if process_type == "ncpc":
            data = extracting_dictionary(cluster_params, input_values)
            return [data]

        if process_type == "lease":            
            data = extracting_dictionary_for_lease(cluster_params, input_values)

            return [data]

        elif process_type == "niova-lookout":
            niova_lookout_path = "%s/%s/niova_lookout" % (cluster_params['base_dir'],
                                                           cluster_params['raft_uuid'])
            if os.path.exists(niova_lookout_path):
                print("file already exist")
            else:
                os.mkdir(niova_lookout_path)
             
            niova_lookout_process = start_niova_lookout_process(cluster_params, input_values['uport'])
            
            return [niova_lookout_process]

        elif process_type == "testApp":
            start_test_application = start_testApp(cluster_params, input_values)

            return [start_test_application]

        elif process_type == "gotest":
            # Run Go test for controlplane client
            data = run_go_test(cluster_params, input_values)
            return [data]

        elif process_type == "prometheus":
            hport = input_values['Hport']

            #Prepare path for executables.
            binary_dir = os.getenv('NIOVA_BIN_PATH')
            prometheus_path = '%s/prometheus' % binary_dir

            # Set the target ports in the targets.json file for prometheus exporter

            if cluster_params['prometheus_support'] == "1":
                prom_targets_path = os.environ['PROMETHEUS_PATH'] + '/' + "targets.json"
                prom_targets = []

                if hport == "":
                    http_port_path = "%s/%s/http_port.json" % (cluster_params['base_dir'],
                                                           cluster_params['raft_uuid'])
                    with open(http_port_path, "r") as f:
                        Lines = f.readlines()
                        for line in Lines:
                            prom_targets.append({'targets':['localhost:'+str(line)]})
                    config_file_path = "%s/prometheus.yml" % binary_dir
                    prometheus_process = subprocess.Popen([prometheus_path, "--config.file=%s" % config_file_path])

                    return [prometheus_process]

                else:
                    with open(prom_targets_path, "r") as f:
                        prom_targets = json.load(f)
                        prom_targets.append({'targets':['localhost:'+str(hport)]})
                    
                with open(prom_targets_path, "w") as f:
                    json.dump(prom_targets, f)

                return [prom_targets]

            return ["prometheus_support_disabled"]
