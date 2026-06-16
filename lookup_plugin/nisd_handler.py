from ansible.plugins.lookup import LookupBase
import fcntl, psutil
import sys
import json
import termios
import os
import time
import shutil
import subprocess
from genericcmd import *
from func_timeout import func_timeout, FunctionTimedOut
import time as time_global
import logging
from constants import NIOVA_BIN_DIR

def initialize_logger(log_file):

    #now we will Create and configure logger
    logging.basicConfig(filename=log_file,
                                        format='%(asctime)s [%(filename)s:%(lineno)d] %(message)s',
                                        filemode='a+')

    #Let us Create an object
    logger=logging.getLogger()

    #Now we are going to Set the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    return logger

def set_nisd_environ_variables(minio_config_path):
    # Read the JSON file
    with open(os.path.normpath(minio_config_path), 'r') as file:
        config = json.load(file)
    
    # Extract values
    endpoint = config.get("endpoint", "")
    access_key = config.get("accessKey", "")
    secret_key = config.get("secretKey", "")
    region = config.get("region", "")
    
    # Set environment variables
    os.environ["NIOVA_BLOCK_AWS_URL"] = f"http://{endpoint}/paroscale-test"
    os.environ["NIOVA_BLOCK_AWS_OPTS"] = f"aws:amz:{region}:s3"
    os.environ["NIOVA_BLOCK_AWS_AUTH"] = f"{access_key}:{secret_key}"
    
    # Print environment variable values to verify
    print("Environment variables have been set:")
    print("NIOVA_BLOCK_AWS_URL =", os.environ["NIOVA_BLOCK_AWS_URL"])
    print("NIOVA_BLOCK_AWS_OPTS =", os.environ["NIOVA_BLOCK_AWS_OPTS"])
    print("NIOVA_BLOCK_AWS_AUTH =", os.environ["NIOVA_BLOCK_AWS_AUTH"])


def run_nisd_command(cluster_params, input_values):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']

    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = os.path.join(binary_dir, NIOVA_BIN_DIR, "nisd")
    app_name = cluster_params['app_type']

    base_path = "%s/%s/" % (base_dir, raft_uuid)

    nisd_uuid = input_values['nisd_uuid']
    device_path = input_values['nisd_device_path']
    peer_port = input_values["peer_port"]
    client_port = input_values["client_port"]
    enable_authentication = input_values["enable_auth"]

    s3config = '/%s/s3.config.example' % binary_dir
    bin_path = os.path.normpath(bin_path)
    set_nisd_environ_variables(s3config)

    sock_dir = f"/tmp/.niova/{nisd_uuid}"
    os.makedirs(sock_dir, exist_ok=True)

    if enable_authentication == 1:
        os.environ["NIOVA_NISD_SECRET"] = "Nisd-secret"
        os.environ["NIOVA_NISD_DO_TOKEN_VALIDATION"] = '1'

    os.environ["NIOVA_INOTIFY_BASE_PATH"] = "%s/%s/nisd-interface" % (base_dir, raft_uuid)
    os.environ["NIOVA_BLOCK_SOCK_PATH"] = f"/tmp/.niova/{nisd_uuid}" 
    
    env = os.environ.copy()
    os.environ["NIOVA_BLOCK_TCP_PEER_PORT"] = str(peer_port)
    os.environ["NIOVA_BLOCK_TCP_CLIENT_PORT"] = str(client_port)

    command = [bin_path, "-u", nisd_uuid, "-d", device_path]

    # Prepare nisd log file path
    log_file_path = "%s/%s/nisd_%s.log" % (base_dir, raft_uuid, nisd_uuid)
    logger = initialize_logger(log_file_path)

    logger.info("Executing nisd command: %s", command)    
    # Open log file in append mode
    with open(log_file_path, 'a') as log_file:
        # Launch the command as a non-blocking subprocess
        process = subprocess.Popen(command, stdout=log_file, stderr=log_file, text=True, cwd=base_path)
        
        # Log the process ID for reference
        logger.info("NISD command started with PID %d. Logs will be written to %s", process.pid, log_file_path)    
    recipe_conf = load_recipe_op_config(cluster_params)

    pid = process.pid
    ps = psutil.Process(pid)

    if not "nisd_process" in recipe_conf:
        recipe_conf['nisd_process'] = {}

    recipe_conf['nisd_process']['process_pid'] = pid
    recipe_conf['nisd_process']['process_type'] = "nisd_process"
    recipe_conf['nisd_process']['process_app_type'] = app_name
    recipe_conf['nisd_process']['process_status'] = ps.status()

    genericcmdobj = GenericCmds()
    genericcmdobj.recipe_json_dump(recipe_conf)

    # Return the process to allow further handling if needed
    return [process]

def install_linux_modules():
    try:
        update_command = "sudo apt update"
        update_result = subprocess.run(update_command, shell=True, check=True, text=True, capture_output=True)

        install_command = "sudo apt install linux-modules-extra-$(uname -r)"
        install_result = subprocess.run(install_command, shell=True, check=True, text=True, capture_output=True)

        return update_result.stdout + install_result.stdout
    except subprocess.CalledProcessError as e:
        return f"An error occurred: {e.stderr}"

def load_kernel_module(module_name):
    install_linux_modules()
    try:
        # Run the modprobe command to load the kernel module
        subprocess.run(["sudo", "modprobe", module_name], check=True)
        print(f"Module '{module_name}' loaded successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to load module '{module_name}': {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def replace_last_path_segment(path, old_segment, new_segment):
    # Split the path into head and tail
    head, tail = os.path.split(path)
    
    # Check if the last segment matches the old_segment to replace
    if tail == old_segment:
        # Replace it with the new segment
        return os.path.join(head, new_segment)
    else:
        # Return the path unchanged if the last segment doesn't match
        return path

# start a ublk device of size 8GB
def run_niova_ublk(cluster_params, cntl_uuid):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    
    #format and run the niova-block-ctl
    bin_path = os.path.join(binary_dir, NIOVA_BIN_DIR, "niova-ublk")
    bin_path = os.path.normpath(bin_path)
    app_name = cluster_params['app_type']
    base_path = "%s/%s" % (base_dir, raft_uuid)

    # generate ublk uuid
    genericcmdobj = GenericCmds()
    ublk_uuid = genericcmdobj.generate_uuid()

    # Prepare path for log file.
    log_file = "%s/%s/ublk_%s_log.txt" % (base_dir, raft_uuid, ublk_uuid)

    # Initialize the logger
    logger = initialize_logger(log_file)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_file, "a+")

    niova_lib_path = os.path.normpath(f'{binary_dir}/lib')
    default_lib_path = "/usr/local/lib"
    ld_library_path = f"{niova_lib_path}:{default_lib_path}:{os.environ.get('LD_LIBRARY_PATH', '')}"
    os.environ["LD_LIBRARY_PATH"] = ld_library_path
    logger.info(f"LD_LIBRARY_PATH set to: {os.environ['LD_LIBRARY_PATH']}")

    command = [
        bin_path,
        "-s", "8589934592",
        "-t", cntl_uuid,
        "-v", ublk_uuid,
        "-u", ublk_uuid,
        "-q", "128",
        "-b", "1048576"
    ]
    
    # Combine the environment variable and command into a single string
    full_command = " ".join(str(item) for item in command)
    logger.info(f"ublk command: {full_command}")
    try:
        # Run the command
        process = subprocess.Popen(full_command, stdout=fp, stderr=fp, shell=True, executable="/bin/bash",cwd=base_path)
        logger.info("Command executed successfully.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    
    recipe_conf = load_recipe_op_config(cluster_params)

    pid = process.pid
    ps = psutil.Process(pid)

    if not "ublk_process" in recipe_conf:
        recipe_conf['ublk_process'] = {}

    recipe_conf['ublk_process']['process_pid'] = pid
    recipe_conf['ublk_process']['process_type'] = "ublk_process"
    recipe_conf['ublk_process']['process_app_type'] = app_name
    recipe_conf['ublk_process']['process_status'] = ps.status()

    genericcmdobj = GenericCmds()
    genericcmdobj.recipe_json_dump(recipe_conf)

    # Sync the log file so all the logs from run_niova_ublk gets written to log file.
    os.fsync(fp)
    return [ublk_uuid]

# this method is similar to start_niova_block_ctl_process but the difference is it doesn't create the device internally
def run_niova_block_ctl(cluster_params, input_value):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']

    genericcmdobj = GenericCmds()

    # Accept nisd_uuid from input, otherwise generate one
    nisd_uuid = input_value.get("nisd_uuid")
    if not nisd_uuid:
        nisd_uuid = genericcmdobj.generate_uuid()

    # Prepare path for log file.
    base_path = "%s/%s" % (base_dir, raft_uuid)
    log_file = "%s/%s/niovablockctl_%s_log.txt" % (base_dir, raft_uuid, nisd_uuid)

    # Initialize the logger
    logger = initialize_logger(log_file)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_file, "a+")

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    #format and run the niova-block-ctl
    # TODO check how the bin can be passed
    bin_path = os.path.join(binary_dir, NIOVA_BIN_DIR, "niova-block-ctl")

    # nisd_dict = { nisd_uuid : 0 }

    # nisd_dict[nisd_uuid] = 0

    ## TODO check if we need to write info to the lookout

    logger.debug("nisd-uuid: %s", nisd_uuid)

    # Run command equivalent to:
    # src/niova-block-ctl -d <DEV_PATH> -i -f -u $NISD_UUID
    process_popen = subprocess.Popen(
        [   
            bin_path,
            "-d", input_value["nisd_device_path"],
            "-i",
            "-f",
            "-u", nisd_uuid
        ],
        stdout=fp,
        stderr=fp,
        cwd=base_path
    )

    logger.info(
        "niova-block-ctl args: %s -d %s -i -f -u %s",
        bin_path,
        input_value["nisd_device_path"],
        nisd_uuid
    )

    #Check if niova-block-ctl process exited with error
    if process_popen.poll() is None:
        logger.info("niova-block-ctl process started successfully")
    else:
        logger.error("niova-block-ctl process failed to start")
        raise subprocess.SubprocessError(process_popen.returncode)

    # Sync the log file so all the logs from niova-block-ctl gets written to log file.
    os.fsync(fp)

    return nisd_uuid

def start_niova_block_ctl_process(cluster_params, nisd_uuid, input_values):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']

    genericcmdobj = GenericCmds()
    nisd_uuid = genericcmdobj.generate_uuid()

    # Prepare path for log file.
    log_file = "%s/%s/niovablockctl_%s_log.txt" % (base_dir, raft_uuid, nisd_uuid)

    # Initialize the logger
    logger = initialize_logger(log_file)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_file, "a+")

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    #format and run the niova-block-ctl
    bin_path = os.path.join(binary_dir, NIOVA_BIN_DIR, "niova-block-ctl")

    nisd_dict = { nisd_uuid : 0 }

    nisd_dict[nisd_uuid] = 0

     #writing the information of lookout uuids dict into raft_uuid.json
    recipe_conf = load_recipe_op_config(cluster_params)

    if not "lookout_uuid_dict" in recipe_conf:
        recipe_conf['lookout_uuid_dict'] = {}

    if input_values['lookout_uuid'] != "":
        if not "nisd_uuid_dict" in recipe_conf['lookout_uuid_dict'][input_values['lookout_uuid']]:
            recipe_conf['lookout_uuid_dict'][input_values['lookout_uuid']]['nisd_uuid_dict'] = {}

        recipe_conf['lookout_uuid_dict'][input_values['lookout_uuid']]['nisd_uuid_dict'].update(nisd_dict)
    else:
        if not "nisd_uuid_dict" in recipe_conf:
            recipe_conf['nisd_uuid_dict'] = {}
        recipe_conf['nisd_uuid_dict'].update(nisd_dict)

    genericcmdobj.recipe_json_dump(recipe_conf)

    nisdPath = prepare_nisd_device_path(nisd_uuid)

    logger.debug("nisd-uuid: %s", nisd_uuid)

    # Start niova-block-ctl process
    test_device_path = create_nisd_device_and_uuid(nisd_uuid, input_values['nisd_dev_size'])

    process_popen = subprocess.Popen([bin_path, '-n', input_values['alt_name'], '-d', nisdPath, '-i', '-u', nisd_uuid,
                                       '-z', input_values['nisd_dev_size']], stdout = fp, stderr = fp)

    logger.info("niova-block-ctl args: %s", process_popen.args)
    #Check if niova-block-ctl process exited with error
    if process_popen.poll() is None:
        logger.info("niova-block-ctl process started successfully")
    else:
        logger.error("niova-block-ctl process failed to start")
        raise subprocess.SubprocessError(process_popen.returncode)

    # Sync the log file so all the logs from niova-block-ctl gets written to log file.
    os.fsync(fp)

    return nisd_uuid

def start_nisd_process(cluster_params, input_values, nisdPath):
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    base_dir = cluster_params['base_dir']
    app_name = cluster_params['app_type']
    raft_uuid = cluster_params['raft_uuid']

    # Prepare seperate path for nisd log file.
    log_path = "%s/%s/nisd_%s_log.txt" % (base_dir, raft_uuid, input_values['nisd_uuid'])

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_path, "a+")

    genericcmdobj = GenericCmds()
    #writing the information of lookout uuids dict into raft_uuid.json
    recipe_conf = load_recipe_op_config(cluster_params)

    nisd_uuid = input_values['nisd_uuid']
    lookout_uuid = input_values['lookout_uuid']
    uport = input_values['uport']

    if input_values['lookout_uuid'] != "":
        if nisd_uuid in recipe_conf['lookout_uuid_dict'][input_values['lookout_uuid']]['nisd_uuid_dict']:
            recipe_conf['lookout_uuid_dict'][input_values['lookout_uuid']]['nisd_uuid_dict'].update({ nisd_uuid : uport })
    else:
        if input_values['nisd_uuid'] in recipe_conf['nisd_uuid_dict'].keys():
            recipe_conf['nisd_uuid_dict'][nisd_uuid] = uport

    fp.write("starting nisd process\n")
    fp.write("nisd-uuid: "+nisd_uuid+"\n")

    #start nisd process
    bin_path = os.path.join(binary_dir, NIOVA_BIN_DIR, "nisd")

    short_sock_dir = "/tmp/.niova"
    os.makedirs(short_sock_dir, exist_ok=True)

    os.environ["NIOVA_BLOCK_SOCK_PATH"] = f"{short_sock_dir}/{nisd_uuid}"
    os.environ["NIOVA_BLOCK_TCP_PEER_PORT"] = str(uport)

    process_popen = subprocess.Popen([bin_path, '-u', nisd_uuid, '-d', nisdPath],
                                      stdout = fp, stderr = fp)

    fp.write("nisd args: "+str(process_popen.args)+"\n")
    #Check if nisd process exited with error
    if process_popen.poll() is None:
        fp.write("NISD process started successfully")
    else:
        fp.write("NISD process failed to start")
        raise subprocess.SubprocessError(process_popen.returncode)

    # writing the information of lookout and nisd into raft_uuid.json file
    pid = process_popen.pid
    ps = psutil.Process(pid)

    if not "raft_process" in recipe_conf:
        recipe_conf['raft_process'] = {}

    recipe_conf['raft_process'][nisd_uuid] = {}

    recipe_conf['raft_process'][nisd_uuid]['process_raft_uuid'] = nisd_uuid
    recipe_conf['raft_process'][nisd_uuid]['img_file_name'] = nisdPath
    recipe_conf['raft_process'][nisd_uuid]['process_pid'] = pid
    recipe_conf['raft_process'][nisd_uuid]['process_uuid'] = nisd_uuid
    recipe_conf['raft_process'][nisd_uuid]['process_type'] = "nisd"
    recipe_conf['raft_process'][nisd_uuid]['process_app_type'] = app_name
    recipe_conf['raft_process'][nisd_uuid]['process_status'] = ps.status()

    genericcmdobj.recipe_json_dump(recipe_conf)

    # Sync the log file so all the logs from nisd gets written to log file.
    os.fsync(fp)

    return process_popen

def prepare_nisd_device_path(nisd_uuid):
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    nisdPath = "%s/bin/%s.device" % (binary_dir, nisd_uuid)
    return nisdPath

def create_nisd_device_and_uuid(nisd_uuid, nisd_dev_size):
    nisdpath_device = prepare_nisd_device_path(nisd_uuid)

    # truncate the device to a specified size
    file = open(nisdpath_device, 'w')
    file.truncate(int(nisd_dev_size))
    file.close()

    return nisdpath_device

def controlplane_environment_variables(cluster_params,lookout_uuid):
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

def start_niova_block_test_with_inputFile(cluster_params, input_values):
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']

    f = open("niova_block_test_inputs.txt")
    next(f)

    input_dict = {}
    i = 0
    j = 0

    nisd_uuid_to_write = input_values['nisd_uuid_to_write']
    if not 'niova-block-test-input' in input_dict:
         input_dict['niova-block-test-input'] = {}

    input_dict['niova-block-test-input'][nisd_uuid_to_write[5:]] = {}
    input_dict['niova-block-test-input'][nisd_uuid_to_write[5:]]['write-input'] = {}
    input_dict['niova-block-test-input'][nisd_uuid_to_write[5:]]['read-input'] = {}

    for line in f:
      # parse input, assign values to variables
        currentline = line.split(",")

        operation_type = currentline[0]
        read_operation_ratio_percentage = currentline[1]
        random_seed =  currentline[2]
        request_size_in_bytes =  currentline[3]
        queue_depth =  currentline[4]
        num_ops =  currentline[5]
        integrity_check =  currentline[6]
        sequential_writes =  currentline[7]
        blocking_process = currentline[8]
        sleep = int(currentline[9])

        vdev = input_values['vdev']
        client_uuid = input_values['client_uuid']

        info = {'vdev-uuid': vdev, 'read-operation-ratio-percentage' : read_operation_ratio_percentage, 'num-ops': num_ops, 'req-size-bytes': request_size_in_bytes}
        info['vdev-uuid'] = vdev
        info['read-operation-ratio-percentage'] = read_operation_ratio_percentage
        info['num-ops'] = num_ops
        info['req-size-bytes'] = request_size_in_bytes

        if operation_type == "write":
            i += 1
            input_cnt = "input" + str(i)
            input_dict['niova-block-test-input'][nisd_uuid_to_write[5:]]['write-input'][input_cnt] = {}
            input_dict['niova-block-test-input'][nisd_uuid_to_write[5:]]['write-input'][input_cnt] = info
        else:
            j += 1
            input_cnt = "input" + str(j)
            input_dict['niova-block-test-input'][nisd_uuid_to_write[5:]]['read-input'][input_cnt] = {}
            input_dict['niova-block-test-input'][nisd_uuid_to_write[5:]]['read-input'][input_cnt] = info

        if operation_type == 'write':
            # prepare path for log file.
            log_path = "%s/%s/niova-block-test_%s_%s.log" % (base_dir, raft_uuid, operation_type, nisd_uuid_to_write[5:])
        else:
            # Prepare path for log file.
            log_path = "%s/%s/niova-block-test_%s_%s.log" % (base_dir, raft_uuid, operation_type, nisd_uuid_to_write[5:])

        # Initialize the logger
        logger = initialize_logger(log_path)

        # Open the log file to pass the fp to subprocess.Popen
        fp = open(log_path, "a+")

        #start niova block test process
        bin_path = os.path.join(binary_dir, NIOVA_BIN_DIR, "niova-block-test")

        logger.debug("Do write/read operation on nisd by starting niova-block-test")
        logger.debug("nisd-uuid: %s", nisd_uuid_to_write[5:])
        logger.debug("vdev-uuid: %s", vdev)
        logger.debug("client-uuid: %s", client_uuid)

        if sequential_writes == True and integrity_check == False and blocking_process == False:
            ps = subprocess.Popen([bin_path, '-d', '-c', nisd_uuid_to_write, '-v', vdev, '-r', read_operation_ratio_percentage,
                                       '-u', client_uuid, '-Z', request_size_in_bytes,
                                       '-q', queue_depth, '-N', num_ops, '-I', '-Q'], stdout=fp, stderr=fp)

        elif integrity_check == True and sequential_writes == False and blocking_process == False:
            ps = subprocess.Popen([bin_path, '-d', '-c', nisd_uuid_to_write, '-v', vdev, '-r', read_operation_ratio_percentage,
                                       '-a', random_seed, '-u', client_uuid, '-Z', request_size_in_bytes,
                                       '-q', queue_depth, '-N', num_ops, '-I'], stdout=fp, stderr=fp)

        elif blocking_process == True and sequential_writes == False and integrity_check == False:
            ps = subprocess.Popen([bin_path, '-d', '-c', nisd_uuid_to_write, '-v', vdev, '-r', read_operation_ratio_percentage,
                                       '-u', client_uuid, '-Z', request_size_in_bytes,
                                       '-q', queue_depth, '-N', num_ops, '-I', '-Q'], stdout=fp, stderr=fp)

        else:
            ps = subprocess.Popen([bin_path, '-d', '-c', nisd_uuid_to_write, '-v', vdev, '-r', read_operation_ratio_percentage,
                                       '-a', random_seed, '-u', client_uuid, '-Z', request_size_in_bytes,
                                       '-q', queue_depth, '-N', num_ops], stdout=fp, stderr=fp)

        out, err = ps.communicate()
        logger.info("niova-block-test: %s", ps.args)
        logger.info("return code: %d", ps.returncode)
        info['returncode'] = ps.returncode
        # Sync the log file so all the logs from niova-block-test gets written to log file.
        os.fsync(fp)
        time.sleep(sleep)

    f.close()
    return input_dict

def start_niova_block_test(cluster_params, input_values):
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']

    raft_dir = os.path.join(base_dir, raft_uuid)

    # Resolve gossipNodes file path
    gossip_nodes_path = os.path.join(raft_dir, "gossipNodes.json")
    if not os.path.exists(gossip_nodes_path):
        gossip_nodes_path = os.path.join(raft_dir, "gossipNodes")

    # Authentication environment variables
    env = os.environ.copy()
    os.environ["NIOVA_GOSSIP_KEY"] = raft_uuid
    os.environ["NIOVA_GOSSIP_PATH"] = gossip_nodes_path
    os.environ["NIOVA_LOG_LEVEL"] = "5"

    enable_authentication = input_values["enable_auth"]
    
    if enable_authentication == 1:
        os.environ["NIOVA_NISD_DO_TOKEN_VALIDATION"] = "1"
        os.environ["NIOVA_NISD_SECRET"] = "NISD-secret"

    #get input parameters
    cp_mode = input_values['cp_mode']
    nisd_uuid_to_write = input_values['nisd_uuid_to_write']
    vdev = input_values['vdev']
    client_uuid = input_values['client_uuid']
    read_operation_ratio_percentage = input_values['rd_op_ratio']
    random_seed = input_values['random_seed_pt']
    request_size_in_bytes = input_values['request_size_in_bytes']
    queue_depth = input_values['queue_depth']
    num_ops = input_values['num_ops']
    integrity_check = input_values['integrity_check']
    sequential_writes = input_values['sequential_writes']
    blocking_process = input_values['blocking_process']

    if read_operation_ratio_percentage == '0':
        # prepare path for log file.
        log_path = "%s/%s/niova-block-test_write_%s.log" % (base_dir, raft_uuid, vdev)
    else:
        # Prepare path for log file.
        log_path = "%s/%s/niova-block-test_read_%s.log" % (base_dir, raft_uuid, vdev)

    # Initialize the logger
    logger = initialize_logger(log_path)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(log_path, "a+")

    #start niova block test process
    bin_path = os.path.join(binary_dir, NIOVA_BIN_DIR, "niova-block-test")

    logger.debug("Do write/read operation on nisd by starting niova-block-test.")
    logger.debug("nisd-uuid: %s", nisd_uuid_to_write)
    logger.debug("vdev-uuid: %s", vdev)
    logger.debug("client-uuid: %s", client_uuid)
    file_size_in_bytes = "8589934592"
    
    if sequential_writes == True and integrity_check == False and blocking_process == False:
        ps = subprocess.run((bin_path, '-d', '-c', nisd_uuid_to_write, '-v', vdev, '-r', read_operation_ratio_percentage,
                                   '-u', client_uuid, '-Z', request_size_in_bytes,
                                   '-q', queue_depth, '-N', num_ops, '-I', '-Q', '-z', file_size_in_bytes), stdout=fp, stderr=fp)

    elif integrity_check == True and sequential_writes == False and blocking_process == False:
        ps = subprocess.run((bin_path, '-d', '-c', nisd_uuid_to_write, '-v', vdev, '-r', read_operation_ratio_percentage,
                                   '-a', random_seed, '-u', client_uuid, '-Z', request_size_in_bytes,
                                   '-q', queue_depth, '-N', num_ops, '-I', '-z', file_size_in_bytes), stdout=fp, stderr=fp)

    elif blocking_process == True and sequential_writes == False and integrity_check == False:
        proc = subprocess.Popen([bin_path, '-d', '-c', nisd_uuid_to_write, '-v', vdev, '-r', read_operation_ratio_percentage,
                                   '-u', client_uuid, '-Z', request_size_in_bytes,
                                   '-q', queue_depth, '-N', num_ops, '-I', '-Q', '-z', file_size_in_bytes], stdout=fp, stderr=fp)

        poll = proc.poll()  # returns the exit code or None if the process is still running
        logger.info("niova-block-test args: %s", proc.args)
        logger.info("return code: %s", proc.returncode)
        return proc.returncode

    elif blocking_process == False and sequential_writes == False and integrity_check == False:
        if cp_mode == 1:
            os.environ["NIOVA_BLOCK_CP_AUTH_USERNAME"] = input_values['auth_username']
            os.environ["NIOVA_BLOCK_CP_AUTH_SECRET"] = input_values['auth_secret']
            logger.debug("Starting niova-block-test in cp mode.....")
            ps = subprocess.run((bin_path, '-c', 'cp', '-v', vdev, '-u', client_uuid, '-r', read_operation_ratio_percentage,
                         '-Z', request_size_in_bytes, '-N', num_ops, '-a', random_seed), stdout=fp, stderr=fp)

        else: 
            ps = subprocess.run((bin_path, '-c', nisd_uuid_to_write, '-v', vdev, '-u', client_uuid, '-r', read_operation_ratio_percentage,
                         '-Z', request_size_in_bytes, '-N', num_ops, '-a', random_seed), stdout=fp, stderr=fp)
            
    logger.info("niova-block-test args: %s", ps.args)
    logger.info("return code: %d", ps.returncode)
    # Sync the log file so all the logs from niova-block-test gets written to log file.
    os.fsync(fp)

    return ps.returncode

def load_recipe_op_config(cluster_params):
    recipe_conf = {}
    raft_json_fpath = "%s/%s/%s.json" % (cluster_params['base_dir'],
                                         cluster_params['raft_uuid'],
                                         cluster_params['raft_uuid'])
    if os.path.exists(raft_json_fpath):
        with open(raft_json_fpath, "r+", encoding="utf-8") as json_file:
            recipe_conf = json.load(json_file)

    return recipe_conf

class LookupModule(LookupBase):
    def run(self,terms,**kwargs):
        #Get lookup parameter values
        process_type = terms[0]
        input_values = terms[1]
        nisd_uuid = ""

        cluster_params = kwargs['variables']['ClusterParams']

        if process_type == "run-niova-block-ctl":
               nisd_uuid = run_niova_block_ctl(cluster_params, input_values)

               return [nisd_uuid]

        elif process_type == "load_module":

            load_kernel_module(input_values)
        
        elif process_type == "run_ublk_device":

            nisd_uuid = terms[1]
            return [run_niova_ublk(cluster_params, nisd_uuid)]

        elif process_type == "run_nisd":
            return [run_nisd_command(cluster_params, input_values)]

        elif process_type == "niova-block-ctl":

               controlplane_environment_variables(cluster_params, input_values['lookout_uuid'])
               niova_block_ctl_process = start_niova_block_ctl_process(cluster_params, nisd_uuid, input_values)

               return [niova_block_ctl_process]

        elif process_type == "nisd":

               controlplane_environment_variables(cluster_params, input_values['lookout_uuid'])
               #start nisd process
               nisdPath = prepare_nisd_device_path(input_values['nisd_uuid'])
               nisd_process = start_nisd_process(cluster_params, input_values, nisdPath)

               return [nisd_process]

        elif process_type == "niova-block-test":

               NiovaBlocktest_input_file = cluster_params['niovaBlockTest_input_file_path']

               if cluster_params['niovaBlockTest_input_file_path'] == True:
                    # Take niova-block-test input parameters from input file and start process
                    niova_block_test_process = start_niova_block_test_with_inputFile(cluster_params, input_values)
               else:
                    # Start niova-block-test
                    niova_block_test_process = start_niova_block_test(cluster_params, input_values)

               return [niova_block_test_process]