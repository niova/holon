from ansible.plugins.lookup import LookupBase
import json
import os, random, psutil
from datetime import datetime
import subprocess ,re
import uuid, random
import shutil
import glob
from genericcmd import *
from func_timeout import func_timeout, FunctionTimedOut
import time as time_global
import pwd
import grp
import psutil
import signal
import logging
from multiprocessing import Pool
from lookup_plugin.helper import *

Marker_vdev = 0
Marker_chunk = 1
Marker_seq = 2
Marker_type = 4

def load_parameters_from_json(filename):
    # Load parameters from a JSON file
    with open(filename, 'r') as json_file:
        params = json.load(json_file)
    return params

def create_directory(path):
    """
    Creates a directory if it doesn't exist.

    :param path: str, path of the directory to create
    """
    try:
        os.makedirs(path, exist_ok=True)
        print(f"Directory created successfully at: {path}")
    except OSError as e:
        print(f"Error creating directory at {path}: {e}")

def create_bucket(cluster_params, bucket):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = '%s/s3Operation' % binary_dir
    s3_config = '%s/s3.config.example' % binary_dir

    command = [
        bin_path,
        "-s3config", s3_config,
        "-bucketName", bucket,
        "-operation", "create_bucket"
    ]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print("Output:", result.stdout)
        print("Errors:", result.stderr)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print("An error occurred:", e.stderr)
        raise e


def run_fio_test(directory_path):
    fio_command_base = [
        "sudo",
        "/usr/bin/fio",
        f"--filename={directory_path}/gc0.tf",
        f"--filename={directory_path}/gc1.tf",
        "--direct=1",
        "--ioengine=io_uring",
        "--iodepth=128",
        "--numjobs=1",
        "--group_reporting",
        "--name=iops-test-job",
        "--size=100M",  # Generate 100MB of data
        "--bs=4k",
        "--fixedbufs",
        "--buffer_compress_percentage=50",
        "--rw=randwrite"
    ]

    for i in range(10):  # Repeat 10 times
        print(f"Running fio test iteration {i+1}...")
        try:
            result = subprocess.run(fio_command_base, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running fio test on iteration {i+1}:", e)
            raise e
        
        time.sleep(30)

def start_s3_data_validator(cluster_params, device_path, ublk_uuid, nisd_uuid):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = '%s/s3DataValidator' % binary_dir
    log_dir = "%s/%s/s3DV" % (base_dir, raft_uuid)
    s3_config = '%s/s3.config.example' % binary_dir
    nisd_cmdintf_path = "/tmp/.niova/%s" % nisd_uuid
    
    # Ensure log directory exists
    create_directory(log_dir)
    
    # Build command to run
    cmd = ["sudo", bin_path, '-v', ublk_uuid, '-c', s3_config, '-p', log_dir, '-b', 'paroscale-test', '-d', device_path, '-nisdP', nisd_cmdintf_path]
    
    print(f"s3 dv cmd {cmd}")
    # Run the command and capture the exit code
    try:
        result = subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise e 


def create_gc_partition(cluster_params):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    log_dir = "%s/%s/" % (base_dir, raft_uuid)
    disk_ipath = os.path.join(log_dir, 'GC.img')
    mount_pt = os.path.join(log_dir, 'gc')
    dir_name = os.path.join(mount_pt, 'gc_download')

    uid = os.geteuid()

    # Get the current user's info
    user_info = pwd.getpwuid(uid)
    username = user_info.pw_name

    # Get the current GID
    gid = user_info.pw_gid

    # Get the group information
    group_info = grp.getgrgid(gid)
    groupname = group_info.gr_name

    try:
        result = subprocess.run(f"dd if=/dev/zero of={disk_ipath} bs=64M count=27", check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

    try:
        result = subprocess.run(["sudo", "losetup", "-fP", disk_ipath], check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

    try:
        result = subprocess.run(["sudo", "mkfs.btrfs", disk_ipath], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

    os.mkdir(mount_pt, 0o777)

    try:
        result = subprocess.run(["sudo", "mount", disk_ipath, mount_pt], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

    try:
        result = subprocess.run(["sudo", "mkdir", dir_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

    try:
        result = subprocess.run(["sudo", "chown", username, dir_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

    try:
        result = subprocess.run(["sudo", "chown", groupname, dir_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")


    try:
        result = subprocess.run(["sudo", "chmod", "777", dir_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def get_unmounted_ublk_device(cluster_params):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    output_file = "%s/%s/%s" % (base_dir, raft_uuid, "lsblk_output.txt")
    timeout = 3 * 60  # Total timeout in seconds (3 minutes)
    interval = 5  # Interval in seconds between retries

    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            result = subprocess.run(
                ["lsblk", "-o", "NAME,MOUNTPOINT", "-n"],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Store output to a file
            with open(output_file, 'w') as file:
                file.write(result.stdout)

            # Parse output and find the first unmounted ublk device
            for line in result.stdout.splitlines():
                parts = line.split()

                if not parts:
                    continue
                    
                name = parts[0]
                mountpoint = parts[1] if len(parts) > 1 else ""

                if name.startswith("ublkb") and mountpoint == "":
                    return f"/dev/{name}" 

            print("No unmounted ublk device found. Retrying in 30 seconds...")

        except subprocess.CalledProcessError as e:
            print("Error retrieving unmounted ublk devices:", e)

        time.sleep(interval)  # Wait for 30 seconds before retrying

    print("No unmounted ublk device found after 3 minutes.")
    return None  # No unmounted ublk device found after retries


def setup_btrfs(cluster_params, mount_point):
    """
    Automates the setup of a Btrfs filesystem:
    1. Formats the specified device with Btrfs.
    2. Creates the mount point directory if it doesn't exist.
    3. Mounts the device to the specified mount point.

    Parameters:
        device (str): The device name (e.g., /dev/ublkb0).
        mount_point (str): The directory to mount the filesystem (e.g., /ci_btrfs).

    Raises:
        RuntimeError: If any command fails during the setup.
    """
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    mount_path = "%s/%s/%s" % (base_dir, raft_uuid, mount_point)

    device = get_unmounted_ublk_device(cluster_params)
    if device == None: 
        raise RuntimeError(f"no ublk device available")
    #TODO Add check to see if the value is none or not
    try:
        # Step 1: Format the device with Btrfs
        print(f"Formatting {device} with Btrfs...")
        subprocess.run(["sudo","mkfs.btrfs", device], check=True)
        print(f"Formatted {device} successfully.")

        # Step 2: Create the mount point directory if it doesn't exist
        if not os.path.exists(mount_path):
            print(f"Creating mount point directory {mount_path}...")
            os.makedirs(mount_path)
            subprocess.run(["sudo", "chmod", "777", mount_path], check=True)
            print(f"Directory {mount_path} created.")

        # Step 3: Mount the device to the mount point
        print(f"Mounting {device} to {mount_path}...")
        subprocess.run(["sudo", "mount", device, mount_path], check=True)
        print(f"Mounted {device} to {mount_path} successfully.")

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"An error occurred while setting up Btrfs: {e}")

    recipe_conf = load_recipe_op_config(cluster_params)
    
    if not "btrfs_process" in recipe_conf:
        recipe_conf['btrfs_process'] = {}

    recipe_conf['btrfs_process']['mount_point'] = mount_path

    genericcmdobj = GenericCmds()
    genericcmdobj.recipe_json_dump(recipe_conf)
    return [mount_path, device]


def create_file(cluster_params, filename, bs, count):
    """
    Creates a file with specified parameters using the 'dd' command.

    :param cluster_params: dict, dictionary containing 'base_dir' and 'raft_uuid'
    :param filename: str, name of the file to create within the log directory
    :param bs: str, block size for 'dd' command
    :param count: int, count of blocks for 'dd' command
    """
        
    # Get base directory and raft UUID from cluster parameters
    base_dir = cluster_params.get('base_dir', '').rstrip('/')  # Remove trailing slash from base_dir if present
    raft_uuid = cluster_params.get('raft_uuid', '')

    # Remove leading slash from filename, ensuring it won't override the path
    filename = filename.lstrip('/')

    # Construct log directory path using os.path.join for platform independence
    log_dir = os.path.join(base_dir, raft_uuid)

    # Construct the full path by joining log_dir with the filename
    full_path = os.path.join(log_dir, filename)

    # Normalize the path to remove redundant slashes, if any
    full_path = os.path.normpath(full_path)

    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        print("nisd file path: ", full_path)

        dd_command = f"sudo dd if=/dev/zero of={full_path} bs={bs} count={count}"

        print(f"Running command: {dd_command}")

        result = subprocess.run(
            dd_command,
            check=True, shell=True
        )
        print(f"File created successfully at: {full_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    
    return full_path

def delete_file(cluster_params):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    log_dir = "%s/%s/" % (base_dir, raft_uuid)
    filename = os.path.join(log_dir, 'gc/gc_download/file.img')

    if os.path.exists(filename):
        os.remove(filename)
        print(f"The file {filename} has been deleted successfully.")
    else:
        print(f"The file {filename} does not exist.")

def delete_partition(cluster_params):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    log_dir = "%s/%s/" % (base_dir, raft_uuid)
    disk_ipath = os.path.join(log_dir, 'GC.img')
    mount_pt = os.path.join(log_dir, 'gc')

    try:
        result = subprocess.run(["sudo", "umount", "-l", mount_pt], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

    output = subprocess.check_output(["losetup", "-a"], text=True)
    for line in output.splitlines():
        if disk_ipath in line:
            loop_dev = line.split(':')[0]
            try:
                result = subprocess.run(["sudo", "losetup", "-d", loop_dev], check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error: {e}")
            break

def pause_gcProcess(pid):
    try:
        pid = int(pid)  # Convert string pid to integer
    except ValueError:
        logging.error(f"pause_gc_process: Invalid PID format: {pid}")
        return -1

    try:
        process_obj = psutil.Process(pid)
    except psutil.NoSuchProcess:
        logging.error(f"pause_process: Process with PID {pid} not found")
        return -1

    logging.info(f"Pausing process {pid} by sending SIGSTOP")
    try:
        process_obj.send_signal(signal.SIGSTOP)
        return 0
    except PermissionError as e:
        logging.error(f"resume_process: Insufficient permissions to send signal: {e}")
        return -1
    except psutil.NoSuchProcess:  # Handle cases where process terminates during resume
        logging.error(f"resume_process: Process {pid} terminated unexpectedly")
        return -1
    except Exception as e:  # Catch unexpected errors
        logging.error(f"resume_process: Unexpected error: {e}")
        return -1

def resume_gcProcess(pid):
    try:
        pid = int(pid)  # Convert string pid to integer
    except ValueError:
        logging.error(f"pause_gc_process: Invalid PID format: {pid}")
        return -1

    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        logging.error(f"pause_process: Process with PID {pid} not found")
        return -1

    logging.info(f"Resuming process {pid} by sending SIGCONT")

    try:
        process.send_signal(signal.SIGCONT)
        return 0
    except PermissionError as e:
        logging.error(f"resume_process: Insufficient permissions to send signal: {e}")
        return -1
    except psutil.NoSuchProcess:  # Handle cases where process terminates during resume
        logging.error(f"resume_process: Process {pid} terminated unexpectedly")
        return -1
    except Exception as e:  # Catch unexpected errors
        logging.error(f"resume_process: Unexpected error: {e}")
        return -1

def run_dummyData_cmd(command):
    print("command", command)
    try:
        result = subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def generate_dbi_dbo_concurrently(cluster_params, dirName, no_of_chunks):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = '%s/dummyData' % binary_dir
    path = "%s/%s/%s/" % (base_dir, raft_uuid, dirName)
    if not os.path.exists(path):
        # Create the directory path
        try:
            os.makedirs(path, mode=0o777)
        except Exception as e:
            print(f"An error occurred while creating '{path}': {e}")

    s3configPath = '%s/s3.config.example' % binary_dir
    s3LogFile = "%s/%s/s3Upload" % (base_dir, raft_uuid)

    commands = []
    for chunk in range(1, no_of_chunks + 1):
        command = [bin_path, "-c", str(chunk), "-mp", "1024", "-mv", "2097152", "-p", path, "-pa", "6000", 
                   "-pp", "0", "-ps", "2048", "-seed", "1", "-ss", "0", "-t", "1", "-va", "2097152", "-l", "2",
                   "-vp", "100000", "-vdev", "643eef86-e42b-11ee-8678-22abb648e432", "-bs", "4", "-bsm", "32", 
                   "-vs", "0", "-s3config", s3configPath, "-s3log", s3LogFile, "-dbic", "0"]
        commands.append(command)

    with Pool(processes = no_of_chunks) as pool:
        results = pool.map(run_dummyData_cmd, commands)

def multiple_iteration_params(cluster_params, dirName, input_values):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']

    path = "%s/%s/%s/" % (base_dir, raft_uuid, dirName)

    # Create the new directory
    if not os.path.exists(path):
        # Create the directory path
        try:
            os.makedirs(path, mode=0o777)
        except Exception as e:
            print(f"An error occurred while creating '{path}': {e}")

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    # Prepare path for log file.
    dbiLogFile = "%s/%s/dbiLog.log" % (base_dir, raft_uuid)
    
    # Open the log file to pass the fp to subprocess.Popen
    fp = open(dbiLogFile, "a+")

    #Get dummyDBI example
    bin_path = '%s/dummyData' % binary_dir
    dbicount = "0"
    jsonPath = get_dir_path(cluster_params, dirName)

    if jsonPath != None:
        entries = os.listdir(jsonPath)
        chunk_no = input_values["chunk"]
        if chunk_no not in entries:
            jsonPath = None
        else:
            newPath = jsonPath + "/" + input_values["chunk"] + "/DV"
            json_data = load_json_contents(newPath + "/dummy_generator.json")
            input_values["vdev"] = str(json_data['Vdev'])
            input_values["seqStart"] = str(json_data['SeqEnd'] + 1)
            dbicount = str(json_data['TMinDbiFileForForceGC'])

    # Initialize the command list with common arguments
    cmd = [
        bin_path, "-c", input_values['chunk'], "-mp", input_values['maxPunches'], "-mv", input_values['maxVblks'], "-p", path,
        "-pa", input_values['punchAmount'], "-pp", input_values['punchesPer'], "-ps", input_values['maxPunchSize'], "-seed", input_values['seed'],
        "-ss", input_values['seqStart'], "-t", input_values['genType'], '-va', input_values['vbAmount'], '-vp', input_values['vblkPer'], '-l', '2',
        "-vdev", input_values["vdev"], "-bs", input_values['blockSize'], "-bsm", input_values['blockSizeMax'], "-vs", input_values['startVblk'],
        "-dbic", dbicount
    ]

    # Add the S3-specific options if s3Support is "true"
    if s3Support == "true":
        s3configPath = '%s/s3.config.example' % binary_dir
        s3LogFile = "%s/%s/s3Upload" % (base_dir, raft_uuid)
        cmd.extend(['-s3config', s3configPath, '-s3log', s3LogFile])

    if input_values["punchwholechunk"] == "=true":
        cmd.extend(["-pc", input_values["punchwholechunk"]])
    if input_values["strideWidth"] != "":
        cmd.extend(["-sw", input_values["strideWidth"]])
    if input_values["overlapSeq"] != "" and input_values["numOfSet"] != "":
        cmd.extend(["-se", input_values["overlapSeq"], "-ts", input_values["numOfSet"]])

    print("cmd: ", cmd)
    # Launch the subprocess with the constructed command
    process = subprocess.Popen(cmd, stdout=fp, stderr=fp)
    # Wait for the process to finish and get the exit code
    exit_code = process.wait()
    # Close the log file
    fp.close()
    # Check if the process finished successfully (exit code 0)
    if exit_code == 0:
        print("Process completed successfully.")
    else:
        error_message = f"Process failed with exit code {exit_code}."
        raise RuntimeError(error_message)

    return process

def delete_contents_of_paths(paths):
    """
    Deletes all contents of the specified directories if they exist.

    Parameters:
    paths (list): A list of directory paths to delete contents from.
    """
    for path in paths:
        # Check if the directory exists
        if os.path.exists(path):
            # List all contents
            for filename in os.listdir(path):
                file_path = os.path.join(path, filename)
                try:
                    # Check if it is a file or directory and delete accordingly
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Remove file or symbolic link
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Remove directory
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')

def prepare_command_from_parameters(cluster_params, jsonParams, dirName, operation, params_type):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    dbiPath = "%s/%s/%s/" % (base_dir, raft_uuid, dirName)
    #Create the new directory
    if not os.path.exists(dbiPath):
        # Create the directory path
        try:
            os.makedirs(dbiPath, mode=0o777)
        except Exception as e:
            print(f"An error occurred while creating '{path}': {e}")

    processes = []

    for params in jsonParams:
       path = dbiPath + params["seed"]
       #Create the new directory
       if not os.path.exists(path):
          # Create the directory path
          try:
             os.makedirs(path, mode=0o777)
          except Exception as e:
             print(f"An error occurred while creating '{path}': {e}")

       cmd = []
       # Prepare path for log file.
       dbiLogFile = "%s/%s/dbiLog_%s.log" % (base_dir, raft_uuid, params["seed"])
       gcLogFile = "%s/%s/gcLog_%s.log" % (base_dir, raft_uuid, params["seed"])
       data_validator_log = "%s/%s/dataValidatorResult_%s" % (base_dir, raft_uuid, params["seed"])
       s3configPath = '%s/s3.config.example' % binary_dir
       gcDownloadPath = "%s/%s/gc-downloaded-obj" % (base_dir, raft_uuid)
       dvDownloadPath = "%s/%s/dv-downloaded-obj/" % (base_dir, raft_uuid)
       if operation == "run_example":
          dbicount = "0" 
          bin_path = '%s/dummyData' % binary_dir
          jsonPath = get_dir_path(cluster_params, dirName, params["seed"])
          if jsonPath != None:
               newPath = jsonPath + "/" + params["chunk"] + "/DV"
               json_data = load_json_contents(newPath + "/dummy_generator.json")
               params["seqStart"] = str(json_data['SeqEnd'] + 1)
               params["vdev"] = str(json_data['Vdev'])
               dbicount = str(json_data['TMinDbiFileForForceGC']) 
          else:
               params["seqStart"] = "0"
               params["vdev"] = ""
               dbicount = "0"
          if s3Support == "true":
               s3UploadLogFile = "%s/%s/s3Upload" % (base_dir, raft_uuid)
               cmd.extend([bin_path, "-c", params["chunk"], "-mp", params["maxPunches"],
                   "-mv", params["maxVblks"], "-p", path, "-pa", params["punchAmount"],
                   "-pp", params["punchesPer"], "-ps", params["maxPunchSize"], "-seed", params["seed"],
                   "-ss", params["seqStart"], "-va", params["vbAmount"], "-vp", params["vblkPer"],
                   "-t", params["genType"], "-bs", params["blockSize"], "-bsm", params["blockSizeMax"],
                   "-vs", params["startVblk"], "-vdev", params["vdev"], "-s3config", s3configPath,
                   "-s3log", s3UploadLogFile, '-b=paroscale-test', "-dbic", dbicount])
          else:
               cmd.extend([bin_path, "-c", params["chunk"], "-mp", params["maxPunches"],
                   "-mv", params["maxVblks"], "-p", path, "-pa", params["punchAmount"],
                   "-pp", params["punchesPer"], "-ps", params["maxPunchSize"], "-seed", params["seed"],
                   "-ss", params["seqStart"], "-va", params["vbAmount"], "-vp", params["vblkPer"],
                   "-t", params["genType"], "-bs", params["blockSize"], "-bsm", params["blockSizeMax"],
                   "-vs", params["startVblk"], "-vdev", params["vdev"]])
          if params["strideWidth"] != "":
                cmd.extend(["-sw", params["strideWidth"]])
          if params["overlapSeq"] != "" and params["numOfSet"] != "":
                cmd.extend(["-se", params["overlapSeq"], "-ts", params["numOfSet"]])

       elif operation == "run_gc":
          bin_path = '%s/gcTester' % binary_dir
          get_path = get_dir_path(cluster_params, dirName, params["seed"])
          json_data = load_json_contents(get_path + "/" + params["chunk"] + "/DV" + "/dummy_generator.json")
          modified_path = modify_path(get_path, params["seed"])
          vdev = str(json_data['Vdev'])
          if s3Support == "true":
               s3DownloadLogFile = "%s/%s/s3Download_%s" % (base_dir, raft_uuid, params["seed"])
               cmd.extend([bin_path, '-v', vdev, '-c', params["chunk"], "-s3config", s3configPath,
                       "-s3log", s3DownloadLogFile, "-path", gcDownloadPath, '-b=paroscale-test'])
          else:
              cmd.extend([bin_path, "-i", modified_path, '-v', vdev, '-c', params["chunk"]])
          if params["debugMode"]:
              cmd.append('-d')

       elif operation == "run_data_validator":
          bin_path = '%s/dataValidator' % binary_dir
          get_path = get_dir_path(cluster_params, dirName, params["seed"])
          json_data = load_json_contents(get_path + "/" + params["chunk"] + "/DV" + "/dummy_generator.json")
          modified_path = modify_path(get_path, params["seed"])
          vdev = str(json_data['Vdev'])
          if s3Support == "true":
                cmd.extend([bin_path, '-d', dvDownloadPath, '-c', params['chunk'], '-v', vdev,
                    '-b=paroscale-test', '-s3config', s3configPath, '-l', data_validator_log, '-ll', '4'])
          else:
             cmd.extend([bin_path, '-d', modified_path, '-c', params['chunk'],
                    '-v', vdev, '-l', data_validator_log])
       fp = open(dbiLogFile if operation == "run_example" else gcLogFile, "a+")
       process = subprocess.Popen(cmd, stdout=fp, stderr=fp)
       # Wait for the process to finish and get the exit code
       exit_code = process.wait()

       # close the log file
       fp.close()

       # Check if the process finished successfully (exit code 0)
       if exit_code == 0:
          print("Process completed successfully.")
       else:
          error_message = f"Process failed with exit code {exit_code}."
          raise RuntimeError(error_message)

       # Call the function to delete contents
       delete_contents_of_paths([gcDownloadPath, dvDownloadPath])

def load_recipe_op_config(cluster_params):
    recipe_conf = {}
    raft_json_fpath = "%s/%s/%s.json" % (cluster_params['base_dir'],
                                         cluster_params['raft_uuid'],
                                         cluster_params['raft_uuid'])
    if os.path.exists(raft_json_fpath):
        with open(raft_json_fpath, "r+", encoding="utf-8") as json_file:
            recipe_conf = json.load(json_file)

    return recipe_conf

def start_minio_server(cluster_params, dirName):
    s3Support = cluster_params['s3Support']
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    # Prepare path for log file.
    s3_server = "%s/%s/s3Server.log" % (base_dir, raft_uuid)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(s3_server, "w")

    if s3Support:
        # Check if the directory exists, and if not, create it.
        if not os.path.exists(os.path.expanduser(f'/local/{dirName}')):
            os.makedirs(os.path.expanduser(f'/local/{dirName}'), mode=0o777)

        command = f"minio server /local/{dirName} --console-address ':2000' --address ':2090'"
        
        process_popen = subprocess.Popen(command, shell=True, stdout=fp, stderr=fp)

        # Check if MinIO process started successfully
        if process_popen.poll() is None:
            logging.info("MinIO server started successfully in the background.")
        else:
            logging.info("MinIO server failed to start")
            raise subprocess.SubprocessError(process_popen.returncode)

        # Wait for a short time to ensure the MinIO process has started.
        time.sleep(2)
        genericcmdobj = GenericCmds()
        recipe_conf = load_recipe_op_config(cluster_params)

        if not "s3_process" in recipe_conf:
            recipe_conf['s3_process'] = {}

        # Find the MinIO server process using psutil
        minio_process = None
        for child in psutil.Process(process_popen.pid).children(recursive=True):
            if "minio" in child.name().lower():
                minio_process = child
                break

        if minio_process:
            minio_pid = minio_process.pid
            minio_status = minio_process.status()
            logging.info(f"MinIO server PID: {minio_pid}, Status: {minio_status}")
            recipe_conf['s3_process']['process_pid'] = minio_pid
            recipe_conf['s3_process']['process_status'] = minio_status
            genericcmdobj.recipe_json_dump(recipe_conf)
        else:
            logging.info("MinIO server process not found")

    return process_popen

def get_dir_path(cluster_params, dirName, seed=None):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    baseDir = os.path.join(base_dir, raft_uuid)
    #dbi_dir = os.path.join(baseDir, dirName, seed)

    if seed is not None:
        dbi_dir = os.path.join(baseDir, dirName, seed)
    else:
        dbi_dir = os.path.join(baseDir, dirName)

    # Get a list of all entries (files and directories) under the 'dbi-dbo' directory
    entries = os.listdir(dbi_dir)

    # Filter out directories only
    directories = [entry for entry in entries if os.path.isdir(os.path.join(dbi_dir, entry))]

    if directories:
        # Sort directories based on creation time (most recent first)
        directories.sort(key=lambda d: os.path.getctime(os.path.join(dbi_dir, d)), reverse=True)
        # Return the path of the most recently created directory with a trailing slash
        most_recent_directory = directories[0]
        if len(directories) > 1:
            most_recent_directory = directories[1]
        directory_path = os.path.join(dbi_dir, most_recent_directory, '')  # Add the trailing slash here
        return directory_path
    else:
        return None

def start_pattern_generator(cluster_params, genType, dirName, input_values, removeFiles=True):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    path = "%s/%s/%s/" % (base_dir, raft_uuid, dirName)
    # Create the new directory
    if not os.path.exists(path):
        # Create the directory path
        try:
            os.makedirs(path, mode=0o777)
        except Exception as e:
            print(f"An error occurred while creating '{path}': {e}")

    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    # Prepare path for log file.
    dbiLogFile = "%s/%s/dbiLog.log" % (base_dir, raft_uuid)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(dbiLogFile, "a+")

    #Get dummyDBI example
    bin_path = '%s/dummyData' % binary_dir
    vdev = ""

    chunkNum = ""
    if input_values['chunkNumber'] == "-1":
        # Generate a random chunkNumber
        chunkNum = str(random.randint(1, 200))

    # Generate random values for the dbi pattern generation
    jsonPath = get_dir_path(cluster_params, dirName)
    if jsonPath != None:
        newPath = jsonPath + "/" + input_values['chunkNumber'] + "/DV"
        json_data = load_json_contents(newPath + "/dummy_generator.json")
        chunk = str(json_data['TotalChunkSize'])
        seqStart = str(json_data['SeqEnd'] + 1)
        vdev = str(json_data['Vdev'])
        dbicount = str(json_data['TMinDbiFileForForceGC'])
    else:
        chunk = chunkNum
        seqStart = "0"
        dbicount = "0"

    maxPunches = str(random.randint(1, 50))
    maxVblks = str(random.randint(100, 1000))
    punchAmount = str(random.randint(51, 100))
    punchesPer = "0"
    maxPuncheSize = str(random.randint(1, 1024))
    seed = str(random.randint(1, 100))
    vbAmount =  str(random.randint(1000, 10000))
    vblkPer = str(random.randint(1, 20))
    blockSize = str(random.randint(1, 32))
    blockSizeMax = str(random.randint(1, 32))
    startVblk = "0"
    strideWidth = str(random.randint(1, 50))
    numOfSet = str(random.randint(1, 10))

    # Initialize the command list with common arguments
    cmd = [
        bin_path, "-c", chunk, "-mp", maxPunches, "-mv", maxVblks, "-p", path,
        "-pa", punchAmount, "-pp", punchesPer, "-ps", maxPuncheSize, "-seed", seed,
        "-ss", seqStart, "-t", genType, "-b", "paroscale-test",
        "-bs", blockSize, "-bsm", blockSizeMax, "-vs", startVblk, "-sw", strideWidth,
        "-dbic", dbicount
    ]
    # Add the -se and -ts option if overlapSeq is provided
    if 'overlapSeq' not in input_values:
        cmd.extend(['-va', str(vbAmount), '-vp', str(vblkPer)])
    else:
        cmd.extend(['-va', input_values['vbAmount'], '-vp', input_values['vblkPer'],
                    '-se', input_values['overlapSeq'], '-ts', str(numOfSet)])

    # Add the -vdev option if vdev is provided
    if vdev:
       cmd.extend(['-vdev', vdev])

    if 'dbiWithPunches' in input_values:
       cmd.extend(['-va', input_values['vbAmount'], '-vp', input_values['vblkPer'],
                        '-e', input_values['dbiWithPunches']])
    # Add the S3-specific options if s3Support is "true"
    if s3Support == "true":
         s3config = '%s/s3.config.example' % binary_dir
         s3LogFile = "%s/%s/s3Upload" % (base_dir, raft_uuid)
         cmd.extend(['-s3config', s3config, '-s3log', s3LogFile])
         if not removeFiles:
              cmd.append('-r=true')

    print("cmd: ", cmd)
    # Launch the subprocess with the constructed command
    process = subprocess.Popen(cmd, stdout=fp, stderr=fp)

    # Wait for the process to finish and get the exit code
    exit_code = process.wait()
    # Close the log file
    fp.close()

    # Check if the process finished successfully (exit code 0)
    if exit_code == 0:
        print("Process completed successfully.")
    else:
        error_message = f"Process failed with exit code {exit_code}."
        raise RuntimeError(error_message)

    return chunk

def start_gc_process(cluster_params, dirName, debugMode, chunk, crcCheck=None):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    baseDir = os.path.join(base_dir, raft_uuid)
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    # Prepare path for log file.
    gcLogFile = "%s/%s/gcLog.log" % (base_dir, raft_uuid)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(gcLogFile, "a+")

    path = get_dir_path(cluster_params, dirName)
    bin_path = '%s/gcTester' % binary_dir
    matches = re.findall(r'[\w-]{36}', path)
    vdev_uuid = matches[-1] if matches else None
    modified_path = modify_path(path)
    cmd = []
    if s3Support == "true":
         s3config = '%s/s3.config.example' % binary_dir
         # Prepare path for log file.
         s3LogFile = "%s/%s/s3Download" % (base_dir, raft_uuid)
         downloadPath = "%s/%s/gc-downloaded-obj" % (base_dir, raft_uuid)
         cmd = [bin_path, '-c', chunk, '-v', vdev_uuid, '-s3config', s3config, '-path', downloadPath, '-s3log', s3LogFile, '-b', 'paroscale-test']
    else:
        cmd = [bin_path, '-i', modified_path, '-v', vdev_uuid, '-c', chunk]

    if debugMode:
        cmd.append('-d')

    if crcCheck:
        cmd.append('-ec=true')
    process = subprocess.Popen(cmd, stdout = fp, stderr = fp)

    # Wait for the process to finish and get the exit code
    exit_code = process.wait()
    # Close the log file
    fp.close()

    return exit_code

def start_gcService_process(cluster_params, dirName, dryRun, delDBO, partition, force_gc, no_of_chunks):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    app_name = cluster_params['app_type']

    baseDir = os.path.join(base_dir, raft_uuid)
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    # Prepare path for log file.
    gcLogFile = "%s/%s/gcS3Log.log" % (base_dir, raft_uuid)

    # Open the log file to pass the fp to subprocess.Popen
    fp = open(gcLogFile, "a+")
    bin_path = '/%s/GCService' % binary_dir
    bin_path = os.path.normpath(bin_path)

    cmd = []
    s3config = '%s/s3.config.example' % binary_dir
    # Prepare path for log file.
    s3LogFile = "%s/%s/s3Download" % (base_dir, raft_uuid)
    
    if partition:
        downloadPath = "%s/%s/gc/gc_download" % (base_dir, raft_uuid)
    else:
        downloadPath = "%s/%s/gc-downloaded-obj" % (base_dir, raft_uuid)
        if not os.path.exists(downloadPath):
            # Create the directory path
            try:
                os.mkdir(downloadPath, 0o777)
            except Exception as e:
                print(f"An error occurred while creating '{downloadPath}': {e}")
    
    cmd = [bin_path, '-path', downloadPath, '-s3config', s3config, '-s3log', s3LogFile, '-t', '120',
              '-l', '4', '-p', '7500', '-b', 'paroscale-test', '-mp', str(no_of_chunks)]

    if dryRun:
        cmd.append('-dr')

    if delDBO:
        cmd.append('-dd')
    
    if force_gc:
        cmd.append('-f')
        
    process_popen = subprocess.Popen(cmd, stdout = fp, stderr = fp)

    #Check if gcService process exited with error
    if process_popen.poll() is None:
        logging.info("gcService process started successfully")
    else:
        logging.info("gcService failed to start")
        raise subprocess.SubprocessError(process_popen.returncode)

    if downloadPath != None:
        print("downloadPath exist ", downloadPath)
    else:
        print("download path doesn't exist")
    #writing the information of lookout uuids dict into raft_uuid.json
    recipe_conf = load_recipe_op_config(cluster_params)

    #writing the information of lookout uuid in gcService_process into raft_uuid.json
    pid = process_popen.pid
    ps = psutil.Process(pid)

    if not "gcService_process" in recipe_conf:
        recipe_conf['gcService_process'] = {}

    recipe_conf['gcService_process']['process_pid'] = pid
    recipe_conf['gcService_process']['process_type'] = "gcService"
    recipe_conf['gcService_process']['process_app_type'] = app_name
    recipe_conf['gcService_process']['process_status'] = ps.status()

    genericcmdobj = GenericCmds()
    genericcmdobj.recipe_json_dump(recipe_conf)

    os.fsync(fp)

def modify_path(path, seed=None):
    # Split the path into parts
    parts = path.split('/')

    # Remove the first element if it's empty due to a leading slash
    if parts[0] == '':
        parts.pop(0)

    # Remove the last element if it's empty due to a trailing slash
    if parts[-1] == '':
        parts.pop()

    # Find the index of 'dbi-dbo' in the list
    try:
        dbi_dbo_index = parts.index('dbi-dbo')
    except ValueError:
        # If 'dbi-dbo' is not found, return the original path
        return path

    # Ensure there's at least one directory after 'dbi-dbo' to check
    if dbi_dbo_index < len(parts) - 1:
        # Get the directory name after 'dbi-dbo'
        next_directory = parts[dbi_dbo_index + 1]

        # Check if the next directory is seed
        if next_directory == seed:
            # Remove the directory that comes after seed
            parts.pop(dbi_dbo_index + 2)
        else:
            # Remove the directory that comes after 'dbi-dbo'
            parts.pop(dbi_dbo_index + 1)

    # Rejoin the remaining parts
    new_path = '/' + '/'.join(parts)

    return new_path

def start_data_validate(cluster_params, dirName, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    # Prepare path for executables.
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    # Prepare path for log file.
    logFile = "%s/%s/dataValidateResult" % (base_dir, raft_uuid)
    jsonPath = get_dir_path(cluster_params, dirName)

    newPath = jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json"
    json_data = load_json_contents(newPath)
    vdev = str(json_data['Vdev'])

    path = get_dir_path(cluster_params, dirName)
    downloadPath = "%s/%s/gc-downloaded-obj" % (base_dir, raft_uuid)
    bin_path = '%s/dataValidator' % binary_dir
    if path != None:
        newPath = path + "/" + chunk + "/DV"
        json_data = load_json_contents(newPath + "/dummy_generator.json")
        vdev = str(json_data['Vdev'])

    modified_path = modify_path(path)

    if s3Support == "true":
        dvPath = "%s/%s/dv-downloaded-obj" % (base_dir, raft_uuid)
        s3config = '%s/s3.config.example' % binary_dir
        process = subprocess.Popen([bin_path, '-d', dvPath, '-c', chunk, '-v', vdev, '-s3config', s3config, '-b', 'paroscale-test', '-l', logFile, '-ll', '2'])
    else:
        process = subprocess.Popen([bin_path, '-d', modified_path, '-c', chunk, '-v', vdev, '-l', logFile, '-ll', '2'])

    # Wait for the process to finish and get the exit code
    exit_code = process.wait()

    # Check if the process finished successfully (exit code 0)
    if exit_code == 0:
        f"Process completed successfully."
    else:
        error_message = f"Process failed with exit code {exit_code}."
        raise RuntimeError(error_message)

def load_json_contents(path):
    counter = 0
    timeout = 200
    # Wait till the output json file gets created.
    while True:
        if not os.path.exists(path):
            counter += 1
            time.sleep(1)
            if counter == timeout:
                return {'outfile_status':-1}
        else:
            break

    json_data = {}
    with open(path, "r+", encoding="utf-8") as json_file:
        json_data = json.load(json_file)

    return json_data

def copy_file(source_path, dest_path):
    shutil.copy(source_path, dest_path)

def corrupt_file(cluster_params, dirName, operation, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, dirName)
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    newPath = jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json"
    json_data = load_json_contents(newPath)

    dbi_input_path = str(json_data['DbiPath'])
    vdev = str(json_data['Vdev'])
    logFile = "%s/%s/s3operation" % (base_dir, raft_uuid)
    path = get_dir_path(cluster_params, dirName)
    # Get a list of files in the source directory
    file_list = os.listdir(dbi_input_path)
    last_file = ""
    if len(file_list) > 1:
        # Sort the file list by modification time (oldest to newest)
        file_list.sort(key=lambda x: os.path.getmtime(os.path.join(dbi_input_path, x)))
        #removing directories from list
        file_list = [f for f in file_list if not os.path.isdir(os.path.join(dbi_input_path, f))]
        # Get the last file in the sorted list
        last_file = file_list[-1]
    # Construct the source file path
    source_file_path = os.path.join(dbi_input_path, last_file)
    backup_dir = os.path.join(base_dir, raft_uuid, 'orig-dbi')
    dest_file_path = os.path.join(backup_dir, last_file)

    # Copy the file to the destination directory
    copy_file(source_file_path, dest_file_path)

    # Read the binary file
    with open(source_file_path, "rb") as f:
        data = bytearray(f.read())

    # Ensure the file is at least 16 bytes long
    if len(data) < 16:
        print("File is too small to modify the first 16 bytes")
        return

    # Create a new 16-byte entry with Type set to 1 and the rest set to zero
    new_entry = bytearray(16)
    new_entry[0] = 0x01  # Set Type to 1

    # Copy the new entry to the first 16 bytes of the data
    data[:16] = new_entry

    # Write the modified data back to the original file
    with open(source_file_path, "wb") as f:
        f.write(data)

    # upload the file 
    Perform_S3_Operation(cluster_params, source_file_path, "upload", chunk) 

def uploadAndDeleteCorruptedFile(cluster_params, dirName, operation, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, dirName)
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    newPath = jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json"
    json_data = load_json_contents(newPath)

    dbi_input_path = str(json_data['DbiPath'])
    vdev = str(json_data['Vdev'])
    logFile = "%s/%s/s3operation" % (base_dir, raft_uuid)

    # Get a list of files in the source directory
    file_list = os.listdir(dbi_input_path)

    last_file = ""
    if len(file_list) > 1:
        # Sort the file list by modification time (oldest to newest)
        file_list.sort(key=lambda x: os.path.getmtime(os.path.join(dbi_input_path, x)))
        # Get the last file in the sorted list
        last_file = file_list[-1]

    # Construct the source file path
    source_file_path = os.path.join(dbi_input_path, last_file)
    # Get the size of the file
    fileSize = os.path.getsize(source_file_path)
    # Split the file name using '.' as the delimiter
    parts = last_file.split('.')
    numOfEntries = parts[4]
    # Calculate the size of each entry
    entry_size = int(fileSize) // int(numOfEntries)
    # Construct the destination directory path
    dest_dir = os.path.join(base_dir, raft_uuid, 'origDBIFile')
    dest_file_path = os.path.join(dest_dir, last_file)
    s3config = '%s/s3.config.example' % binary_dir
    bin_path = '%s/s3Operation' % binary_dir
    if operation == "upload":
            # Check if the destination directory exists, and create if not
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir, mode=0o777)
            # Copy the file to the destination directory
            shutil.copy(source_file_path, dest_file_path)
            # Modify the last file by adding "0000" at the end
            with open(source_file_path, 'r+b') as f:
                 # Seek to the position where the last entry starts
                 f.seek(fileSize - entry_size)
                 # Write zeroes to the remaining part of the file
                 f.write(b'\x00' * entry_size)

            process = subprocess.Popen([bin_path, '-bucketName', 'paroscale-test', '-operation', operation, '-s3config', s3config, '-filepath', source_file_path, '-l', logFile])

    elif operation == "delete":

            process = subprocess.Popen([bin_path, '-bucketName', 'paroscale-test', '-operation', operation, '-v', vdev, '-c', chunk, '-s3config', s3config, '-filepath', source_file_path, '-l', logFile])

def PushOrigFileToS3(cluster_params, dirName, operation, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, dirName)
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    json_data = load_json_contents(jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json")
    dbi_input_path = str(json_data['DbiPath'])
    vdev = str(json_data['Vdev'])
    logFile = "%s/%s/s3operation" % (base_dir, raft_uuid)

    # Construct the destination directory path
    dest_dir = os.path.join(base_dir, raft_uuid, 'orig-dbi')

    # Get a list of files in the source directory
    file_list = os.listdir(dest_dir)
    if len(file_list) >= 1:
        # Sort the file list by modification time (oldest to newest)
        file_list.sort(key=lambda x: os.path.getmtime(os.path.join(dest_dir, x)))

        # Get the last file in the sorted list
        last_file = file_list[0]

        # Construct the source file path
        source_file_path = os.path.join(dest_dir, last_file)
        # Construct the destination directory path
        dest_file_path = os.path.join(dbi_input_path, last_file)
        # Copy the file to the destination directory
        shutil.copy(source_file_path, dest_file_path)
        s3config = '%s/s3.config.example' % binary_dir
        bin_path = '%s/s3Operation' % binary_dir
        print("cmd ", bin_path, '-bucketName', 'paroscale-test', '-operation', operation, '-s3config', s3config, '-filepath', dest_file_path, '-v', vdev, '-l', logFile)
        process = subprocess.Popen([bin_path, '-bucketName', 'paroscale-test', '-operation', operation, '-s3config', s3config, '-filepath', dest_file_path,'-v', vdev, '-l', logFile])

def uploadOrigFile(cluster_params, dirName, operation, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, dirName)
    binary_dir = os.getenv('NIOVA_BIN_PATH')

    json_data = load_json_contents(jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json")
    dbi_input_path = str(json_data['DbiPath'])
    vdev = str(json_data['Vdev'])
    logFile = "%s/%s/s3operation" % (base_dir, raft_uuid)

    # Get a list of files in the source directory
    file_list = os.listdir(dbi_input_path)

    if len(file_list) > 1:
        # Sort the file list by modification time (oldest to newest)
        file_list.sort(key=lambda x: os.path.getmtime(os.path.join(dbi_input_path, x)))

        # Get the last file in the sorted list
        last_file = file_list[-1]

        # Construct the source file path
        source_file_path = os.path.join(dbi_input_path, last_file)
        # Construct the destination directory path
        dest_dir = os.path.join(base_dir, raft_uuid, 'origDBIFile')
        dest_file_path = os.path.join(dest_dir, last_file)
        # Copy the file to the destination directory
        shutil.copy(dest_file_path, source_file_path)
        s3config = '%s/s3.config.example' % binary_dir
        bin_path = '%s/s3Operation' % binary_dir
        process = subprocess.Popen([bin_path, '-bucketName', 'paroscale-test', '-operation', operation, '-s3config', s3config, '-filepath', source_file_path, '-l', logFile])

def get_latest_modified_file(cluster_params, dirName, chunk):
    jsonPath = get_dir_path(cluster_params, dirName)
    json_data = load_json_contents(jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json")
    dbi_input_path = str(json_data['DbiPath'])
    # Get a list of files in the source directory
    file_list = os.listdir(dbi_input_path)
    if len(file_list) > 1:
        # Sort the file list by modification time (oldest to newest)
        file_list.sort(key=lambda x: os.path.getmtime(os.path.join(dbi_input_path, x)))
        # Get the last file in the sorted list
        last_file = file_list[-2]
        file_path = dbi_input_path + "/" + last_file
        return file_path

def Perform_S3_Operation(cluster_params, path, operation, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, DBI_DIR)
    json_data = load_json_contents(jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json")
    vdev = str(json_data['Vdev'])
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = '%s/s3Operation' % binary_dir
    s3config = '%s/s3.config.example' % binary_dir
    logFile = "%s/%s/s3operation" % (base_dir, raft_uuid)
    cmd = [bin_path, '-bucketName', 'paroscale-test', '-operation', operation, '-v', vdev, '-c', chunk, '-s3config', s3config, '-l', logFile, "-p", path]

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process


def get_DBiFileNames(cluster_params, dirName, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, dirName)
    json_data = load_json_contents(jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json")
    dbi_input_path = str(json_data['DbiPath'])

    # Initialize a list to store file names
    file_names = []

    # Iterate over the files in the directory
    for filename in os.listdir(dbi_input_path):
        if os.path.isfile(os.path.join(dbi_input_path, filename)):
            file_names.append(filename)

    # Create a JSON file and write the list of file names to it
    json_filename = 'DBIFileNames.json'
    path = "%s/%s/%s" % (base_dir, raft_uuid, json_filename)
    with open(path, 'w') as json_file:
        json.dump(file_names, json_file)

def copy_DBI_file_generatorNum(cluster_params, dirName, chunk):
    jsonPath = get_dir_path(cluster_params, dirName)
    path = jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json"
    json_data = load_json_contents(path)
    dbi_input_path = str(json_data['DbiPath'])
    
    # Get a list of files in the source directory
    file_list = os.listdir(dbi_input_path)

    # Check if there are any files in the directory
    if file_list:
        # Select a random file from the list
        random_file = random.choice(file_list)
        filename_parts = random_file.split(".")
        # Extract the generation number
        genration_num = filename_parts[1]

        # Increment the extracted element by 1
        # Decrement as the number is inversed
        new_genration_num = str(int(genration_num, 16) - 1)

        # Update the filename with the incremented element
        filename_parts[1] = new_genration_num
        new_filename = ".".join(filename_parts)
        # Full path for the copied and renamed file
        source_file_path = os.path.join(dbi_input_path, random_file)
        new_file_path = os.path.join(dbi_input_path, new_filename)

        # Copy the file and rename the copy
        shutil.copy(source_file_path, new_file_path)
    else:
        print("No files found in the directory.")

def deleteFiles(cluster_params, dirName, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    json_filename = 'DBIFileNames.json'

    jsonPath = get_dir_path(cluster_params, dirName)
    json_data = load_json_contents(jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json")
    dbi_input_path = str(json_data['DbiPath'])

    # Read the JSON file to get the list of file names
    json_filename = 'DBIFileNames.json'
    path = "%s/%s/%s" % (base_dir, raft_uuid, json_filename)
    with open(path, 'r') as json_file:
        file_names = json.load(json_file)

    # Calculate how many files you want to delete (half of the total)
    total_files = len(file_names)
    files_to_delete = total_files // 2

    for i in range(files_to_delete):
        file_to_delete = os.path.join(dbi_input_path, file_names[i])
        try:
            os.remove(file_to_delete)
        except Exception as e:
            print(f"Error deleting {file_to_delete}: {str(e)}")

def deleteSetFileS3(cluster_params, dirName, operation, chunk):
    jsonPath = get_dir_path(cluster_params, dirName)
    json_path = jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json"
    json_data = load_json_contents(json_path)
    dbi_input_path = str(json_data['DbiPath'])
    vdev = str(json_data['Vdev'])

    filenames = []
    destination_path = os.path.join(jsonPath, "dbisetFname.txt")
    with open(destination_path, 'r') as file:
        # Read the contents of the file
        file_contents = file.read()

    file_list = file_contents.rstrip(', ').split(', ')
    filename = random.choice(file_list)
    file_path = filename
    fname = os.path.basename(filename)
    prefix = filename.split('.')[0]

    # Create a list to store files with the same prefix
    files_with_same_prefix = [file for file in file_list if file.startswith(prefix)]
    with open(destination_path, 'w') as file:
        for item in files_with_same_prefix:
            file.write(item + ", ")
    # Delete file locally
    os.remove(file_path)
    process = Perform_S3_Operation(cluster_params, fname, operation, chunk)

def copyDBIset_NewDir(cluster_params, dirName, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, dirName)
    json_path = jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json"
    json_data = load_json_contents(json_path)
    dbi_input_path = str(json_data['DbiPath'])
    vdev = str(json_data['Vdev'])

    newDir = "dbiSetFiles"
    print("jsonPath:", jsonPath)
    file_path = os.path.join(jsonPath, "dbisetFname.txt")
    file_contents = ""
    try:
        with open(file_path, 'r') as file:
            # Read the contents of the file
            file_contents = file.read()
    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")

    file_list = file_contents.rstrip(', ').split(', ')
    for fileNames in file_list:
        source_path = fileNames
        destination_path = jsonPath + newDir
        if not os.path.exists(destination_path):
            os.makedirs(destination_path, mode=0o777)
        shutil.copy2(source_path, destination_path)

def copyDBIFile_changeSeqNum(cluster_params, dirName, chunk):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    jsonPath = get_dir_path(cluster_params, dirName)
    json_path = jsonPath + "/" + str(chunk) + "/DV/" + "dummy_generator.json"
    json_data = load_json_contents(json_path)
    dbi_input_path = str(json_data['DbiPath'])

    # Check if the path is a directory
    if not os.path.isdir(dbi_input_path):
        print(f"{dbi_input_path} is not a valid directory path.")
        return

    # Get the list of files in the directory
    files = [f for f in os.listdir(dbi_input_path) if os.path.isfile(os.path.join(dbi_input_path, f))]

    # Check if there are any files in the directory
    if not files:
        print(f"No files found in {dbi_input_path}.")
        return

    # Get the last file from the list
    last_file = files[-1]

    filename_parts = last_file.split(".")
    old_seq_num = filename_parts[0]
    seq_num = old_seq_num.split("_")

    # Increment the extracted element by 1
    new_end_seq = str(int(seq_num[0]) + 1)
    new_start_seq = str(int(seq_num[1]) + 1)
    joined_seq = "_".join([new_end_seq, new_start_seq])

    filename_parts[0] = joined_seq
    new_filename = ".".join(filename_parts)

    os.rename(dbi_input_path, new_filename)

def extracting_dictionary(cluster_params, operation, dirName):
    if operation == "load_contents":
        data = load_json_contents(input_values['path'])
        return data

def check_if_mType_Present(vdev, chunk, mList, mType):
    for line in (mList.splitlines()):
        parts = line.split(".")
        if vdev in parts[Marker_vdev] and chunk in parts[Marker_chunk] and mType in parts[Marker_type]:
            return parts[2]
    return None

def GetSeqOfMarker(cluster_params, dirName, chunk, value):
    jsonPath = get_dir_path(cluster_params, dirName)
    if jsonPath is not None:
        newPath = os.path.join(jsonPath, chunk, "DV")
        json_data = load_json_contents(os.path.join(newPath, "dummy_generator.json"))
        vdev = str(json_data['Vdev'])

        process = Perform_S3_Operation(cluster_params, "m", "list", chunk)
        exit_code = process.wait()
        if exit_code == 0:
            print("Process completed successfully.")
        else:
            error_message = print("Process failed with exit code {exit_code}.")
            raise RuntimeError(error_message)

        # Read the output and error
        stdout, stderr = process.communicate()

        # Check if any file is found
        GcSeq = check_if_mType_Present(vdev, chunk, stdout, "gc")
        NisdSeq = check_if_mType_Present(vdev, chunk, stdout, "nisd")
        print("GcSeq : ", GcSeq)
        print("NisdSeq : ", NisdSeq)
        MSeq = []
        match value:
            case 'gc':
                MSeq.extend([GcSeq, None])
                return MSeq
            case 'nisd':
                MSeq.extend([None, NisdSeq])
                return MSeq
            case 'Both':
                MSeq.extend([GcSeq, NisdSeq])
                return MSeq
    else:
        print("Invalid path or directory not found.")
        return False


# TODO check for all chunks
def GetSeqOfMarkerWithVdev(cluster_params, vdev, chunk, value):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    s3Support = cluster_params['s3Support']
    logFile = "%s/%s/s3operation" % (base_dir, raft_uuid)
    binary_dir = os.getenv('NIOVA_BIN_PATH')
    bin_path = '%s/s3Operation' % binary_dir
    s3config = '%s/s3.config.example' % binary_dir
    cmd = [bin_path, '-bucketName', 'paroscale-test', '-operation', 'list', '-v', vdev, '-c', chunk, '-s3config', s3config, '-p', 'm', '-l', logFile]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    exit_code = process.wait()
    if exit_code == 0:
        print("Process completed successfully.")
    else:
        error_message = print("Process failed with exit code {exit_code}.")
        raise RuntimeError(error_message)

    # Read the output and error
    stdout, stderr = process.communicate()

    # Check if any file is found
    GcSeq = check_if_mType_Present(vdev, chunk, stdout, "gc")
    NisdSeq = check_if_mType_Present(vdev, chunk, stdout, "nisd")
    print("GcSeq : ", GcSeq)
    print("NisdSeq : ", NisdSeq)
    MSeq = []
    match value:
        case 'gc':
            MSeq.extend([GcSeq, None])
            return MSeq
        case 'nisd':
            MSeq.extend([None, NisdSeq])
            return MSeq
        case 'Both':
            MSeq.extend([GcSeq, NisdSeq])
            return MSeq

class LookupModule(LookupBase):
    def run(self,terms,**kwargs):
        #Get lookup parameter values
        dirName = "dbi-dbo"
        s3Dir = ""
        port = ""
        operation = terms[0]

        # Generate a random chunkNumber
        chunkNumber = str(random.randint(1, 200))
        cluster_params = kwargs['variables']['ClusterParams']

        if operation == "generate_pattern":
            input_values = terms[1]
            popen = start_pattern_generator(cluster_params, input_values['genType'], dirName, input_values, removeFiles=False)
            return popen

        elif operation == "start_s3":
            s3Dir = terms[1]
            process = start_minio_server(cluster_params, s3Dir)

        elif operation == "start_gc":
            # Assume terms is a list with relevant arguments
            if len(terms) >= 3:
               debugMode = terms[1]
               chunk = terms[2]
               # Check if crcCheck is provided
               crcCheck = terms[3] if len(terms) > 3 else None
               popen = start_gc_process(cluster_params, dirName, debugMode, chunk, crcCheck)
               return popen
            else:
               raise ValueError("Not enough arguments provided for start_gc operation")

        elif operation == "start_gcService":
            dryRun = terms[1]
            delDBO = terms[2]
            partition = terms[3]
            no_of_chunks = terms[4]
            force_gc = terms[5]
            start_gcService_process(cluster_params, dirName, dryRun, delDBO, partition, force_gc, no_of_chunks)

        elif operation == "data_validate":
            Chunk = terms[1]
            popen = start_data_validate(cluster_params, dirName, Chunk)

        elif operation == "createPartition":
            create_gc_partition(cluster_params)

        elif operation == "createFile":
            img_dir = terms[1]
            bs = terms[2]
            count = terms[3]
            device_path = create_file(cluster_params, img_dir, bs, count)
            return device_path

        elif operation == "deleteFile":
            delete_file(cluster_params)

        elif operation == "deletePartition":
            delete_partition(cluster_params)

        elif operation == "generate_data":
            device_path = terms[1]
            run_fio_test(device_path)

        elif operation == "setup_btrfs": 
            mount = terms[1]
            mount_path =  setup_btrfs(cluster_params, mount)
            return mount_path

        elif operation == "pauseGCProcess":
            pid = terms[1]
            pause_gcProcess(pid)
        
        elif operation == "resumeGCProcess":
            pid = terms[1]
            resume_gcProcess(pid)

        elif operation == "parallel_data_generation":
            no_of_chunks = terms[1]
            generate_dbi_dbo_concurrently(cluster_params, dirName, no_of_chunks)

        elif operation == "get_DBI_fileNames":
            Chunk = terms[1]
            get_DBiFileNames(cluster_params, dirName, Chunk)

        elif operation == "delete_DBI_files":
            Chunk = terms[1]
            deleteFiles(cluster_params, dirName, Chunk)

        elif operation == "deleteSetFileS3":
            operation = terms[1]
            chunk = terms[2]
            popen = deleteSetFileS3(cluster_params, dirName, operation, chunk)

        elif operation == "performS3Operation":
            s3Operation = terms[1]
            chunk = terms[2]
            filename = terms[4]
            list_prefix = terms[5]
            Perform_S3_Operation(cluster_params, path, s3Operation, chunk)

        elif operation == "get_latest_modified_file":
            chunk = terms[1]
            filename = get_latest_modified_file(cluster_params, dirName, chunk)
            return filename

        elif operation == "corruptedFileOps":
            s3Operation = terms[1]
            chunk = terms[2]
            uploadAndDeleteCorruptedFile(cluster_params, dirName, s3Operation, chunk)

        elif operation == "uploadOrigFile":
            s3Operation = terms[1]
            chunk = terms[2]
            uploadOrigFile(cluster_params, dirName, s3Operation, chunk)

        elif operation == "performCorruptedFileOps":
            s3Operation = terms[1]
            chunk = terms[2]
            copy_file_to_backup(cluster_params, dirName, s3Operation, chunk)

        elif operation == "pushOrigFileToS3":
            s3Operation = terms[1]
            chunk = terms[2]
            PushOrigFileToS3(cluster_params, dirName, s3Operation, chunk)

        elif operation == "copyDBIset":
            chunk = terms[1]
            copyDBIset_NewDir(cluster_params, dirName, chunk)

        elif operation == "copy_DBI_file_generatorNum":
            chunk = terms[1]
            copy_DBI_file_generatorNum(cluster_params, dirName, chunk)

        elif operation == "copyDBIFile_changeSeqNum":
            chunk = terms[1]
            copyDBIFile_changeSeqNum(cluster_params, dirName, chunk)

        elif operation == "multiple_iteration_params" or operation == "example_params":
            input_values = terms[1]
            popen = multiple_iteration_params(cluster_params, dirName, input_values)

            return popen

        elif operation == "GetSeqOfMarker":
            chunk = terms[2]
            value = terms[1]
            MarkerSeq = GetSeqOfMarker(cluster_params, dirName, chunk, value)

            return MarkerSeq

        elif operation == "GetSeqOfMarkerWithVdev":
            chunk = terms[2]
            value = terms[1]
            vdev = terms[3]
            MarkerSeq = GetSeqOfMarkerWithVdev(cluster_params, vdev, chunk, value)

            return MarkerSeq
        
        elif operation == "run_s3DV":
            device_path = terms[1]
            ublk_uuid   = terms[2]
            nisd_uuid   = terms[3]
            start_s3_data_validator(cluster_params, device_path, ublk_uuid, nisd_uuid)

        elif operation == "create_bucket":
            bucket = terms[1]
            create_bucket(cluster_params, bucket)

        elif operation == "json_params":
            params_type = terms[1]
            load_params = load_parameters_from_json("seed.json")
            if params_type == "single_iteration":
               single_itr_seq = ['run_example', 'run_gc', 'run_data_validator']
               for params in load_params["single_iteration"]:
                  for i in single_itr_seq:
                     prepare_command_from_parameters(cluster_params, [params], dirName, i, params_type)
        else:
            data = extracting_dictionary(cluster_params, operation, dirName)
            return data
