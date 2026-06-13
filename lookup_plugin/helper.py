import json
import os  
import shutil, time
from genericcmd import *
import random, subprocess
import pwd
from ansible.plugins.lookup import LookupBase

# s3 bucket name
S3_BUCKET = "paroscale-test" 
# dummy-generator directory
DBI_DIR = "dbi-dbo" 
TEMP_DIR = "temp_dir"
# dbi set list file name
DBI_SET_LIST  = "dbi_set_list.txt"
# constant to get vdev, chunk and type from marker file
MARKER_VDEV = 0
MARKER_CHUNK = 1
MARKER_TYPE = 4
# constant to get generation number from dbi file name
GEN_NUM = 1

def load_parameters_from_json(filename):
    with open(filename, 'r+', encoding="utf-8") as json_file:
        params = json.load(json_file)
    return params

def create_file(file_name: str, content: str):
    with open(file_name, 'w') as file:
        file.write(content)

def delete_file(file_name: str):
    if os.path.exists(file_name):
        os.remove(file_name)

def load_recipe_op_config(cluster_params):
    recipe_conf = {}
    raft_json_fpath = "%s/%s/%s.json" % (cluster_params['base_dir'],
                                         cluster_params['raft_uuid'],
                                         cluster_params['raft_uuid'])
    if os.path.exists(raft_json_fpath):
        with open(raft_json_fpath, "r+", encoding="utf-8") as json_file:
            recipe_conf = json.load(json_file)

    return recipe_conf

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
        dbi_dbo_index = parts.index(DBI_DIR)
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

def create_dir(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, mode=0o777)

def clone_dbi_files(cluster_params, chunk):
    dir_path = get_dir_path(cluster_params, DBI_DIR)
    dbi_list_path = os.path.join(dir_path, DBI_SET_LIST)
    file_list = read_file_list(dbi_list_path)
    dest_path = os.path.join(dir_path, TEMP_DIR)
    create_dir(dest_path)
    copy_files(file_list, dest_path)
    print("destination path:", dest_path)
    return [dest_path]

# generates the dummy generator config path
def get_dummy_gen_config_path(data_dir, chunk):
    return os.path.join(data_dir, "dataVal",str(chunk), "dummy_generator.json")

def read_file_list(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().rstrip(', ').split(', ')
    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        return []

def copy_files(file_list, destination_path):
    # If a single file is passed, wrap it in a list
    if isinstance(file_list, str):
        file_list = [file_list]
    os.makedirs(destination_path, exist_ok=True)

    for file_name in file_list:

        if not os.path.isdir(file_name):
            # cleaning if any additional quotes are present
            src = file_name.strip("'")
            os.chmod(src, 0o777)
            shutil.copy2(src, destination_path)

def get_dir_path(cluster_params, dir_name, seed=None):
    base_dir = cluster_params['base_dir']
    raft_uuid = cluster_params['raft_uuid']
    base_directory = os.path.join(base_dir, raft_uuid, dir_name, seed) if seed else os.path.join(base_dir, raft_uuid, dir_name)

    # Get a list of all directories under the specified path
    directories = [entry for entry in os.listdir(base_directory) if os.path.isdir(os.path.join(base_directory, entry))]

    if directories:
        # Sort directories based on creation time (most recent first)
        directories.sort(key=lambda d: os.path.getctime(os.path.join(base_directory, d)), reverse=True)
        # Return the path of the most recently created directory with a trailing slash
        return os.path.join(base_directory, directories[0], '')
    return None

def list_files_from_dir(dir_path):
    return [os.path.abspath(os.path.join(dir_path, file)) for file in os.listdir(dir_path)]   

def get_marker_by_type(vdev, chunk, mList, mType):
    for line in (mList.splitlines()):
        parts = line.split(".")
        if vdev in parts[MARKER_VDEV] and chunk in parts[MARKER_CHUNK] and mType in parts[MARKER_TYPE]:
            return parts[2]
    return None   

def get_last_file_from_dir(dir_path):
    file_list = list_files_from_dir(dir_path) # Get a list of files in the source directory
    if len(file_list) > 1:
        # Sort the file list by modification time (oldest to newest)
        file_list.sort(key=lambda x: os.path.getmtime(os.path.join(dir_path, x)))
        # Remove directories from list
        file_list = [f for f in file_list if not os.path.isdir(os.path.join(dir_path, f))]
        return file_list[-1]  # return the last file in the sorted list
    return None

def get_unmounted_ublk_device(path):
    output_file = "%s/%s" % (path, "lsblk_output.txt")
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
            with open(output_file, 'w') as file:
                file.write(result.stdout)

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
            raise e

        time.sleep(interval)  
    print("No unmounted ublk device found after 3 minutes.")
    return None

class helper:
    def __init__(self, cluster_params):
        self.cluster_params = cluster_params
        self.bin_dir =  os.getenv('NIOVA_BIN_PATH')
        self.base_path = f"{cluster_params['base_dir']}/{cluster_params['raft_uuid']}/"
    
    def create_dd_file(self, filename, bs, count):
    
        # Normalize filename to avoid leading /
        safe_filename = filename.lstrip(os.sep)
        full_path = os.path.normpath(os.path.join(self.base_path, safe_filename))
        
        rc = 0

        try:
            # os.makedirs(os.path.dirname(full_path), exist_ok=True)
            dir_path = os.path.dirname(full_path)
            dd_command = f"dd if=/dev/zero of={full_path} bs={bs} count={count}"
            print(f"Running command: {dd_command}")
            result = subprocess.run(
                dd_command,
                check=True, shell=True
            )
            print(f"File created successfully at: {full_path}")
        except subprocess.CalledProcessError as e:
            rc = e.returncode
            print(f"Error: {e}") 
        return {
            "path": full_path,
            "rc": rc
        }
        
    def create_gc_partition(self, dir, total_blocks):
        dir_name_abs = os.path.join(self.base_path, dir)
        dir_name = dir

        # Get the current UID
        uid = os.geteuid()

        # Get the current user's info
        user_info = pwd.getpwuid(uid)
        username = user_info.pw_name

        disk_info = self.create_dd_file("GC.img", "64M", total_blocks)

        if disk_info["rc"] != 0:
            return disk_info

        disk_ipath = disk_info["path"]

        try:
            result = subprocess.run(["losetup", "-fP", disk_ipath], check=True)
        except subprocess.CalledProcessError as e:
            return {
                "path": disk_ipath,
                "rc": e.returncode,
                "stderr": str(e)
            }
            
        # setup btrfs and mount
        self.setup_btrfs(dir_name, disk_ipath)

        try:
            subprocess.run(["chown", username, dir_name_abs], check=True)
            subprocess.run(["chmod", "777", dir_name_abs], check=True)
        except subprocess.CalledProcessError as e:
            return {
                "path": disk_ipath,
                "rc": e.returncode,
                "stderr": str(e)
            }
            
        return {
            "path": disk_ipath,
            "rc": 0
        }

    def delete_dd_file(self, filename):
        file_path = os.path.join(self.base_path, filename)
        delete_file(file_path)

    def generate_data(self, directory_path):
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
                raise e
            time.sleep(30)

    def clear_dir_contents(self, path):
        path = os.path.join(self.base_path, path)
        try:
            if not os.path.isdir(path):
                raise NotADirectoryError(f"'{path}' is not a directory.")
            for item in os.listdir(path):
                item_path = os.path.join(path, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
        except Exception as e:
            print(f"Error: {e}")

    def setup_btrfs(self, mount_point, device_path):
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
        mount_path = "%s/%s" % (self.base_path, mount_point)

        if device_path != "":
            device = device_path
        else :
            device = get_unmounted_ublk_device(self.base_path)

        if device == None: 
            raise RuntimeError(f"no ublk device available")
        try:
            # Step 1: Format the device with Btrfs
            subprocess.run(["mkfs.btrfs", device], check=True)
            print(f"Formatted {device} successfully.")

            # Step 2: Create the mount point directory if it doesn't exist
            if not os.path.exists(mount_path):
                os.makedirs(mount_path)
                subprocess.run(["chmod", "777", mount_path], check=True)
                print(f"Directory {mount_path} created.")

            # Step 3: Mount the device to the mount point
            subprocess.run(["mount", device, mount_path], check=True)
            print(f"Mounted {device} to {mount_path} successfully.")

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"An error occurred while setting up Btrfs: {e}")

        recipe_conf = load_recipe_op_config(self.cluster_params)
        if not "btrfs_process" in recipe_conf:
            recipe_conf['btrfs_process'] = {}
        recipe_conf['btrfs_process']['mount_point'] = mount_path
        genericcmdobj = GenericCmds()
        genericcmdobj.recipe_json_dump(recipe_conf)
        return [mount_path, device]

    def get_set_file_list(self, chunk, deleted_file):
        dbi_Path = get_dir_path(self.cluster_params, DBI_DIR)
        dummy_config = get_dummy_gen_config_path(dbi_Path, chunk)
        dummy_json_data = load_parameters_from_json(dummy_config)
        vdev = str(dummy_json_data['Vdev'])
        file_path = f"{self.base_path}/{DBI_DIR}/{vdev}/{DBI_SET_LIST}"
        with open(file_path, 'r') as f:
            file_names = {os.path.basename(line.strip())  for line in f if line.strip()}
        if deleted_file and os.path.basename(deleted_file.strip()) in file_names:
            file_names.remove(os.path.basename(deleted_file.strip()))
        return list(file_names)

    def compare_files(self, file_names, command_output) -> None:
        output_files = {os.path.basename(line.strip()) for line in command_output.splitlines() if line.strip()}
        missing_files = set(file_names) - output_files
        print("missing ", missing_files)
        if missing_files:
            raise ValueError(f"The following files are missing in the command output: {', '.join(missing_files)}")
    
    def corrupt_last_file(self, chunk):
        dbi_path = get_dir_path(self.cluster_params, DBI_DIR)
        dummy_config = load_parameters_from_json(get_dummy_gen_config_path(dbi_path, chunk))
        vdev = dummy_config['Vdev']
        dbi_input_path = str(dummy_config['DbiPath'])
        dbi_list = list_files_from_dir(dbi_input_path)
        dest_path = f"{self.base_path}/{TEMP_DIR}/{vdev}/{str(chunk)}"
        copy_files(dbi_list, dest_path)
        orig_file_path = get_last_file_from_dir(dest_path)
        corrupt_file_path = get_last_file_from_dir(dbi_input_path)
        if not corrupt_file_path:
            print("No files found in the directory.")
            return None
        with open(corrupt_file_path, "rb") as f:
            data = bytearray(f.read())
        if len(data) < 16: # Ensure the file is at least 16 bytes long
            print("File is too small to modify the first 16 bytes")
            return None
        new_entry = bytearray(16)  # Create a new 16-byte entry with Type set to 1 and the rest set to zero
        new_entry[0] = 0x01  # Set Type to 1
        data[:16] = new_entry # Copy the new entry to the first 16 bytes of the data
        with open(corrupt_file_path, "wb") as f: # Write the modified data back to the original file
            f.write(data)
        print("corrupted file path:",corrupt_file_path)
        return [corrupt_file_path, orig_file_path]
    
    def inc_dbi_gen_num(self, chunk):
        dbi_Path = get_dir_path(self.cluster_params, DBI_DIR)
        dummy_config = get_dummy_gen_config_path(dbi_Path, chunk)
        dummy_json_data = load_parameters_from_json(dummy_config)
        dbi_input_path = str(dummy_json_data['DbiPath'])
        files_list = list_files_from_dir(dbi_input_path)    # List files from dbipath
        
        if files_list:
            random_file = random.choice(files_list)  # Select a random file from the list
            filename_parts = random_file.split(".")
            genration_num = filename_parts[GEN_NUM]  # Extract the generation number
            # Increment the extracted element by 1, Decrement as the number is inversed
            inc_gen_num = str(hex(int(genration_num, 16) - 1)[2:])
            filename_parts[GEN_NUM] = inc_gen_num  # Update the filename with the incremented element
            new_filename = ".".join(filename_parts)
            source_file_path = os.path.join(dbi_input_path, random_file)
            new_file_path = os.path.join(dbi_input_path, new_filename)
            return [source_file_path, new_file_path]
        else:
            print("No files found in the directory.")

    def keep_last_entry(self, file_rel_path):
        # Read the file contents
        file_path = os.path.normpath(f"{self.base_path}/{file_rel_path}")
        
        print(f"{os.path.normpath(f"{self.base_path}/{file_rel_path}")}")
         
        with open(file_path, 'r') as file:
            lines = file.read().split('\n')
        
        # Filter out any empty lines and get the last non-empty entry
        non_empty_lines = [line for line in lines if line.strip()]
        last_entry = non_empty_lines[-1] if non_empty_lines else ''
        
        # Overwrite the file with only the last entry
        with open(file_path, 'w') as file:
            file.write(last_entry + '\n')

class LookupModule(LookupBase):
    def run(self, terms, **kwargs):
        operation = terms[0]
        cluster_params = kwargs['variables']['ClusterParams']
        help = helper(cluster_params)

        if operation == "create_dd_file":
            img_dir = terms[1]
            bs = terms[2]
            count = terms[3]
            result = help.create_dd_file(img_dir, bs, count)
            return [result["path"]]

        elif operation == "delete_dir":
            dir = terms[1]
            help.clear_dir_contents(dir)
            return []

        elif operation == "setup_btrfs": 
            mount = terms[1]
            mount_path =  help.setup_btrfs(mount, "")
            return [mount_path]
        
        elif operation == "generate_data":
            device_path = terms[1]
            help.generate_data(device_path)
            return []

        elif operation == "clone_dbi_set":
            chunk = terms[1]
            return [clone_dbi_files(cluster_params, chunk)]
            
        elif operation == "corrupt_last_file":
            chunk = terms[1]
            return [help.corrupt_last_file(chunk)]

        elif operation == "inc_dbi_gen_num":
            chunk = terms[1]
            return [help.inc_dbi_gen_num(chunk)]

        elif operation == "copy_file":
            source_file = terms[1]
            dest_file = terms[2]
            copy_files(source_file, dest_file)
            return []

        elif operation == "create_partition":
            dir = terms[1]
            total_blocks = terms[2]
            result = help.create_gc_partition(dir, total_blocks)
            return [result]
        
        elif operation == "delete_dd_file":
            filename = terms[1]
            help.delete_dd_file(filename)
            return []

        elif operation == "get_set_file_list":
            chunk = terms[1]
            deleted_file = terms[2]
            return [help.get_set_file_list(chunk, deleted_file)]

        elif operation == "check_files":
            file_list = terms[1]
            stdout = terms[2]
            help.compare_files(file_list, stdout)
            return []
        
        elif operation == "keep_last_entry":
            file_rel_path = terms[1]
            
            result = help.keep_last_entry(file_rel_path)
            
            return [result]
    
        else:
            raise ValueError(f"Unsupported operation: {operation}")