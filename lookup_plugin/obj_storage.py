from ansible.plugins.lookup import LookupBase
import os, random
import subprocess
import logging
import psutil
from genericcmd import GenericCmds
from lookup_plugin.helper import *
import signal

GET_VDEV = "get_vdev_from_dummy_generator.json"

class Minio:
    def __init__(self, cluster_params, minio_path):
        self.cluster_params = cluster_params
        self.minio_path = minio_path
        self.base_path = f"{cluster_params['base_dir']}/{cluster_params['raft_uuid']}/"
        self.s3_server_log = f"{self.base_path}/s3_server.log"

    def start(self):
        s3Support = self.cluster_params['s3Support']
        
        minio_bin_path = shutil.which("minio")

        if not minio_bin_path:
            binary_dir = os.getenv('NIOVA_BIN_PATH')
            minio_ci_bin_path = os.path.join(binary_dir, 'minio')
            minio_bin_path = minio_ci_bin_path

        if s3Support:
            create_dir(self.minio_path)
            
            command = [
                    minio_bin_path,
                    "server",
                    self.minio_path,
                    "--console-address",
                    ":2000",
                    "--address",
                    ":2090"
                ]
            
            with open(self.s3_server_log, "w") as fp:
                process_popen = subprocess.Popen(command, stderr=fp, stdout=subprocess.PIPE, text=True)
                
            if process_popen.poll() is None:
                logging.info("MinIO server started successfully in the background.")
            else:
                logging.info("MinIO server failed to start")
                logging.error(f"MinIO server failed to start: {stderr.decode().strip()}")
                raise subprocess.SubprocessError(process_popen.returncode)

            self._update_recipe_conf(int(process_popen.pid))
                        
            return [process_popen.pid]
         
    def stop(self):
        try:
            subprocess.run(["pkill", "minio"], check=True)
            logging.info("MinIO server stopped successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to stop MinIO server: {e}")

    def _update_recipe_conf(self, pid):
        genericcmdobj = GenericCmds()
        recipe_conf = load_recipe_op_config(self.cluster_params)

        if not "s3_process" in recipe_conf:
            recipe_conf['s3_process'] = {}
            
        process_obj = psutil.Process(int(pid))

        if process_obj:
            minio_pid = process_obj.pid
            minio_status = process_obj.status()
            logging.info(f"MinIO server PID: {minio_pid}, Status: {minio_status}")
            recipe_conf['s3_process']['process_pid'] = minio_pid
            recipe_conf['s3_process']['process_status'] = minio_status
            genericcmdobj.recipe_json_dump(recipe_conf)

    def pause(self, minio_pid):        
        process_obj = psutil.Process(int(minio_pid))
        
        # Pause the minio process
        try:
            process_obj.send_signal(signal.SIGSTOP)
            print("MinIO has been paused.")
        except (ValueError, psutil.NoSuchProcess) as e:
            logging.error(f"minio: {e}")
            return -1
            
        return 0
    
    def resume(self, minio_pid):
        process_obj = psutil.Process(int(minio_pid))
               
        try:
            process_obj.send_signal(signal.SIGCONT)
            print("MinIO has been resumed.")
        except subprocess.SubprocessError as e:
            logging.error("Failed to send CONT signal with error: %s" % e)
            return -1        
        
        return 0
        
class s3_operations:
    def __init__(self, cluster_params):
        self.cluster_params = cluster_params
        self.bin_dir =  os.getenv('NIOVA_BIN_PATH')
        self.base_path = f"{cluster_params['base_dir']}/{cluster_params['raft_uuid']}/"
        self.s3_operations_log = f"{self.base_path}/s3_operation"

    def delete_dbi_set_s3(self, input_param):
        dir_path = get_dir_path(self.cluster_params, DBI_DIR)
        dbi_list_path = os.path.join(dir_path, DBI_SET_LIST)
        dbi_list = read_file_list(dbi_list_path)
        rand_dbi_path = random.choice(dbi_list)
        rand_dbi = os.path.basename(rand_dbi_path)
        dbi_prefix = rand_dbi.split('.')[0]
        # Create a list to store files with the same prefix
        dbi_set_files = [file for file in dbi_list if os.path.basename(file).startswith(dbi_prefix)]
        with open(dbi_list_path, 'w') as file:
            for item in dbi_set_files:
                file.write(item + "\n")
        # Delete file locally
        os.remove(rand_dbi_path)
        print("deleted file:", rand_dbi_path)
        input_param['path'] = rand_dbi_path
        input_param['vdev'] = GET_VDEV
        process = self.perform_operations("delete", input_param)
        return [rand_dbi_path]

    def delete_gc_dbi_from_s3(self, input_param, file_list):
        dir_path = get_dir_path(self.cluster_params, DBI_DIR)
        dbi_list = [line.strip() for line in file_list.splitlines() if line.strip()]

        if len(dbi_list) < 2:
            print("Not enough DBI files to select the second one.")
            return []

        # Select the second file instead of a random one
        second_dbi_path = dbi_list[1]  
        print("Deleted file:", second_dbi_path)

        input_param['path'] = second_dbi_path
        input_param['vdev'] = GET_VDEV
        process = self.perform_operations("delete", input_param)

        return [second_dbi_path]

    def get_dbi_list(self, input_param):
        input_param['path'] = "GET_VDEV"
        input_param['vdev'] = GET_VDEV
        stdout = self.perform_operations("list", input_param)
        return stdout

    def delete_half_dbis(self, input_param):
        # Split the output into lines
        list_output = input_param['output']
        lines = list_output.splitlines()

        # Get half of the lines
        half_lines = lines[:len(lines) // 2]
        dbi_path = os.path.join(self.base_path, "gc-download")
        # Print half of the lines
        for file in half_lines:
           if "solutionArray" in os.path.basename(file):
               continue 
           obj = os.path.join(dbi_path, file)
           os.remove(obj)
           input_param['path'] = file
           input_param['vdev'] = GET_VDEV 
           process = self.perform_operations("delete", input_param) 
        

    def perform_operations(self, operation, input_param):
        vdev = input_param.get('vdev')
        if vdev == GET_VDEV:
            dbi_path = get_dir_path(self.cluster_params, DBI_DIR)
            json_data = load_parameters_from_json(f"{dbi_path}/dataVal/{input_param['chunk']}/dummy_generator.json")
            vdev = str(json_data['Vdev'])
        elif vdev == None:
            vdev = ""
        bin_path = f'{self.bin_dir}/s3Operation'
        s3_config = f'{self.bin_dir}/s3.config.example'
        log_path = f'{self.s3_operations_log}_{operation}'
        outputfile = f'{self.base_path}/stdout.txt'
        if input_param['path'] == "GET_VDEV":
            input_param['path'] = f"{vdev}/{input_param['chunk']}"
        cmd = [
            bin_path, '-b', S3_BUCKET, '-o', operation,
            '-v', vdev, '-c', input_param['chunk'], '-s3config', s3_config, '-l', log_path, '-p', input_param['path']
        ]
        with open(outputfile, "w") as outfile:
            print("cmd: ", cmd)
            process = subprocess.Popen(cmd, stdout=outfile, stderr=subprocess.PIPE, text=True)
            process.wait()
            # Check if the process completed successfully
            if process.returncode != 0:
                print(f"Command failed: {' '.join(cmd)}")
                print(f"Return code: {process.returncode}")
                return f"Error: {stderr.strip()}"
  
        # Read the output file
        with open(outputfile, "r") as outfile:
            process_stdout = outfile.read()
            return process_stdout

    def get_markers(self, input_param):
        def process_and_get_markers(input_param):
            input_param['path'] = "m"
            stdout = self.perform_operations("list", input_param)

            gc_seq = get_marker_by_type(input_param['vdev'], input_param['chunk'], stdout, "gc")
            nisd_seq = get_marker_by_type(input_param['vdev'], input_param['chunk'], stdout, "nisd")
            print("gc_seq:", gc_seq, "nisd_seq:", nisd_seq)
            return [gc_seq, nisd_seq]
        
        if input_param.get('vdev') != None:
            return process_and_get_markers(input_param)
        else:
            dbi_path = get_dir_path(self.cluster_params, DBI_DIR)
            if dbi_path:
                json_path = f"{dbi_path}/dataVal/{input_param['chunk']}/dummy_generator.json"
                json_data = load_parameters_from_json(json_path)
                input_param['vdev'] = str(json_data.get('Vdev', input_param.get('vdev', 'default_value')))  # Use the vdev from JSON if available
                return process_and_get_markers(input_param)

            print("Invalid path or directory not found.")
            return False


class LookupModule(LookupBase):
    def run(self, terms, **kwargs):
        command = terms[0]
        cluster_params = kwargs['variables']['ClusterParams']
        
        if command == "minio":
            sub_cmd  = terms[1]
            if sub_cmd == "start":
                # Parameter :  Start the minio using the provided local path
                # to store minio data.
                minio_path = terms[2]
                minio = Minio(cluster_params, minio_path)
                minio_pid = minio.start()
                
                return [minio_pid]

            elif sub_cmd == "stop":
                minio = Minio(cluster_params, "")
                minio.stop()
                return []  
            
            elif sub_cmd == "pause":
                minio_path = terms[2]
                minio_pid = terms[3]
                
                minio = Minio(cluster_params, minio_path)
                
                minio.pause(minio_pid)
                
                return []
            
            elif sub_cmd == "resume":
                minio_path = terms[2]
                minio_pid = terms[3]
                
                minio = Minio(cluster_params, minio_path)
                
                minio.resume(minio_pid)
                
                return []
        
        elif command == "operation":
            operation = terms[1]
            '''
            Parameters: chunk, path, vdev, operation
            operation: to list/download/upload/delete
            chunk : chunk number, vdev: vdev uuid
            path: to download/upload/delete file and prefix in case of listing
            '''
            input_param = terms[2]
            s3 = s3_operations(cluster_params)
            if operation == "create_bucket":
                input_param['path'] = ""
                process = s3.perform_operations(operation, input_param)
            else: 
                input_param['vdev'] = GET_VDEV
                process = s3.perform_operations(operation, input_param)
            return [process]
        
        elif command == "delete_set_file":
            '''
            Parameters: chunk, path, vdev
            chunk : chunk number, vdev: vdev uuid
            path: to delete files from s3
            '''
            input_param = terms[1]
            s3 = s3_operations(cluster_params)
            return [s3.delete_dbi_set_s3(input_param)]

        elif command == "get_markers":
            '''
            Parameters: chunk, path, vdev
            chunk : chunk number, vdev: vdev uuid
            path: prefix(m) to list from s3
            ''' 
            input_param = terms[1]
            s3 = s3_operations(cluster_params)
            marker_seq = s3.get_markers(input_param)
            return [marker_seq]

        elif command == "get_list":
            '''
            Parameters: chunk, path, vdev
            chunk : chunk number, vdev: vdev uuid
            path: prefix(vdev/chunk) to list from s3
            '''
            input_param = terms[1]
            s3 = s3_operations(cluster_params)
            list_op = s3.get_dbi_list(input_param)
            return [list_op]

        elif command == "delete_half_files":
            input_param = terms[1]
            s3 = s3_operations(cluster_params)
            s3.delete_half_dbis( input_param )
            return []

        elif command == "delete_gc_dbi_from_s3":
            '''
            Parameters: chunk, path, vdev
            chunk : chunk number, vdev: vdev uuid
            path: to delete files from s3
            '''
            input_param = terms[1]
            file_list = terms[2] 
            s3 = s3_operations(cluster_params)
            return [s3.delete_gc_dbi_from_s3(input_param, file_list)]

        else:
            raise ValueError(f"Unsupported operation: {command}")