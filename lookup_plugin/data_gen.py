from ansible.plugins.lookup import LookupBase
import os, random
import subprocess
from multiprocessing import Pool
from lookup_plugin.helper import *

class data_generator:
    def __init__(self, cluster_params):
        self.cluster_params = cluster_params
        self.bin_dir =  os.getenv('NIOVA_BIN_PATH')
        self.base_path = f"{cluster_params['base_dir']}/{cluster_params['raft_uuid']}"
        self.s3_upload_log = f"{self.base_path}/s3Upload"

    def generate_random_values(self, dgen_args):
        dbi_path = get_dir_path(self.cluster_params, DBI_DIR)

        if dbi_path != None:
            json_data = load_parameters_from_json(f"{dbi_path}/dataVal/{dgen_args['chunk']}/dummy_generator.json")
            dgen_args['chunk'] = str(json_data['TotalChunkSize'])
            dgen_args["seqStart"] = str(json_data['SeqEnd'] + 1)
            dgen_args["vdev"] = str(json_data['Vdev'])
            dbicount = str(json_data['TMinDbiFileForForceGC'])
            is_prev_snapshot = bool(json_data['CurIterSnapShot'])
        else:
            dgen_args["seqStart"] = "0"
            dbicount = "0"
            is_prev_snapshot = False

        if 'chunk' not in dgen_args or dgen_args['chunk'] == '-1':
            dgen_args['chunk'] = str(random.randint(1, 200))

        defaults = {
            'maxPunches': lambda: str(random.randint(1, 50)),
            'maxVblks': lambda: str(random.randint(100, 1000)),
            'punchAmount': lambda: str(random.randint(51, 100)),
            'punchesPer': lambda: "0",
            'maxPunchSize': lambda: str(random.randint(1, 1024)),
            'seed': lambda: str(random.randint(1, 100)),
            'vbAmount': lambda: str(random.randint(1000, 10000)),
            'vblkPer': lambda: str(random.randint(1, 20)),
            'blockSize': lambda: str(random.randint(1, 32)),
            'blockSizeMax': lambda: str(random.randint(1, 32)),
            'startVblk': lambda: "0",
            'strideWidth': lambda: str(random.randint(1, 50)),
            'numOfSet': lambda: str(random.randint(1, 10)),
        }

        for key, random_val in defaults.items():
            if key not in dgen_args:
                dgen_args[key] = random_val()
    
        return dgen_args, dbicount, is_prev_snapshot

    def set_vals_from_json(self, dgen_args):
        dbi_path = get_dir_path(self.cluster_params, DBI_DIR)
        dbicount = "0"
        is_prev_snapshot = False
        
        if dbi_path != None:
            entries = os.listdir(dbi_path)
            chunk_no = dgen_args["chunk"]
            if chunk_no not in entries:
                dbi_path = None
            else:
                json_data = load_parameters_from_json(f"{dbi_path}/dataVal/{dgen_args['chunk']}/dummy_generator.json")
                dgen_args["vdev"] = str(json_data['Vdev'])
                dgen_args["seqStart"] = str(json_data['SeqEnd'] + 1)
                dbicount = str(json_data['TMinDbiFileForForceGC'])
                is_prev_snapshot = bool(json_data['CurIterSnapShot'])

        return dgen_args, dbicount, is_prev_snapshot

    def add_params_to_cmd(self, commands, dgen_args):
        for cmd in commands:
            if 'vdev' in dgen_args and dgen_args['vdev'] != '':
                cmd.extend(['-vdev', dgen_args['vdev']])
            if 'punchwholechunk' in dgen_args and dgen_args['punchwholechunk'] != '':
                cmd.extend(["-pc", str(dgen_args["punchwholechunk"])])
            if 'strideWidth' in dgen_args and dgen_args['strideWidth'] != '':
                cmd.extend(["-sw", dgen_args["strideWidth"]])
            if 'overlapSeq' in dgen_args and dgen_args['overlapSeq'] != '' and \
            'numOfSet' in dgen_args and dgen_args['numOfSet'] != '':
                cmd.extend(["-se", dgen_args["overlapSeq"], "-ts", dgen_args["numOfSet"]])
            if dgen_args.get("targetEntry") in [True, "true"]:
                cmd.extend(["-te=true"]) 
        return commands, dgen_args


    def run_dummy_generator(self, command):
        print("command", command)
        try:
            result = subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")

    def generate_data(self, dgen_args, params):
        """
        Generate dummy dbi/dbo files using dummy generator utility.

        Args:
            dgen_args (dict): dictionary containing arguments for data generation utility (e.g., max punches, chunk, seed).
            params (dict): Configuration parameters, including:
                - 'is_random' (bool): Flag to determine if dummy generator params should be randomly generated.
                - 'total_chunks' (int): total number of chunks for which dummy data needs to be generated.
                - 'remove_files' (bool): Whether to remove generated files after uploading to s3.

        Returns:
            str: returns the chunk identifier if random data generation is enabled.
        """
        bin_path = f'{self.bin_dir}/dummyData'
        s3_config = f'{self.bin_dir}/s3.config.example'
        path = f"{self.base_path}/{DBI_DIR}"

        # Create the new directory
        if not os.path.exists(path):
            # Create the directory path
            try:
                os.makedirs(path, mode=0o777)
            except Exception as e:
                print(f"An error occurred while creating '{path}': {e}")

        dbicount = "0"
        if params['is_random']:
            dgen_args, dbicount, is_prev_snapshot = self.generate_random_values(dgen_args)
        else:
            dgen_args, dbicount, is_prev_snapshot = self.set_vals_from_json(dgen_args)

        commands = []
        for chunk in range(1, params['total_chunks'] + 1):
            if params['total_chunks'] > 1:
                dgen_args['chunk'] = str(chunk)
            command = [bin_path, "-c", dgen_args['chunk'], "-mp", dgen_args['maxPunches'], "-mv", dgen_args['maxVblks'], 
                       "-p", path, "-pa", dgen_args['punchAmount'], "-pp", dgen_args['punchesPer'], "-ps", dgen_args['maxPunchSize'], 
                       "-seed", dgen_args['seed'], "-ss", dgen_args['seqStart'], "-t", dgen_args['genType'], "-va", dgen_args['vbAmount'], 
                       "-l", "5", "-vp", dgen_args['vblkPer'], "-bs", dgen_args['blockSize'], "-bsm", dgen_args['blockSizeMax'], 
                       "-vs", dgen_args['startVblk'], "-s3config", s3_config, "-s3log", self.s3_upload_log, "-dbic", dbicount]
            commands.append(command)

        commands, dgen_args = self.add_params_to_cmd(commands, dgen_args)

        if params['is_random']:
            for cmd in commands:
                cmd.extend(['-b', S3_BUCKET])
                if 'dbiWithPunches' in dgen_args:
                    cmd.extend(['-e', dgen_args['dbiWithPunches']])
                if params.get("remove_files") in [True, "true"]: cmd.append('-r=true')

        for cmd in commands:
            if 'snapshot' in dgen_args:
                cmd.append('-s=true')
            
            if is_prev_snapshot:
                cmd.append('-sp=true')
                
        with Pool(processes = params['total_chunks']) as pool:
            results = pool.map(self.run_dummy_generator, commands)

        if params['is_random']:
            return dgen_args['chunk']

class data_validator:
    def __init__(self, cluster_params):
        self.cluster_params = cluster_params
        self.bin_dir =  os.getenv('NIOVA_BIN_PATH')
        self.base_path = f"{cluster_params['base_dir']}/{cluster_params['raft_uuid']}"
        self.s3_config = f'{self.bin_dir}/s3.config.example'
        self.data_validate_log = f"{self.base_path}/dataValidateResult"

    def validate_data(self, chunk, has_snapshot):
        """
        Validates a dbi/dbo data using data validator utility.

        Args:
            chunk (str): chunk to be validated.

        Raises:
            RuntimeError: If the validation process fails (non-zero exit code).
        """
        bin_path = f'{self.bin_dir}/dataValidator'
        dbi_path = get_dir_path(self.cluster_params, DBI_DIR)
        dv_path = f"{self.base_path}/dv-downloaded-obj"
        if dbi_path != None:
            json_data = load_parameters_from_json(f"{dbi_path}/dataVal/{chunk}/dummy_generator.json")
            vdev = str(json_data['Vdev'])
            
        commands = [bin_path, '-d', dv_path, '-c', chunk, '-v', vdev, '-s3config', self.s3_config, '-b', S3_BUCKET, '-l', self.data_validate_log, '-ll', '4']
        
        if has_snapshot:
            commands.append("-s=true")

        print("cmd: ", commands)
        process = subprocess.Popen(commands)

        # Wait for the process to finish and get the exit code
        exit_code = process.wait()

        # Check if the process finished successfully (exit code 0)
        if exit_code == 0:
            f"Process completed successfully."
        else:
            error_message = f"Process failed with exit code {exit_code}."
            raise RuntimeError(error_message)
    
    def validate_s3_disk_data(self, params):
        """
        Validates disk data with dbi/dbo data using s3 data validator utility.

        Args:
            params (dict): A dictionary of parameters required for validation, including:
                - 'nisd_uuid': NISD uuid.
                - 'ublk_uuid': ublk device uuid.
                - 'device_path': disk device path to be validated.

        Raises:
            subprocess.CalledProcessError: If the external validation process fails.
        """
        bin_path = f'{self.bin_dir}/s3DataValidator'
        log_dir = f'{self.base_path }/s3DV' 
        nisd_cmdintf_path = "/tmp/.niova/%s" % params['nisd_uuid']  
        # Ensure log directory exists
        create_dir(log_dir)    
        cmd = ["sudo", bin_path, '-v', params['ublk_uuid'], '-c', self.s3_config, '-p', log_dir, '-b', S3_BUCKET, '-d', params['device_path'], '-nisdP', nisd_cmdintf_path]
        print(f"s3 dv cmd {cmd}")
        try:
            result = subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            raise e 

class LookupModule(LookupBase):
    def run(self,terms,**kwargs):
        operation = terms[0]
        cluster_params = kwargs['variables']['ClusterParams']

        if operation == "generator":
            dgen_args = terms[1]
            params = terms[2]
            data = data_generator(cluster_params)
            chunk = data.generate_data(dgen_args, params)
            return [chunk]
        
        elif operation == "validator":
            chunk = terms[1]
            has_snapshot = terms[2] if len(terms) > 2 else False
            dv = data_validator(cluster_params)
            dv.validate_data(chunk, has_snapshot)
            return []
        
        elif operation == "s3_disk_validator":
            params = terms[1]
            dv = data_validator(cluster_params)
            dv.validate_s3_disk_data(params)
            return []
