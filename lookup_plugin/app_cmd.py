from ansible.plugins.lookup import LookupBase
import sys
import json
import termios
import os
import time
import subprocess
from inotifypath import *
from constants import NIOVA_LIBEXEC_DIR

def initialize_logger(cluster_params):
    log_path = "%s/%s/%s.log" % (cluster_params['base_dir'],
                                 cluster_params['raft_uuid'],
                                 cluster_params['raft_uuid'])

    logging.basicConfig(filename=log_path, filemode='a',
                        level=logging.DEBUG,
                        format='%(asctime)s [%(filename)s:%(lineno)d] %(message)s')

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
        #Get lookup parameter values[cmd,client_uuid]
        cmd = terms[0]
        client_uuid = terms[1]
        cluster_params = kwargs['variables']['ClusterParams']

        #export NIOVA_THREAD_COUNT
        os.environ['NIOVA_THREAD_COUNT'] = cluster_params['nthreads']

        #Initialize the logger
        initialize_logger(cluster_params)

        raft_uuid = cluster_params['raft_uuid']
        app_name = cluster_params['app_type']
        recipe_conf = load_recipe_op_config(cluster_params)
        base_dir =  recipe_conf['raft_config']['base_dir_path']

        #Get operation and file_name from cmd
        opcode,fname = cmd.split("#")[0],cmd.split("#")[-1]
        temp_file = "%s/%s_log_Pmdb_%s_%s.txt" % (base_dir, app_name, "client", client_uuid)
        fp = open(temp_file, "w")

        #Prepare path for executables.
        binary_dir = os.getenv('NIOVA_BIN_PATH')
        if app_name == "covid":
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "covid_app_client")
        else:
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "foodpalaceappclient")

        ctlsvc_path = "%s/configs" % base_dir

        get_process_type = ''
        lookout_uuid = ''

        inotifyobj = InotifyPath(base_dir, True, get_process_type, lookout_uuid)
        inotifyobj.export_ctlsvc_path(ctlsvc_path)

        #start client process and pass the cmd.
        process_popen = subprocess.Popen([bin_path,'-r', raft_uuid,'-u',client_uuid,'-l', base_dir,'-c',cmd],
                                             stdout = fp, stderr = fp)

        #Wait till output json file created
        counter = 0
        timeout = 2500
        client_json = "%s/%s/%s.json" % (cluster_params['base_dir'],cluster_params['raft_uuid'],fname)
        while True:
            if os.path.exists(client_json):
                try:
                    with open(client_json, "r", encoding="utf-8") as json_file:
                        if len(json_file.readlines()) != 0:
                                json_file.seek(0)
                                request = json.load(json_file)
                except:
                    return [{"status":-1,"msg":"Invalid json format in output file"}]
                break
            else:
                #Wait, fail at max count
                counter += 1
                time.sleep(0.5)
                if counter == timeout:
                    return [{"status":-1,"msg":"Timeout checking for output file"}]

        #Output parsing
        try:
            if "Read" in request['Operation']:
                return [{"status":0,"response":request['Data']}]
            elif "Write" in request['Operation']:
                return [{"status":0,"response":request['Data']}]
        except:
            return [{"status":0,"response":request['LeaderUUID']}]
