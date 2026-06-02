from ansible.plugins.lookup import LookupBase
import json, re
import os, time
import dpath.util
import subprocess
from genericcmd import *
from basicio import *
from raftconfig import *
from inotifypath import *
from ctlrequest import *

'''
Send the ctlrequest cmd to the peer.
This will create the ctlrequest python object to apply
the cmd on the given peer-uuid.
'''
def niova_ctlreq_cmd_send(recipe_conf, ctlreq_dict, peer_uuid, get_process_type, lookout_uuid, nisd_uuid=None):
    wait_for_ofile = True
    copy_to = "input"
    operation = ctlreq_dict['operation']
    cmd = ctlreq_dict['cmd']
    where = ctlreq_dict['where']
    recipe_name = ctlreq_dict['recipe_name']
    stage = ctlreq_dict.get('stage', '')
    peerno = "peer%s" % peer_uuid
    input_base = inotify_input_base.REGULAR

    # Get the peer_uuid from the raft json dictionary
    base_dir =  recipe_conf['raft_config']['base_dir_path']
    genericcmdobj = GenericCmds()
    app_uuid = genericcmdobj.generate_uuid()

    if get_process_type == "nisd":
        base_dir = f"/tmp/.niova/{nisd_uuid}"

    inotifyobj = InotifyPath(base_dir, True, get_process_type, lookout_uuid)

    # For idle_on cmd , input_base would be PRIVATE_INIT.
    if 'copy_to' in ctlreq_dict:
       copy_to = ctlreq_dict['copy_to']

    if copy_to == "init":
        input_base = inotify_input_base.PRIVATE_INIT

    if 'wait_for_ofile' in ctlreq_dict:
        wait_for_ofile = ctlreq_dict['wait_for_ofile']

    '''
    Prepare the file name for input/output files from recipe_name and
    stage name:
    for example: recipe_name-stage_name.unique_app_uuid
    '''

    filecounter = recipe_conf['raft_config']['file_counter']
    timestamp = str(round(time.time() * 1000))
    fname = "%04d.%s.%s-%s" % (filecounter,timestamp, recipe_name, stage)
    filecounter = filecounter + 1
    recipe_conf['raft_config']['file_counter'] = filecounter

    # Prepare the ctlreq object
    if wait_for_ofile == False:
        get_process_type = ""
        lookout_uuid = ""
        ctlreqobj = CtlRequest(inotifyobj, operation, cmd, where, peer_uuid, app_uuid,
                input_base, fname).Apply()
    else:
        ctlreqobj = CtlRequest(inotifyobj, operation, cmd, where, peer_uuid, app_uuid,
                            input_base, fname).Apply_and_Wait(False)

    ctlreq_dict_list = []
    ctlreq_dict_list.append(ctlreqobj.__dict__)

    # Add the ctlreq to the raft json directionary
    if not recipe_name in recipe_conf:
        recipe_conf[recipe_name] = {}
    if not peerno in recipe_conf[recipe_name]:
        recipe_conf[recipe_name][peerno] = {}
    if not stage in recipe_conf[recipe_name][peerno]:
        recipe_conf[recipe_name][peerno][stage] = ctlreq_dict_list
    else:
        ctl_list = recipe_conf[recipe_name][peerno][stage]
        recipe_conf[recipe_name][peerno][stage] = ctl_list + ctlreq_dict_list

    genericcmdobj.recipe_json_dump(recipe_conf)
    return ctlreqobj.__dict__


'''
Lookup the raft key(s) in the output JSON file and return
the values in the dictionary format for recipes to read it.
'''
def niova_raft_lookup_values(ctlreq_dict, raft_key_list):

    raft_values_dict = {}
    out_fpath = ctlreq_dict['output_fpath']

    # Read the output file and lookup for the raft key value
    with open(out_fpath, 'r') as json_file:
        raft_dict = json.load(json_file)

    '''
    Lookup each key from the raft_key_list.
    The output would be stored in another dictionary with key as
    last two keys from the complete key path.
    '''
    for key in raft_key_list:
        value = dpath.util.values(raft_dict, key)
        if len(value) == 0:
            value.append("null")
        elif value[0] == "":
            value[0] = "null"
        output_key = "/%s/%s" % (os.path.basename(os.path.dirname(key)), os.path.basename(key))
        raft_values_dict[output_key] = value[0]

    return raft_values_dict


'''
Lookup the raft keys for the given peer, first by sending
the ctlrequest to the peer and then reading the values
from the output JSON file.
'''
def niova_raft_lookup_ctlreq(recipe_conf, ctlreq_cmd_dict, peer_uuid, getProcess_type, lookout_uuid, nisd_uuid=None):
    raft_keys = ctlreq_cmd_dict['lookup_key']
    if isinstance(raft_keys, list):
        ctlreq_cmd_dict['cmd'] = "/.*/.*/.*/.*"
    else:
        single_key = re.sub('/0/', '/', raft_keys)
        ctlreq_cmd_dict['cmd'] = single_key
        '''
        If single key is passed by user, niova_raft_lookup_values expects
        the keys are added in a list. So add the key in the list.
        '''
        raft_keys = [raft_keys]

    '''
    Send the ctlrequest cmd to get the values of the raft keys.
    '''
    ctlreq_obj_dict = niova_ctlreq_cmd_send(recipe_conf, ctlreq_cmd_dict, peer_uuid, getProcess_type, lookout_uuid, nisd_uuid)

    '''
    If lookup was called with wait_for_ofile = False, the recipe is not
    looking for output.
    Example could be, pausing the process and doing lookup which is expected
    to be failed.
    '''
    if 'wait_for_ofile' in ctlreq_cmd_dict and ctlreq_cmd_dict['wait_for_ofile'] == False:
        return {}

    '''
    Get the values from the output file for these specific raft_keys
    '''
    raft_values = niova_raft_lookup_values(ctlreq_obj_dict, raft_keys)

    logging.info(raft_values)
    return raft_values

'''
Load the recipe json file and get the file contents as dictionary.
'''
def niova_get_recipe_json_data(cluster_params):
	# Prepare the path for the recipe json file
    raft_json_fpath = "%s/%s/%s.json" % (cluster_params['base_dir'], cluster_params['raft_uuid'], cluster_params['raft_uuid'])
	# Load the recipe json file.
    recipe_conf = {}
    if os.path.exists(raft_json_fpath):
        with open(raft_json_fpath, "r+", encoding="utf-8") as json_file:
            recipe_conf = json.load(json_file)

    return recipe_conf


'''
Initialize the logger for ctlrequest cmd.
'''
def niova_ctlrequest_init_logger(cluster_params):

	# Prepare the log path
    log_path = "%s/%s/%s.log" % (cluster_params['base_dir'], cluster_params['raft_uuid'], cluster_params['raft_uuid'])
	# Initialize logger
    logging.basicConfig(filename=log_path, filemode='a', level=logging.DEBUG, format='%(asctime)s [%(filename)s:%(lineno)d] %(message)s')


'''
Prepare ctlrequest cmd parameter dictionary with passed parameters
'''
def niova_ctlrequest_get_cmdline_input_dict(global_args, local_args):
    ctlreq_cmd_dict = {}
	# Get the values from ansibles global cache
    ctlreq_cmd_dict['recipe_name'] = global_args['variables']['recipe_name']
    if 'stage' in global_args.get('variables', {}):
        ctlreq_cmd_dict['stage'] = global_args['variables']['stage']

    # If wait_for_ofile is passed explicitly.
    if 'wait_for_ofile' in global_args['variables']:
        ctlreq_cmd_dict['wait_for_ofile'] = global_args['variables']['wait_for_ofile']

	# cmdline parameters to the ctlrequest lookup plugin.
    ctlreq_cmd_dict['operation'] = local_args[0]

    if not isinstance(local_args[1], list):
        ctlreq_cmd_dict['peer_uuid_list'] = [local_args[1]]
    else:
        ctlreq_cmd_dict['peer_uuid_list'] = local_args[1]


	# Now get the parameters specific to the operation.
    if ctlreq_cmd_dict['operation'] == "apply_cmd":
        ctlreq_cmd_dict['cmd'] = local_args[2]
        ctlreq_cmd_dict['where'] = local_args[3]
        ctlreq_cmd_dict['raft_key'] = "None"
        if 'copy_to' in global_args['variables']:
            ctlreq_cmd_dict['copy_to'] = global_args['variables']['copy_to']


    elif ctlreq_cmd_dict['operation'] == "lookup":
        ctlreq_cmd_dict['lookup_key'] = local_args[2]
        ctlreq_cmd_dict['cmd'] = None
        ctlreq_cmd_dict['where'] = None
        '''
        If this lookup is gonna run for number of iterations.
        '''
        ctlreq_cmd_dict['iter_info'] = None
        if len(local_args) > 3 and isinstance(local_args[3], dict):
            ctlreq_cmd_dict['iter_info'] = local_args[3]

    return ctlreq_cmd_dict

'''
Main function for the niova_ctlrequest lookup plugin.
'''
class LookupModule(LookupBase):
    def run(self, terms, **kwargs):
        '''
        Get the variables from ansible global cache and cmdline arguments
        for this lookup plugin.
        '''
        if (len(terms) >= 5):
            getProcess_type = "nisd"
        else:
            getProcess_type = "pmdb"

        ctlreq_cmd_dict = niova_ctlrequest_get_cmdline_input_dict(kwargs, terms)
        '''
        Initialize the logger for ctlrequest logs.
        '''
        cluster_params = kwargs['variables']['ClusterParams']

        niova_ctlrequest_init_logger(cluster_params)

        '''
        Get the recipe json contents.
        '''
        recipe_conf = niova_get_recipe_json_data(cluster_params)
        
        logging.warning(
             "Ctlrequest for recipe: %s, stage: %s, operation: %s, peer_uuid_list: %s" %
                (
                    ctlreq_cmd_dict.get('recipe_name', ''),
                    ctlreq_cmd_dict.get('stage', ''),
                    ctlreq_cmd_dict.get('operation', ''),
                    ctlreq_cmd_dict.get('peer_uuid_list', '')
                )
            )
   
        result_array = []
        logging.warning("operation: %s" % ctlreq_cmd_dict['operation'])
        for peer_uuid in ctlreq_cmd_dict['peer_uuid_list']:
            '''
            Operation is to simply apply the ctlrequest cmd to the peer.
            '''
            if ctlreq_cmd_dict['operation'] == "apply_cmd":
                if getProcess_type == "nisd":
                    lookout_uuid = terms[5]
                else:
                    lookout_uuid = ""
                logging.warning("Apply cmd: %s on peer-uuid_list: %s" % (ctlreq_cmd_dict['cmd'], ctlreq_cmd_dict['peer_uuid_list']))
                result = niova_ctlreq_cmd_send(recipe_conf, ctlreq_cmd_dict, peer_uuid, getProcess_type, lookout_uuid)

            elif ctlreq_cmd_dict['operation'] == "lookup":
                if getProcess_type == "nisd":
                    nisd_uuid = terms[1]
                    lookout_uuid = terms[4]
                else:
                     lookout_uuid = ""
                '''
                    Operation to send the ctlrequest cmd first and then read the values for
                    the given raft keys from the output JSON file.
                '''
                logging.warning("Lookup for key: %s on peers: %s" % (ctlreq_cmd_dict['lookup_key'], ctlreq_cmd_dict['peer_uuid_list']))

                iter_info = None
                if ctlreq_cmd_dict['iter_info'] != None:
                    iter_info = ctlreq_cmd_dict['iter_info']

                result = []
                iter_cnt = 1
                sleep_sec = 0
                '''
                If this lookup is gonna run for number of iterations.
                This iteration option will send the ctlrequest cmd to the peer
                specific number of times and could sleep between the iterations if
                sleep time is specified by the recipe author.
                Note: The result would be added in a list for all the iterations.
                '''

                if iter_info != None:
                    iter_cnt = int(iter_info['iter'])
                    sleep_sec = int(iter_info['sleep_after_cmd'])

                for i in range(iter_cnt):
                    logging.warning("Sending lookup for itr: %d" % i)
                    values = niova_raft_lookup_ctlreq(recipe_conf, ctlreq_cmd_dict, peer_uuid, getProcess_type, lookout_uuid, nisd_uuid)
                    time.sleep(sleep_sec)
                    result.append(values)

                '''
                If only one element in present in the result array, return only
                first element rather than returning array of array even if
                single result in present for this cmd.
                '''
                if iter_cnt == 1:
                    result =  result[0]
                    logging.warning("Only one element in the array: %s" % result)

            result_array.append(result)

        if len(result_array)  == 1:
            result_array = result_array[0]

        logging.warning("Values of ctlreq cmd: %s" % result_array)
        return [result_array]
