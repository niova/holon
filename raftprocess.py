import signal, subprocess, logging, psutil
import time as time_global
from raftconfig import RaftConfig
import shutil, os
from genericcmd import GenericCmds
from func_timeout import func_timeout, FunctionTimedOut
from constants import NIOVA_LIBEXEC_DIR

def check_for_process_status(pid, process_status):
    '''
    To check process status
    '''
    if process_status == "killed":
        check_if_pid_killed(pid)
    else:
        ps = psutil.Process(pid)
        itr = 0
        while(1):
            if ps.status() == process_status:
                break

            time_global.sleep(0.025)
            #After every 50 iterations, print process status.
            if itr == 50:
                logging.warning("process status %s (expected %s)"% (ps.status(), process_status))
                itr = 0
            itr += 1

def check_if_pid_killed(pid):
    '''
    Check if PID exists or not in a loop
    '''
    if psutil.pid_exists(pid):
        logging.warning("Waiting for process (%d) to be killed" % pid)
        time_global.sleep(0.025)
        check_if_pid_killed(pid)
    else:
        return

def get_executable_path(process_type, app_type, backend_type, binary_dir):

    bin_path = ""
    # Add complete path to app/pumice/raft executable to bin_path
    if app_type == "foodpalace":
        if process_type == "server":
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "foodpalaceappserver")
        else:
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "foodpalaceappclient")

    elif app_type == "covid":
        if process_type == "server":
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "covid_app_server")
        else:
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "covid_app_client")

    elif app_type == "niovakv":
        if process_type == "server":
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "NKV_pmdbServer")
        else:
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "NKV_proxy")

    elif app_type == "controlplane":
        if process_type == "server":
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "CTLPlane_pmdbServer")
        else:
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "CTLPlane_proxy")

    elif app_type == "lease":
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "LeaseApp_pmdbServer")

    elif app_type == "pumicedb":
        if backend_type == "pumicedb":
            if process_type == "server":
                bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "pumice-reference-server")
            else:
                bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "pumice-reference-client")
           

    elif app_type == "raft":
        if process_type == "server":
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "raft-reference-server")
        else:
            bin_path = os.path.join(binary_dir, NIOVA_LIBEXEC_DIR, "raft-reference-client")

    else:
        logging.error("Invalid app type %s " % app_type)
        exit(1)

    return bin_path

def run_process(fp, raft_uuid, peer_uuid, ptype, app_type, bin_path, base_dir, config_path, node_name, coalesced_wr, sync, cluster_params):
    process_popen = {}
    print(bin_path)
    if not os.path.exists(bin_path):
        raise FileNotFoundError(f"Bin path not found {bin_path}")
    # binary_dir = os.getenv('NIOVA_BIN_PATH')
    gossipNodes = "%s/gossipNodes" % base_dir

    if ptype == "server":
        if app_type == "pumicedb"  or app_type == "raft":
            if sync == "0" and coalesced_wr == "1":
                 process_popen = subprocess.Popen([bin_path, '-r',
                                    raft_uuid, '-u', peer_uuid, '-a', '-c'],
                                    stdout = fp, stderr = fp)
            elif sync == "1" and coalesced_wr == "1":
                 process_popen = subprocess.Popen([bin_path, '-r',
                                          raft_uuid, '-u', peer_uuid, '-c'],
                                          stdout = fp, stderr = fp)
            elif sync == "1" and coalesced_wr == "0":
                 process_popen = subprocess.Popen([bin_path, '-r',
                                          raft_uuid, '-u', peer_uuid],
                                          stdout = fp, stderr = fp)
            else:
                 process_popen = subprocess.Popen([bin_path, '-r',
                                          raft_uuid, '-u', peer_uuid, '-a'],
                                          stdout = fp, stderr = fp)
        elif app_type == "controlplane":
            log_path = "%s/%s_pmdbServer.log" % (base_dir, peer_uuid)
            
            if cluster_params['prometheus_support'] == 0: 
                process_popen = subprocess.Popen([bin_path , '-g',  gossipNodes , '-r',
                                    raft_uuid, '-u', peer_uuid, '-l', log_path],
                                    stdout = fp, stderr = fp)
            else:
                process_popen = subprocess.Popen([bin_path , '-g',  gossipNodes , '-r',
                                    raft_uuid, '-u', peer_uuid, '-l' , log_path, '-p', cluster_params['prometheus_support']],
                                    stdout = fp, stderr = fp)

        elif app_type == "niovakv":
            
            log_path = "%s/%s_niovakv_pmdbServer.log" % (base_dir, peer_uuid)
            process_popen = subprocess.Popen([bin_path, '-r',
                                          raft_uuid, '-u', peer_uuid, '-l' ,log_path],
                                          stdout = fp, stderr = fp)

        elif app_type == "covid" or app_type == "foodpalace":
            process_popen = subprocess.Popen([bin_path, '-r',
                                          raft_uuid, '-u', peer_uuid],
                                          stdout = fp, stderr = fp)
        elif app_type == "lease":
            log_path = "%s/%s_lease_pmdbServer.log" % (base_dir, peer_uuid)
            process_popen = subprocess.Popen([bin_path, '-r',
                                          raft_uuid, '-u', peer_uuid, '-l' ,log_path],
                                          stdout = fp, stderr = fp)

    else:
        if app_type == "pumicedb":
            if coalesced_wr == 1:
                process_popen = subprocess.Popen([bin_path, '-r',
                                    raft_uuid, '-u', peer_uuid, '-a'],
                                    stdout = fp, stderr = fp)
            elif sync == "1":
                  process_popen = subprocess.Popen([bin_path, '-r',
                                         raft_uuid, '-u', peer_uuid],
                                         stdout = fp, stderr = fp)
            else:
                  process_popen = subprocess.Popen([bin_path, '-r',
                                                      raft_uuid, '-u', peer_uuid, '-a'],
                                                      stdout = fp, stderr = fp)
        elif app_type == "foodpalace" or app_type == "covid":
            process_popen = subprocess.Popen([bin_path, '-r',
                                    raft_uuid, '-u', peer_uuid, '-l', base_dir],
                                    stdout = fp, stderr = fp)
        elif app_type == "niovakv":
            log_path = "%s/%s_niovakv_server.log" % (base_dir, peer_uuid)
            
            process_popen = subprocess.Popen([bin_path, '-r',
                                    raft_uuid, '-u', peer_uuid,
                                    '-c', gossipNodes, '-n', node_name, '-l', log_path],
                                    stdout = fp, stderr = fp)
        elif app_type == "controlplane":
            log_path = "%s/%s_control_plane_proxy_server.log" % (base_dir, peer_uuid)
            process_popen = subprocess.Popen([bin_path, '-r',
                                    raft_uuid, '-u', peer_uuid, '-pa', gossipNodes,
                                    '-n', node_name, '-l', log_path],
                                    stdout = fp, stderr = fp)
    return process_popen

class RaftProcess:

    process_type = ''
    process_app_type = ''
    process_raft_uuid = ''
    process_uuid = ''
    process_status = ''
    process_backend_type = ''
    process_pid = 0

    '''
        Constructor:
        Purpose: Initialisation
        Parameter:  @cluster_type: Cluster is raft cluster or pumiceDB cluster.
					@uuid: UUID of server or client for which process object is
                            created.
                    @process_type: Type of the process(server or client)
    '''
    def __init__(self, backend_type, raft_uuid, uuid, process_type, app_type):
        self.process_backend_type = backend_type
        self.process_raft_uuid = raft_uuid
        self.process_uuid = uuid
        self.process_pid = 0
        self.process_type = process_type
        self.process_app_type = app_type
    '''
        Method: start_process
        Purpose: Start the process of type process_type
        Parameters: @raftconfobj: RaftConf object.
                    @uuid: UUID of the server or client.
    '''

    def Wait_for_process_status(self, process_status, pid):
        '''
        To wait for change in process status.
        Timeout is added to wait for change in process status till the specified time.
        '''
        rc = 0
        try:
            func_timeout(60, check_for_process_status, args=(pid, process_status))
        except FunctionTimedOut:
                logging.error("Error : timeout occur to change process status to %s" % process_status)
                rc = 1

        #if rc == 1:
        #    exit()

    def start_process(self, base_dir, node_name, coalesced_wr, sync, cluster_params):
        logging.warning("Starting uuid: %s, cluster_type %s" % (self.process_uuid, self.process_backend_type))

        binary_dir = os.getenv('NIOVA_BIN_PATH')

        logging.warning("raft binary path is: %s" % binary_dir)

        app_type = self.process_app_type
        # Otherwise use the default path
        if binary_dir == None:
            binary_dir = "/home/pauln/raft-builds/latest"

        # Add complete path to app/pumice/raft executable to bin_path
        
        try:
            bin_path = get_executable_path(self.process_type, app_type, self.process_backend_type, binary_dir)
        except:
            raise ValueError("Error formatting bin path") 
        '''
        We want to append the output of raft-server log into the recipe log
        adding information about for which peerid the process has started.
        So first get the raft-server init log into temp file, add the prefix
        and then write it to recipe log.

        If its application then we want the logs in the file
        '''
        temp_file = "%s/%s_log_Pmdb_%s_%s.txt" % (base_dir, app_type, self.process_type, self.process_uuid)
        fp = open(temp_file, "w")

        if app_type == "controlplane":
            node_name  = "Node_" + self.process_uuid

        config_path = ""
        process_popen = run_process(fp, self.process_raft_uuid, self.process_uuid,
                                    self.process_type, self.process_app_type, bin_path,
                                    base_dir, config_path, node_name, coalesced_wr, sync, cluster_params)
        #Make sure all the ouput gets flushed to the file before closing it
        os.fsync(fp)
        output_label = "raft-%s.%s" % (self.process_type, self.process_uuid)
        self.process_pid = process_popen.pid

        #To check if process is started
        self.Wait_for_process_status("running", self.process_pid)

        #Check if child process exited with error
        if process_popen.poll() is None:
            self.process_status = "running"
            logging.info("Raft process started successfully")
        else:
            logging.info("Raft process failed to start")
            raise subprocess.SubprocessError(self.process_popen.returncode)

        # Wait till the raft-server output gets written to the temp file
        while(1):
            fsize = os.path.getsize(temp_file)
            if fsize != 0:
                break

        with open(temp_file, "r") as fp:
            Lines = fp.readlines()

        #Print <process_type.peer_index> at the start of raft log messages
        for line in Lines:
            logging.warning("<{}>:{}".format(output_label, line.strip()))



    '''
        Method: pause_process
        Purpose: pause the process by sending sigstop
        Parameters:
    '''
    def pause_process(self, pid):
        self.process_pid = pid
        process_obj = psutil.Process(pid)
        logging.info("pause the process by sending sigstop")
        try:
            process_obj.send_signal(signal.SIGSTOP)
        except subprocess.SubprocessError as e:
            logging.error("Failed to send Stop signal with error: %s" % os.stderror(e.errno))
            return -1

        '''
        To check if process is paused
        '''
        self.Wait_for_process_status("stopped", pid)
        self.process_status = "paused"
        return 0

    '''
        Method: resume_process
        Purpose: resume the process by sending sigcont
        Parameters:
    '''
    def resume_process(self, pid):
        self.process_pid = pid
        process_obj = psutil.Process(pid)
        logging.info("resume the process by sending sigcont")
        try:
            process_obj.send_signal(signal.SIGCONT)
        except subprocess.SubprocessError as e:
            logging.error("Failed to send CONT signal with error: %s" % os.stderror(e.errno))
            return -1

        self.process_status = "running"
        return 0

    '''
        Method: kill_process
        Purpose: kill the process by sending sigterm
        Parameters:
    '''
    def kill_process(self, pid):
        self.process_pid = pid
        process_obj = psutil.Process(pid)
        logging.warning("kill the process(%d) by sending sigterm" % pid)
        try:
            process_obj.send_signal(signal.SIGTERM)
        except subprocess.SubprocessError as e:
            logging.error("Failed to send kill signal with error: %s" % os.stderror(e.errno))
            return -1
        self.Wait_for_process_status("killed", pid)
        self.process_status = "killed"
        return 0

    '''
        Method: kill_child_process
        Purpose: kill the child process by sending sigterm
        Parameters:
    '''

    def kill_child_process(self, pid):
        self.process_pid = pid
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return

        children = parent.children(recursive=True)
        for child in children:
            if child is None:
                pass
            else:
                os.kill(child.pid, signal.SIGTERM)

        #Since we are just killing the child process, set parent process as running
        self.process_status = "running"

        self.Wait_for_process_status("killed", child.pid)

        return 0
