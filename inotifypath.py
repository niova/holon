import os, logging, time
import subprocess
from genericcmd import GenericCmds
from enum import Enum

class inotify_input_base:
    SHARED_INIT = 1
    PRIVATE_INIT = 2
    REGULAR = 3

class InotifyPath:
    base_dir_path = ''
    inotify_path = ''
    inotify_init_path = ''
    inotify_is_base_path = ''
    '''
    Constructor:
    Purpose: Initialisation
    Parameters: @inotify_path: INOTIFY directory path.
                @inotify_is_base_path: Is this NIOVA_INOTIFY_BASE_PATH
                path? (True/False)
    '''
    def __init__(self, base_dir_path, inotify_is_base_path, get_process_type, lookout_uuid):
        if get_process_type == "nisd" and lookout_uuid != "":
            logging.warning("Process type is nisd")
            self.inotify_path = "%s/niova_lookout/%s" % (base_dir_path, lookout_uuid)
        elif get_process_type == "nisd" and lookout_uuid == "":
            self.inotify_path = "%s/nisd-interface" % base_dir_path
            if os.path.exists(self.inotify_path):
                logging.info("file already exist")
            else:
                os.mkdir(self.inotify_path)
        else:
            logging.warning("Process type is pmdb")
            self.inotify_path = "%s/ctl-interface" % base_dir_path

        self.inotify_shared_init_path = "%s/init" % base_dir_path
        self.inotify_is_base_path = inotify_is_base_path
        # Create inotify and init directories
        genericcmdobj = GenericCmds()
        genericcmdobj.make_dir(self.inotify_path)
        genericcmdobj.make_dir(self.inotify_shared_init_path)

        # export the inotify path
        if inotify_is_base_path:
            os.environ['NIOVA_INOTIFY_BASE_PATH'] = self.inotify_path
            logging.warning("exporting NIOVA_INOTIFY_BASE_PATH=%s",
                            os.environ['NIOVA_INOTIFY_BASE_PATH'])
        else:
            os.environ['NIOVA_INOTIFY_PATH'] = self.inotify_path
            logging.warning("exporting NIOVA_INOTIFY_PATH=%s",
                            os.environ['NIOVA_INOTIFY_PATH'])


    '''
    Method: init path initialization
    Purpose: export init path
    Parameters: @init_path: Init directory path
    '''
    def export_init_path(self, peer_uuid):

        genericcmdobj = GenericCmds()

        '''
        if shared_path is true, use the shared init path.
        else use the init directory path inside inotify/peer_uuid/init
        '''
        init_path = self.inotify_shared_init_path

        os.environ['NIOVA_CTL_INTERFACE_INIT_PATH'] = init_path
        logging.warning("exporting NIOVA_CTL_INTERFACE_INIT_PATH=%s",
                        os.environ['NIOVA_CTL_INTERFACE_INIT_PATH'])

    '''
    Method : To export ctl -svc path
    '''
    def export_ctlsvc_path(self, ctl_svc_path):

        '''
        if shared_path is true, use the shared init path.
        else use the init directory path inside inotify/peer_uuid/init
        '''

        os.environ['NIOVA_LOCAL_CTL_SVC_DIR'] = ctl_svc_path
        logging.warning("exporting NIOVA_LOCAL_CTL_SVC_DIR=%s",
                        os.environ['NIOVA_LOCAL_CTL_SVC_DIR'])


    '''
    method: prepare input/output path.
    purpose: Prepare the absolute path for input/output files for specific
    peer_uuid and app_uuid
    '''
    def prepare_input_output_path(self, peer_uuid, base_fname, input_dir,
                             input_base, app_uuid):
        dir_name = "output"

        if input_dir:
            dir_name = "input"
            if input_base == inotify_input_base.SHARED_INIT:
                # The init file should get created inside shared init directory
                fpath = "%s/%s" % (self.inotify_shared_init_path, base_fname)
            elif input_base == inotify_input_base.PRIVATE_INIT:
                fpath = "%s/%s/init/%s" % (self.inotify_path, peer_uuid, base_fname)
            else: # input_base = REGULAR
                fpath = "%s/%s/%s/%s" % (self.inotify_path, peer_uuid, dir_name, base_fname)
            logging.info("Input File path:%s", fpath)
        else:
            fpath = "%s/%s/%s/%s" % (self.inotify_path, peer_uuid, dir_name, base_fname)
            logging.info("Output File Path:%s", fpath)

        return fpath

