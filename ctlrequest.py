import os, logging, re
import subprocess
import time as time_global
from basicio import BasicIO
from genericcmd import GenericCmds
from inotifypath import inotify_input_base
from os import path
from func_timeout import func_timeout, FunctionTimedOut

def ctl_req_create_cmdfile_and_copy(ctlreqobj, operation, cmd, where):
    basicioobj = BasicIO()
    genericcmdobj = GenericCmds()

    o_base = os.path.basename(ctlreqobj.output_fpath)
    i_base = os.path.basename(ctlreqobj.input_fpath)

    # Prepare cmd_string as per the operation (GET or APPLY)
    if operation == "lookup":
        cmd_str = "GET %s\nOUTFILE /%s\n" % (cmd, o_base)
    else:
        cmd_str = "APPLY %s\nWHERE %s\nOUTFILE /%s\n" % (cmd, where, o_base)

    # Before copying the cmdfile, remove the output file if it already exits.
    if os.path.exists(ctlreqobj.output_fpath):
        genericcmdobj.remove_file(ctlreqobj.output_fpath)

    # Write the cmd to tmpfile.
    tmp_file = "/tmp/%s" % i_base

    fd = basicioobj.open_file(tmp_file)
    if fd == None:
        return -1

    rc = basicioobj.write_file(fd, cmd_str)
    if rc == -1:
        return rc

    rc = basicioobj.close_file(fd)
    if rc == -1:
        return rc

    # Create input_fpath directory if does not exist already.
    if not os.path.exists(os.path.dirname(ctlreqobj.input_fpath)):
        genericcmdobj.make_dir(os.path.dirname(ctlreqobj.input_fpath))

    rc = genericcmdobj.move_file(tmp_file, ctlreqobj.input_fpath)
    if rc == -1:
        return rc

    return rc


class CtlRequest:

    input_fpath = ""
    output_fpath = ""
    operation = ""
    cmd = ""
    where = ""
    error = 0

    def __init__(self, inotifyobj, operation, cmd, where, peer_uuid, app_uuid, input_base, fname):

        self.operation = operation
        self.cmd = cmd
        self.where = where
        self.peer_uuid = peer_uuid
        # export the shared init path
        if input_base == inotify_input_base.SHARED_INIT:
            inotifyobj.export_init_path(peer_uuid)

        self.input_fpath = inotifyobj.prepare_input_output_path(peer_uuid,
                                                                fname, True,
                                                                input_base,
                                                                app_uuid)

        self.output_fpath = inotifyobj.prepare_input_output_path(peer_uuid,
                                                                 fname, False,
                                                                 input_base,
                                                                 app_uuid)


    def Apply(self):
        logging.warning("APPLY operation=%s cmd=%s ipath=%s", self.operation, self.cmd, self.input_fpath)
        '''
        Store the return code inside object only so the caller can check for
        the error later.
        '''
        self.error = ctl_req_create_cmdfile_and_copy(self, self.operation, self.cmd, self.where)
        if self.error != 0:
            logging.error("Failed to create ctl req object error: %d" % self.Error())
            exit()
        return self

    def Apply_and_Wait(self, can_fail):
        '''
        To Apply the cmd and Wait for outfile
        Paramter "can_fail" is added so that
        if can_fail is True and timeout occurs,recipe should not terminate and
        if can_fail is False and timeout occurs, recipe will get aborted.
        retry apply_and_wait for 5 times before exiting the recipe with error.
        '''
        retry = 0
        while retry < 5:
            self.Apply()

            # Wait for outfile creation
            logging.warning("calling wait for outfile")
            rc = self.Wait_for_outfile(can_fail)
            if rc == 0:
                break

            if can_fail:
                logging.error("Outfile is not generated, but calling function expects this to fail")
                rc = 0
                break

            retry += 1

        if rc == 1:
            #Aborting the execution as apply failed
            logging.error("Apply_and_Wait() failed")
            exit()

        return self

    def check_for_outfile_creation(self):
        '''
        Check if outfile is created in a loop.
        '''
        while(1):
            if path.exists(self.output_fpath) == True:
                logging.info("Outfile is created :%s" % self.output_fpath)
                break
            time_global.sleep(0.005)

    def Wait_for_outfile(self, can_fail):
        '''
        Wait for outfile creation.
        Timeout is added to wait for outfile creation till the specified time.
        '''
        rc = 0
        try:
            func_timeout(60, self.check_for_outfile_creation, args=())
        except FunctionTimedOut:
            if can_fail == False:
                logging.error("Error : timeout occur for outfile creation : %s" % self.output_fpath)
                rc = 1

        return rc

    def Error(self):
        return self.error

    def delete_files(self):
        genericcmdobj = GenericCmds()

        logging.warning("Destroying ctl obj")
        if os.path.exists(self.input_fpath):
            logging.warning("Removing file: %s" % self.input_fpath)
            genericcmdobj.remove_file(self.input_fpath)

        if os.path.exists(self.output_fpath):
            logging.warning("Removing file: %s" % self.output_fpath)
            genericcmdobj.remove_file(self.output_fpath)

