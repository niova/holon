import os
import subprocess
import sys
import hashlib
import time
from datetime import datetime
from pathlib import Path
from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

class LookupModule(LookupBase):
    def run(self, terms, **kwargs):  
        cluster_params = kwargs['variables']['ClusterParams']

        base_dir = cluster_params['base_dir']
        raft_uuid = cluster_params['raft_uuid']
        config_dir = f"{base_dir}/{raft_uuid}/configs"
        holon_dir = os.getenv('HOLON_LIBS')

        script_path = "%s/verify_kv_crc.sh" % holon_dir

        cmd = [script_path, config_dir]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False
        )

        if result.returncode != 0:
            raise AnsibleError(
                f"verify_kv_crc.sh failed\n"
                f"STDOUT:\n{result.stdout}\n"
                f"STDERR:\n{result.stderr}"
            )

        # Return stdout so it can be used in Ansible
        return [result.stdout.strip()]      