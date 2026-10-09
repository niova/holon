# lookup_plugin/csi_handler.py

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import logging
import os
import shlex
import shutil
import subprocess


CSI_SANITY_REPO = "https://github.com/kubernetes-csi/csi-test.git"
CSI_SANITY_DIR = "csi-test"


def initialize_logger(log_file):
    logger = logging.getLogger("csi_handler")

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def run_command(command, logger, cwd=None):
    """
    Run a command and return its output.

    Raises AnsibleError if the command fails.
    """

    logger.info(
        "Executing command: %s",
        " ".join(shlex.quote(str(x)) for x in command)
    )

    result = subprocess.run(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )

    if result.stdout:
        logger.info(result.stdout)

    if result.returncode != 0:
        raise AnsibleError(
            "Command failed with return code %s: %s"
            % (
                result.returncode,
                " ".join(
                    shlex.quote(str(x))
                    for x in command
                ),
            )
        )

    return result.stdout


def build_csi_sanity(cluster_params, input_values):
    """
    Clone and build csi-sanity.

    Expected input_values:

    {
        "workspace": "/tmp/csi",
        "branch": "master",
        "repo": "https://github.com/kubernetes-csi/csi-test.git"
    }

    Returns:

        [
            "/tmp/csi/csi-test/csi-sanity"
        ]
    """

    base_dir = cluster_params.get(
        "base_dir",
        "/tmp"
    )

    raft_uuid = cluster_params.get(
        "raft_uuid",
        "default"
    )

    workspace = input_values.get(
        "workspace",
        os.path.join(
            base_dir,
            raft_uuid
        )
    )

    branch = input_values.get(
        "branch",
        "master"
    )

    repo_url = input_values.get(
        "repo",
        CSI_SANITY_REPO
    )

    source_dir = os.path.join(
        workspace,
        CSI_SANITY_DIR
    )

    log_dir = os.path.join(
        workspace,
        "logs"
    )

    os.makedirs(
        log_dir,
        exist_ok=True
    )

    log_file = os.path.join(
        log_dir,
        "csi_sanity_build.log"
    )

    logger = initialize_logger(
        log_file
    )

    try:
        os.makedirs(
            workspace,
            exist_ok=True
        )

        #
        # Remove previous checkout
        #
        if os.path.exists(source_dir):
            logger.info(
                "Removing existing csi-test directory: %s",
                source_dir
            )

            shutil.rmtree(
                source_dir
            )

        #
        # Clone repository
        #
        clone_command = [
            "git",
            "clone",
            "--branch",
            branch,
            "--single-branch",
            repo_url,
            source_dir,
        ]

        run_command(
            clone_command,
            logger
        )

        #
        # Build csi-sanity
        #
        #
        # The csi-test repository contains csi-sanity.
        #
        build_command = [
            "make",
            "-C",
            source_dir,
            "csi-sanity",
        ]

        run_command(
            build_command,
            logger
        )

        #
        # Locate binary
        #
        binary_path = os.path.join(
            source_dir,
            "csi-sanity"
        )

        if not os.path.exists(binary_path):
            raise AnsibleError(
                "csi-sanity binary was not found after build: %s"
                % binary_path
            )

        #
        # Make sure binary is executable
        #
        os.chmod(
            binary_path,
            0o755
        )

        logger.info(
            "csi-sanity successfully built: %s",
            binary_path
        )

        return [
            binary_path
        ]

    except AnsibleError:
        raise

    except Exception as exc:
        logger.exception(
            "Failed to build csi-sanity"
        )

        raise AnsibleError(
            "Failed to build csi-sanity: %s"
            % str(exc)
        )


def run_csi_sanity(
    cluster_params,
    input_values,
    service_type
):
    """
    Run csi-sanity.

    service_type:
        node
        controller
    """

    base_dir = cluster_params.get(
        "base_dir",
        "/tmp"
    )

    raft_uuid = cluster_params.get(
        "raft_uuid",
        "default"
    )

    workspace = input_values.get(
        "workspace",
        os.path.join(
            base_dir,
            raft_uuid
        )
    )

    #
    # Binary can either be supplied by the recipe
    # or derived from the workspace.
    #
    binary_path = input_values.get(
        "binary",
        os.path.join(
            workspace,
            CSI_SANITY_DIR,
            "csi-sanity"
        )
    )

    endpoint = input_values.get(
        "endpoint",
        "unix:///var/lib/kubelet/plugins/csi.niova.com/custom.sock"
    )

    log_dir = os.path.join(
        workspace,
        "logs"
    )

    os.makedirs(
        log_dir,
        exist_ok=True
    )

    log_file = os.path.join(
        log_dir,
        "csi_sanity_%s.log" % service_type
    )

    logger = initialize_logger(
        log_file
    )

    try:

        #
        # Verify binary
        #
        if not os.path.exists(binary_path):
            raise AnsibleError(
                "csi-sanity binary not found: %s"
                % binary_path
            )

        #
        # Make sure it is executable
        #
        os.chmod(
            binary_path,
            0o755
        )

        #
        # Select tests to skip
        #
        if service_type == "node":

            skip_tests = (
                "Node Service|"
                "ModifyVolume|"
                "ExpandVolume|"
                "Snapshot"
            )

        elif service_type == "controller":

            skip_tests = (
                "Controller Service|"
                "ModifyVolume|"
                "ExpandVolume|"
                "Snapshot"
            )

        else:
            raise AnsibleError(
                "Unsupported CSI sanity service type: %s"
                % service_type
            )

        #
        # Build command
        #
        command = [
            "sudo",
            binary_path,
            "--csi.endpoint=%s" % endpoint,
            "-ginkgo.skip=%s" % skip_tests,
        ]

        logger.info(
            "Running CSI sanity %s service",
            service_type
        )

        output = run_command(
            command,
            logger
        )

        logger.info(
            "CSI sanity %s service completed successfully",
            service_type
        )

        return [
            output
        ]

    except AnsibleError:
        raise

    except Exception as exc:
        logger.exception(
            "CSI sanity %s service failed",
            service_type
        )

        raise AnsibleError(
            "CSI sanity %s service failed: %s"
            % (
                service_type,
                str(exc)
            )
        )


class LookupModule(LookupBase):

    def run(
        self,
        terms,
        variables=None,
        **kwargs
    ):

        if not terms:
            raise AnsibleError(
                "csi_handler requires an operation"
            )

        #
        # First lookup argument is the operation.
        #
        process_type = terms[0]

        #
        # Second lookup argument is the input dictionary.
        #
        if len(terms) > 1:
            input_values = terms[1]
        else:
            input_values = {}

        #
        # Get Ansible variables.
        #
        if variables is None:
            variables = kwargs.get(
                "variables",
                {}
            )

        #
        # Same pattern as nisd_handler.py
        #
        cluster_params = variables.get(
            "ClusterParams"
        )

        if cluster_params is None:
            raise AnsibleError(
                "ClusterParams is required"
            )

        #
        # Build csi-sanity
        #
        if process_type == "build_csi_sanity":

            return build_csi_sanity(
                cluster_params,
                input_values
            )

        #
        # Run Node Service sanity tests
        #
        elif process_type == "run_csi_sanity_node":

            return run_csi_sanity(
                cluster_params,
                input_values,
                "node"
            )

        #
        # Run Controller Service sanity tests
        #
        elif process_type == "run_csi_sanity_controller":

            return run_csi_sanity(
                cluster_params,
                input_values,
                "controller"
            )

        #
        # Unknown operation
        #
        raise AnsibleError(
            "Unknown csi_handler operation: %s"
            % process_type
        )
