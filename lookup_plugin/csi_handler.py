# lookup_plugin/csi_handler.py

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError

import logging
import os
import shlex
import shutil
import subprocess


CSI_SANITY_REPO = "https://github.com/kubernetes-csi/csi-test.git"
CSI_SANITY_DIR = "csi-test/cmd/csi-sanity"


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


def run_command(command, logger, cwd=None, timeout=300):
    """
    Run a command and return its output.
    Raises AnsibleError if the command fails.
    """

    cmd_str = " ".join(
        shlex.quote(str(x)) for x in command
    )

    logger.info("Executing command: %s", cmd_str)
    logger.info("Command timeout: %s seconds", timeout)

    try: 
        result = subprocess.run(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=timeout,
        )

        output = result.stdout or ""

        if output:
            logger.info("Command output:\n%s", output)

        if result.returncode != 0:
            raise AnsibleError(
                "Command failed with return code %s: %s\n"
                "Command output:\n%s"
                % (
                    result.returncode,
                    cmd_str,
                    output if output else "<no output>",
                )
            )

        return output
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""

        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")

        message = (
            "Command timed out after %s seconds: %s\n"
            "Command output before timeout:\n%s"
            % (
                timeout,
                cmd_str,
                output if output else "<no output captured>",
            )
        )

        logger.error("%s", message)
        raise AnsibleError(message) from exc

    except OSError as exc:
        raise AnsibleError(
            "Unable to execute command %s: %s"
            % (cmd_str, exc)
        ) from exc

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
        
        list_command = [
            "ls",
            "-la",
            "/tmp/csi/csi-test",
        ]

        run_command(
            list_command,
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

    workspace = os.getenv("NIOVA_WORKSPACE")

    #
    # Binary can either be supplied by the recipe
    # or derived from the workspace.
    #
    binary_path = os.path.join(
            workspace,
            CSI_SANITY_DIR,
            "csi-sanity"
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
        if service_type == "controller":

            skip_tests = (
                "Node Service|"
                "ModifyVolume|"
                "ExpandVolume|"
                "Snapshot"
            )

        elif service_type == "node":

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
            "--ginkgo.v",
            "--ginkgo.show-node-events",
        ]

        logger.info(
            "Running CSI sanity %s service",
            service_type
        )

        logger.info("CSI sanity command: %s", " ".join(command))
        timeout = int(input_values.get("timeout", 300))
        output = run_command(
            command,
            logger,
            timeout=timeout,
        )

        logger.info(
            "CSI sanity %s service output:\n%s",
            service_type,
            output,
        )

        logger.info(
            "CSI sanity %s service command finished",
            service_type,
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

def run_csi_ginkgo(cluster_params, input_values):
    """
    Run CSI Go tests with Ginkgo v2.

    Expected input_values:
    {
        "test_path": "/path/to/niova-block-csi",
        "test_dir": "./test/e2e/integrity",
        "focus": "Backend restart during IO",
        "output_file": "csi_integrity_test",
        "timeout": 600,
        "env": {
            "NIOVA_LOG_LEVEL": "0"
        }
    }

    Returns a one-element list containing structured test metadata.
    """

    # Allow the repository path to be passed explicitly. If omitted, use
    # NIOVA_CSI_PATH, then fall back to the existing workspace convention.
    test_path = input_values.get("test_path")
    if not test_path:
        test_path = os.getenv("NIOVA_CSI_PATH")

    if not test_path:
        workspace = os.getenv("NIOVA_WORKSPACE")
        if workspace:
            test_path = os.path.join(workspace, "niova-block-csi")

    test_dir = input_values.get("test_dir")
    focus = input_values.get("focus") or input_values.get("test_name")
    output_file = input_values.get("output_file", "csi_ginkgo_test")
    timeout = input_values.get("timeout", 300)
    extra_args = input_values.get("extra_args", [])

    if not test_path or not os.path.isdir(test_path):
        raise AnsibleError(
            "CSI test_path does not exist or is not a directory: %s"
            % test_path
        )

    if not test_dir:
        raise AnsibleError("test_dir is required, e.g. ./test/security")

    if not focus:
        raise AnsibleError("focus or test_name is required")

    try:
        timeout = int(timeout)
    except (TypeError, ValueError):
        raise AnsibleError("timeout must be an integer")

    if timeout <= 0:
        raise AnsibleError("timeout must be greater than zero")

    if not isinstance(extra_args, list) or not all(
        isinstance(arg, str) for arg in extra_args
    ):
        raise AnsibleError("extra_args must be a list of strings")

    # Keep the log under <test_path>/logs unless output_file is absolute.
    log_dir = os.path.join(test_path, "logs")
    if os.path.isabs(output_file):
        log_file = output_file
    else:
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, output_file)

    if not log_file.endswith(".log"):
        log_file += ".log"

    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    logger = initialize_logger(log_file)

    # Defaults can be overridden by input_values["env"].
    env = os.environ.copy()
    csi_env = {
        "NIOVA_BLOCK_UBLK_UNIFIED": "0",
        "NIOVA_LOG_LEVEL": "0",
        "NIOVA_GOSSIP_PATH": "/etc/niova/gossipNodes",
        "NIOVA_BLOCK_MDSVC_GET_CHUNKS_LIMIT": "256",
        "NIOVA_GOSSIP_KEY": "dummy",
        "NIOVA_BLOCK_CP_AUTH_USERNAME": "admin",
        "NIOVA_BLOCK_CP_AUTH_SECRET": "admin",
        "LD_LIBRARY_PATH": "/usr/local/lib",
    }

    supplied_env = input_values.get("env", {})
    if not isinstance(supplied_env, dict):
        raise AnsibleError("env must be a dictionary")

    for key, value in supplied_env.items():
        if not isinstance(key, str):
            raise AnsibleError("Environment variable names must be strings")
        if not isinstance(value, (str, int, float)):
            raise AnsibleError(
                "Environment variable values must be strings or numbers"
            )
        csi_env[key] = str(value)

    env.update(csi_env)

    # Pass focus as one argument so expressions containing spaces work.
    command = [
        "go",
        "run",
        "github.com/onsi/ginkgo/v2/ginkgo",
        "-v",
        "--focus=" + focus,
    ] + extra_args + [test_dir]

    logger.info("Running CSI Ginkgo test")
    logger.info("Working directory: %s", test_path)
    logger.info(
        "Executing command: %s",
        " ".join(shlex.quote(str(x)) for x in command)
    )
    logger.info("Test directory: %s", test_dir)
    logger.info("Focus: %s", focus)

    timed_out = False
    return_code = None

    try:
        result = subprocess.run(
            command,
            cwd=test_path,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=timeout,
        )
        output = result.stdout or ""
        return_code = result.returncode

    except subprocess.TimeoutExpired as exc:
        timed_out = True
        return_code = 124
        captured = exc.stdout or ""
        if isinstance(captured, bytes):
            captured = captured.decode("utf-8", errors="replace")
        output = captured + (
            "\nERROR: Ginkgo command timed out after %s seconds.\n" % timeout
        )

    except OSError as exc:
        raise AnsibleError("Unable to execute CSI Ginkgo tests: %s" % exc)

    # Write the complete command output to the dedicated test log.
    try:
        with open(log_file, "w", encoding="utf-8") as log_fp:
            log_fp.write("Command: %s\n" % " ".join(
                shlex.quote(str(x)) for x in command
            ))
            log_fp.write("Working directory: %s\n\n" % test_path)
            log_fp.write(output)
    except OSError as exc:
        raise AnsibleError(
            "Unable to write CSI Ginkgo log %s: %s" % (log_file, exc)
        )

    logger.info("CSI Ginkgo return code: %s", return_code)
    logger.info("CSI Ginkgo log: %s", log_file)

    test_result = {
        "test_path": test_path,
        "test_dir": test_dir,
        "test_name": input_values.get("test_name"),
        "focus": focus,
        "return_code": return_code,
        "timed_out": timed_out,
        "log_file": log_file,
        "command": command,
        "parsed_results": {
            "output": output,
        },
    }

    return [test_result]


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
        # Run CSI Go unit/e2e tests with Ginkgo v2
        #
        elif process_type == "run_csi_ginkgo":

            return run_csi_ginkgo(
                cluster_params,
                input_values
            )

        #
        # Unknown operation
        #
        raise AnsibleError(
            "Unknown csi_handler operation: %s"
            % process_type
        )
