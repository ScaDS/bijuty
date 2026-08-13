"""
Environment setup utilities for the big data framework GUI.

This module provides methods for configuring framework environments,
writing configuration files, and initializing the BigDataManager.
It is intended to be used as a mixin with GUIMain.
"""

from __future__ import annotations

import os
import re
import shlex
import time
import traceback
from typing import Any, Dict
import shutil
import sys
import logging

import pyflink

from .config import FRAMEWORK_REGISTRY
from ..utils import run_bash_command

logger = logging.getLogger(__name__)


class GUIEnvSetup:
    """Class providing environment setup functionality for GUIMain."""

    def _set_fw_config_template(self) -> None:
        fw_name = self.get_selected_framework_name()
        if self.is_default_config_template():
            fw_conf_template = FRAMEWORK_REGISTRY[fw_name.upper()].default_template
        else:
            fw_conf_template = self.get_selected_config_template()
        os.environ[f"{fw_name}_CONF_TEMPLATE"] = fw_conf_template

    def _initialize_framework_config(self) -> None:
        """Execute the bash command to set up the framework."""
        self._set_framework_home()
        self._set_fw_config_template()
        self._create_conf_dest_dir()

        fw_name = shlex.quote(self.get_selected_framework_name().lower())
        template = shlex.quote(self.get_selected_config_template())
        dest = shlex.quote(
            os.path.dirname(self.get_selected_config_destination())
        )
        script_dir = shlex.quote(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

        bash_command = (
            f"cd {script_dir} && source ./framework-configure.sh "
            f"--framework {fw_name} "
            f"--template {template} "
            f"--destination {dest} "
            f"&& env | grep {fw_name} || true"
        )
        start_time = time.time()
        self._log(f"Initializing configuration at: {dest}")
        self._log(bash_command, "debug")
        res= run_bash_command(bash_command, shell=True, timeout=6000)
        logger.info(res.stdout)
        logger.info(res.stderr)

        elapsed = time.time() - start_time
        self._log(f"Time elapsed for config init: {elapsed:.2f} seconds", "debug")

        if res.returncode != 0:
            self._log(f"Bash script failed with exit code {res.returncode}.\nError: {res.stderr}", "error")
            raise RuntimeError(f"Bash script failed with exit code {res.returncode}.\nError: {res.stderr}")

        # Set environment variables after configuration initialization into current kernel
        for line in res.stdout.splitlines():
            if "=" in line:
                key, value = line.strip().split("=", 1)
                os.environ[str(key).strip()] = str(value).strip()

    def _update_spark_environment(self) -> None:
        """Update Spark environment configuration."""
        if self.get_selected_framework_name() != "SPARK":
            return

        try:
            self._update_env_file()
            self._update_worker_file()

            self._log(f"Environment updated for {self.get_selected_framework_name()}!", "info")
            self.is_config_set = True
            self._toggle_cluster_buttons(start_disabled=False)

        except Exception as e:
            self._handle_setup_error(e)

    def _build_spark_env_updates(self) -> Dict[str, str]:
        """Build Spark environment variable updates."""
        return {
            "SPARK_MASTER_HOST": self.get_selected_master_host(),
            "SPARK_WORKER_CORES": str(self.get_selected_worker_cpu()),
            "SPARK_WORKER_MEMORY": self.get_selected_worker_memory(),
            "SPARK_EXECUTOR_CORES": str(self.get_selected_executor_cpu()),
            "SPARK_EXECUTOR_MEMORY": self.get_selected_executor_memory(),
            "SPARK_DRIVER_MEMORY": self.get_selected_driver_memory(),
            "SPARK_LOCAL_DIRS": self.get_selected_local_dirs(),
            "SPARK_WORKER_DIR": self.get_selected_worker_dir(),
            "SPARK_CONF_DIR": self.get_selected_config_destination(),
            "SPARK_LOG_DIR": self.get_selected_log_dir(),
            "SPARK_PID_DIR": self.get_selected_pid_dir(),
            "SPARK_MASTER_PORT": self.get_selected_master_port(),
            "LD_LIBRARY_PATH": os.environ.get("LD_LIBRARY_PATH", ""),  # important for slurm modules
        }

    def _update_env_file(self) -> None:
        """Update the Spark environment file."""

        # Setting spark env file
        file_path = os.path.join(
            self.get_selected_config_destination(), "spark-env.sh"
        )

        # Build spark env
        env_updates = self._build_spark_env_updates()

        with open(file_path, "r") as f:
            content = f.read()

        for var_name, new_value in env_updates.items():
            escaped_var = re.escape(var_name)
            replacement = f'export {var_name}="{new_value}"'

            active_pattern = rf"^\s*export\s+\b{escaped_var}\b.*$"
            comment_pattern = rf"^[\s#\-]+(?:export\s+)?\b{escaped_var}\b.*$"

            if re.search(active_pattern, content, flags=re.MULTILINE):
                content = re.sub(active_pattern, replacement, content, flags=re.MULTILINE)
            elif re.search(comment_pattern, content, flags=re.MULTILINE):
                content = re.sub(comment_pattern, replacement, content, count=1, flags=re.MULTILINE)
            else:
                if content and not content.endswith("\n"):
                    content += "\n"
                content += f"{replacement}\n"

            os.environ[str(var_name).strip()] = str(new_value).strip()

        with open(file_path, "w") as f:
            f.write(content)

        # Setting log4j file
        file_path = os.path.join(
            self.get_selected_config_destination(), "log4j2.properties"
        )
        with open(file_path, "r") as f:
            content = f.read().replace("FRAMEWORK_LOG_DIR", self.get_selected_log_dir())
        with open(file_path, "w") as f:
            f.write(content)

    def _update_flink_environment(self) -> None:
        """Update Flink environment configuration."""
        if self.get_selected_framework_name() != "FLINK":
            return

        try:
            self._update_flink_conf_file()
            # self._update_flink_masters_file()
            self._update_worker_file()

            self._log(f"Environment updated for {self.get_selected_framework_name()}!", "info")
            self.is_config_set = True
            self._toggle_cluster_buttons(start_disabled=False)

            # # This is additional for pyflink
            # os.environ['FLINK_PROPERTIES'] = f"""
            #     jobmanager.rpc.address: {self.get_selected_master_host()}
            #     jobmanager.rpc.port: {self.get_selected_master_port()}
            #     rest.address: {self.get_selected_master_host()}
            #     rest.port: 8081
            # """

            self._ensure_pyflink_jar_in_lib()

        except Exception as e:
            self._handle_setup_error(e)

    def _build_flink_env_updates(self) -> Dict[str, str]:
        """Build Spark environment variable updates."""
        return {
            "FLINK_MASTER_HOSTNAME": self.get_selected_master_host(),
            "FLINK_MASTER_PORT": self.get_selected_master_port(),
            "FLINK_MEM_MASTER": self.get_selected_driver_memory(),
            "FLINK_MEM_PER_WORKER": self.get_selected_worker_memory(),
            "FLINK_SLOTS_PER_TASKMANAGER": str(self.get_selected_executor_cpu()),
            "FLINK_CONF_DIR": self.get_selected_config_destination(),
            "FLINK_PARALLELISM": str(self.get_selected_executor_cpu()),
            "FLINK_LOG_DIR": self.get_selected_log_dir(),
            "JAVA_HOME": os.environ.get("JAVA_HOME","None"),
            "LD_LIBRARY_PATH": os.environ.get("LD_LIBRARY_PATH", ""),  # important for slurm modules
        }

    def _update_flink_conf_file(self) -> None:
        """Update the Flink configuration file (flink-conf.yaml)."""

        conf_path = self.get_selected_config_destination()
        conf_files = [ "flink-conf.yaml", "masters", "meta.conf", "config.yaml" ]

        # Build env
        try:
            java_home = os.environ.get("JAVA_HOME")
        except Exception as e:
            logger.error("No JAVA_HOME value isfound in the environment. Please set it in the environment.")

        flink_env_updates = self._build_flink_env_updates()

        for file_i in conf_files:
            file_i_path = os.path.join(conf_path,file_i)
            with open(file_i_path, "r") as f:
                content = f.read()
            for placeholder, value in flink_env_updates.items():
                try:
                    content = content.replace(placeholder, value)
                except:
                    pass
            with open(file_i_path, "w") as f:
                f.write(content)

    # def _update_flink_masters_file(self) -> None:
    #     """Update the Flink masters file."""
    #     file_path = os.path.join(
    #         self.get_selected_config_destination(), "masters"
    #     )
    #     with open(file_path, "r") as f:
    #         content = f.read()

    #     content = content.replace("FLINK_MASTER_HOSTNAME", self.get_selected_master_host())

    #     with open(file_path, "w") as f:
    #         f.write(content)

    def _update_worker_file(self) -> None:
        """Update the framework worker file."""
        worker_file_path = os.path.join(
            self.get_selected_config_destination(),
            FRAMEWORK_REGISTRY[self.get_selected_framework_name()].worker_file,
        )
        with open(worker_file_path, "w") as f:
            for node in self.get_selected_workers():
                f.write(f"{node}\n")

    def _ensure_pyflink_jar_in_lib(self):
        """
        Ensures the correct versioned flink-python jar is present in $FLINK_HOME/lib.
        Operates silently unless an error occurs.
        """
        flink_home = self.get_selected_framework_home()
        if not flink_home:
            logger.error("Error: FLINK_HOME environment variable is not set.")
            return False

        flink_lib_dir = os.path.join(flink_home, 'lib')
        if not os.path.exists(flink_lib_dir):
            logger.error(f"Error: Flink lib directory does not exist at: {flink_lib_dir}")
            return False

        pyflink_dir = os.path.dirname(pyflink.__file__)
        pyflink_opt_dir = os.path.join(pyflink_dir, 'opt')
        if not os.path.exists(pyflink_opt_dir):
            logger.error(f"Error: PyFlink 'opt' directory not found at: {pyflink_opt_dir}")
            return False

        jar_files = [f for f in os.listdir(pyflink_opt_dir) if f.startswith('flink-python') and f.endswith('.jar')]
        if not jar_files:
            logger.error("Error: Could not find any flink-python*.jar in pyflink/opt folder.")
            return False

        jar_name = jar_files[0]
        source_jar_path = os.path.join(pyflink_opt_dir, jar_name)
        target_jar_path = os.path.join(flink_lib_dir, jar_name)

        if not os.path.exists(target_jar_path):
            try:
                shutil.copy2(source_jar_path, target_jar_path)
            except PermissionError:
                logger.error(f"Error: Insufficient permissions to write to {flink_lib_dir}.")
                return False
            except Exception as e:
                logger.error(f"Error: Failed to copy jar: {e}")
                return False

        return True


    def _handle_setup_error(self, error: Exception) -> None:
        """Handle setup errors."""
        self._log(f"FATAL ERROR: {str(error)}")
        tb = traceback.format_exc()
        self._log(tb, msg_type="error")
        self.is_config_set = False
        self._toggle_cluster_buttons(start_disabled=False, stop_disabled=True)

    def _set_framework_home(self) -> None:
        os.environ[f"{self.get_selected_framework_name().upper()}_HOME"] = self.get_selected_framework_home()

    def _create_conf_dest_dir(self) -> None:
        os.makedirs(os.path.dirname(self.get_selected_config_destination()), exist_ok=True)

    def _initialize_big_data_manager(self) -> None:
        """Initialize the BigDataManager with user input."""
        self.bdm.initialize_user_input({
            "fw_name": self.get_selected_framework_name(),
            "fw_home": self.get_selected_framework_home(),
            "master": self.get_selected_master_host(),
            "workers": self.get_selected_workers(),
            "master_port": self.get_selected_master_port(),
            "conf_dir": self.get_selected_config_destination(),
            "log_dir": self.get_selected_log_dir(),
            "fw_mapping": FRAMEWORK_REGISTRY,
        })

    def _update_environment(self,fw_name):
        if fw_name.upper() == "SPARK":
            self._update_spark_environment()
        if fw_name.upper() == "FLINK":
            self._update_flink_environment()

