"""
Big data cluster management for Spark and Flink.

This module provides functionality to configure, start, stop, and monitor
big data clusters (Spark and Flink) running on SLURM-managed resources.
"""

from __future__ import annotations

import getpass
import logging
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
import requests
import shlex

import psutil

from .slurm_utils import SlurmManager
from .gui.config import FrameworkConfig
from .monitoring.process import ProcessMonitor
from .utils import run_bash_command

logger = logging.getLogger(__name__)

# =============================================================================
# Big Data Manager
# =============================================================================

class BigDataManager:
    """
    Manager for big data clusters (Spark, Flink).
    """

    def __init__(self, slurm_info: Optional[SlurmManager] = None):
        """Initialize the BigDataManager."""
        self._initialized = False
        # self._message_spacer = DEFAULT_LOG_SPACER
        self._slurm = slurm_info or SlurmManager()
        self._cluster_name = os.uname().nodename

        # Initialize user inputs namespace
        self._user_inputs = SimpleNamespace(
            fw_name="",
            fw_home="",
            master="",
            workers=[],
            master_port="",
            conf_dir="",
            log_dir="",
        )

        # Framework configuration (will be set during initialization)
        self._fw_mapping: Dict[str, Any] = {}

        # Configuration paths (for non-GUI mode)
        self._conf_dest: Optional[str] = None
        self._conf_template: Optional[str] = None
        self._randomize_ports = False

    # =====================================================================
    # Properties
    # =====================================================================

    @property
    def initialized(self) -> bool:
        """Check if the manager has been initialized."""
        return self._initialized

    @property
    def job_id(self) -> str:
        """Get the SLURM job ID."""
        return self._slurm.job_id

    # =====================================================================
    # Initialization
    # =====================================================================

    def initialize_user_input(self, props: Dict[str, Any]) -> None:
        """Initialize the manager with user-provided configuration."""
        self._user_inputs.fw_name = str(props.get("fw_name"))
        self._user_inputs.fw_home = str(props.get("fw_home"))
        self._user_inputs.master = str(props.get("master"))
        self._user_inputs.workers = props.get("workers")
        self._user_inputs.master_port = str(props.get("master_port"))
        self._user_inputs.conf_dir = str(props.get("conf_dir"))
        self._user_inputs.log_dir = str(props.get("log_dir"))
        self._fw_mapping = props.get("fw_mapping")
        self._initialized = True

    
    # =====================================================================
    # Configuration Getters
    # =====================================================================

    def _get_worker_hosts(self) -> List[str]:
        """Get the list of worker hosts."""
        return list(self._user_inputs.workers)

    def _get_master_host(self) -> str:
        """Get the master host."""
        return self._user_inputs.master

    def _get_master_port(self) -> str:
        """Get the master port."""
        return self._user_inputs.master_port

    def get_conf_dir(self) -> str:
        """Get the configuration directory."""
        return self._user_inputs.conf_dir

    def _get_log_dir(self) -> str:
        """Get the log directory."""
        return self._user_inputs.log_dir

    def get_cluster_log_file(self) -> str:
        """Get the cluster log file path."""
        return f"{self._get_log_dir()}/cluster_log"

    def get_fw_cluster_processes(self, all_procs: bool = False) -> Tuple[dict, ...]:
        """Get framework-specific process names."""
        if not self._fw_mapping:
            return ()

        fw_config = self._fw_mapping.get(self._user_inputs.fw_name.upper(), {})

        master_proc = fw_config.proc_master
        worker_proc = fw_config.proc_worker

        if all_procs:
            other_procs = fw_config.proc_other
            # return (master_proc, worker_proc, *other_procs)
            try:
                return [*other_procs]
            except:
                return []

        return (master_proc, worker_proc)

    # =====================================================================
    # Cluster Status
    # =====================================================================

    def is_cluster_up(self) -> bool:
        """Check if the cluster is running."""
        if not self._initialized:
            return False

        processes = self.get_fw_cluster_processes()

        if len(processes) < 2:
            return False

        master_proc, _ = processes[0], processes[1]
        master_proc_patt = master_proc.get("pattern")
        # worker_proc_patt = worker_proc.get("pattern")
        current_user = getpass.getuser()
        expected_workers = len(self._get_worker_hosts())

        found_master = False

        for proc in psutil.process_iter(["username", "cmdline"]):
            try:
                if proc.info["username"] != current_user:
                    continue

                cmdline = proc.info["cmdline"] or []
                if not cmdline:
                    continue

                cmdline_str = " ".join(cmdline)

                # Log for debugging
                if master_proc_patt in cmdline_str :
                    logger.debug(f"Checking PID {proc.pid}: {cmdline_str}")

                if master_proc_patt in cmdline_str:
                    found_master = True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue


        workers_up, worker_count, err_msg = self._verify_cluster_workers()
        logger.debug(f"Worker Up: {workers_up}, Worker count:{worker_count}, Error Msg: {err_msg}")

        logger.debug(
            f"Status: Master {'UP' if found_master else 'DOWN'}, "
            f"Workers: {worker_count}/{expected_workers} {'UP' if workers_up else 'DOWN'}"
        )

        return found_master and workers_up

    def wait_for_cluster_init(self, timeout: int = 30) -> bool:
        """
        Wait for cluster to initialize.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if cluster initialized within timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.is_cluster_up():
                return True
            time.sleep(1)
        return False

    def wait_for_cluster_stop(self, timeout: int = 30) -> bool:
        """
        Wait for cluster to stop.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if cluster stopped within timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not self.is_cluster_up():
                return True
            time.sleep(1)
        return False

    # =====================================================================
    # Cluster Control
    # =====================================================================

    def start_cluster(self) -> bool:
        """Start the big data cluster."""

        if not self._initialized:
            logger.error("BigDataManager must be initialized before setup_config")
            return 1

        # Stop existing cluster if running
        if self.is_cluster_up():
            logger.info(f"{self._user_inputs.fw_name} cluster already running - stopping first")
            self.stop_cluster()

        logger.info(f"Starting {self._user_inputs.fw_name} cluster")

        fw_config : FrameworkConfig = self._fw_mapping.get(self._user_inputs.fw_name.upper(), {})
        log_path = self.get_cluster_log_file()
        fw_name = self._user_inputs.fw_name.upper()
        conf_dir = shlex.quote(os.environ[f"{fw_name}_CONF_DIR"])
        full_cmd = f"{conf_dir}/cmd.sh start {conf_dir} > {log_path} 2>&1"
        logger.debug(f"Running: {full_cmd}")
        result = run_bash_command(full_cmd, shell=True)

        if result.returncode != 0:
            logger.error(f"Failed to start cluster: {result.stderr}")
            return False

        logger.debug(result.stdout)

        if self.wait_for_cluster_init(timeout=60):
            logger.info(f"{self._user_inputs.fw_name} cluster started successfully")
            return True

        logger.error(f"Failed to start {self._user_inputs.fw_name} cluster")
        return False

    def stop_cluster(self) -> bool:
        """Stop the big data cluster."""
        if not self._initialized:
            logger.error("Manager must be initialized before stopping cluster")
            return 1

        logger.info(f"Stopping {self._user_inputs.fw_name} cluster")
        fw_name = self._user_inputs.fw_name.upper()
        log_path = shlex.quote(self.get_cluster_log_file())
        conf_dir = shlex.quote(os.environ[f"{fw_name}_CONF_DIR"])

        try:
            log_path = self.get_cluster_log_file()
            full_cmd = f"{conf_dir}/cmd.sh stop {conf_dir} > {log_path} 2>&1"
            run_bash_command(f"nohup {full_cmd} >> {log_path} 2>&1", shell=True)

            if self.wait_for_cluster_stop(timeout=30):
                logger.info(f"{self._user_inputs.fw_name} cluster stopped successfully")
                return True
            else:
                logger.warning(
                    f"Graceful stop timed out - attempting process cleanup"
                )
                self.cleanup_cluster()
                return False

        except Exception as e:
            logger.error(f"Failed to stop cluster: {e}")
            return False

    def cleanup_cluster(self) -> None:
        """Forcefully terminate all cluster-related processes."""
        required_procs = list(self.get_fw_cluster_processes(all_procs=True))
        required_procs_list = [i['pattern'] for i in required_procs]
        if not required_procs_list or all(not p for p in required_procs_list):
            logger.error(f"No process names defined for {self._user_inputs.fw_name}")
            return
        found_procs = self._find_cluster_processes(required_procs_list)
        if not found_procs:
            logger.info(f"No running processes found for {self._user_inputs.fw_name}")
            return
        logger.info(f"Terminating {len(found_procs)} {self._user_inputs.fw_name} processes")
        self._terminate_processes(found_procs)

    def _find_cluster_processes(
        self, required_procs: List[str]
    ) -> List[Tuple[int, str, str]]:
        """Find processes matching the required process names."""
        found = []
        current_user = getpass.getuser()

        for proc in psutil.process_iter(["username", "pid", "name", "cmdline"]):
            if proc.info["username"] != current_user:
                continue

            cmdline = proc.info["cmdline"] or []
            if not cmdline:
                continue

            try:
                cmdline_str = " ".join(cmdline)
                if any(req in cmdline_str for req in required_procs if req):
                    found.append((proc.pid, proc.info["name"], cmdline_str))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        return found

    def _terminate_processes(
        self, processes: List[Tuple[int, str, str]]
    ) -> None:
        """Terminate the given processes."""
        proc_objects = []

        for pid, name, cmdline in processes:
            logger.info(f"Terminating PID {pid} ({name}): {cmdline[:50]}...")
            try:
                proc = psutil.Process(pid)
                proc.terminate()
                proc_objects.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                logger.error(f"Failed to terminate PID {pid}: {e}")

        # Wait for processes to exit
        if proc_objects:
            gone, alive = psutil.wait_procs(proc_objects, timeout=5)
            for p in alive:
                logger.warning(f"Process {p.pid} did not exit - hard killing")
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    continue

        logger.info(f"Cleanup complete for {self._user_inputs.fw_name}")

    def _verify_cluster_workers(self) -> tuple[bool, int, Exception | str]:
        """
        Checks if all workers listed in a local text file are active in the cluster.
        Currently only number of workers are counted without matching the exact hostname.
        """

        framework = self._user_inputs.fw_name.lower()

        worker_list = self._get_worker_hosts()
        if worker_list is not None and len(worker_list) < 1:
            return False, 0, "no workers configured"

        expected_hosts = set()
        for worker_i in worker_list:
            expected_hosts.add(worker_i)

        if not expected_hosts:
            return False, 0, "no workers configured"

        #port = self._get_master_port()
        host = self._get_master_host()
        if framework == "spark":
            port = 8080
            url = f"http://{host}:{port}/json/"
        elif framework == "flink":
            port = 8081
            url = f"http://{host}:{port}/taskmanagers"
        else:
            return False, 0, "no workers configured"


        active_hosts = set()
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            if framework == "spark":
                workers = data.get("workers", [])
                for w in workers:
                    if w.get("state") == "ALIVE":
                        active_hosts.add(w.get("host").strip())

            elif framework == "flink":
                active_hosts = data.get("taskmanagers", [])

            # Evaluate if all expected hosts exist in the active set
            return len(expected_hosts) == len(active_hosts), len(active_hosts), ""

        except requests.exceptions.RequestException as e:
            return False, len(active_hosts), e

    # =====================================================================
    # Metrics
    # =====================================================================

    def show_metrics(self) -> None:
        """Display cluster metrics using ProcessMonitor."""
        processes = self.get_fw_cluster_processes(all_procs=True)
        monitor = ProcessMonitor(processes)
        monitor.show()
