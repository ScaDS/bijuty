"""
Big data cluster management for Spark and Flink.

This module provides functionality to configure, start, stop, and monitor
big data clusters (Spark and Flink) running on SLURM-managed resources.
"""

from __future__ import annotations

import getpass
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import psutil

from .slurm_utils import SlurmManager
from .utils import SimpleLogger, run_bash_command

# =============================================================================
# Constants
# =============================================================================

DEFAULT_LOG_SPACER = "  "
DEFAULT_MASTER_PORT_SPARK = 7077
DEFAULT_MASTER_PORT_FLINK = 8081
PORT_RANDOMIZATION_BASE = 7077


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ClusterConfig:
    """Configuration for a big data cluster."""

    fw_name: str
    master: str
    workers: List[str]
    master_port: str
    conf_dir: str
    log_dir: str
    template: Optional[str] = None
    randomize_ports: bool = False


@dataclass
class ProcessInfo:
    """Information about a running process."""

    pid: int
    name: str
    cmdline: str
    type: str  # 'master', 'worker', 'other'


# =============================================================================
# Exceptions
# =============================================================================

class ClusterConfigurationError(Exception):
    """Raised when cluster configuration fails."""
    pass


class ClusterOperationError(Exception):
    """Raised when a cluster operation (start/stop) fails."""
    pass


class NotInitializedError(Exception):
    """Raised when trying to use BigDataManager before initialization."""
    pass


# =============================================================================
# Utility Functions
# =============================================================================

def parse_path(path: str, resolve_symlinks: bool = True) -> str:
    """
    Parse and normalize a file path.

    Args:
        path: The path to parse
        resolve_symlinks: Whether to resolve symlinks

    Returns:
        Normalized absolute path
    """
    path = os.path.expanduser(path)
    path = os.path.abspath(path)
    if resolve_symlinks:
        path = os.path.realpath(path)
    return path


def find_processes_using_dir(target_dir: str) -> List[Tuple[int, str, str]]:
    """
    Find processes that are using a specific directory.

    Args:
        target_dir: The directory to check

    Returns:
        List of tuples (pid, process_name, reason)
    """
    target_dir = parse_path(target_dir)
    found_processes = []

    for proc in psutil.process_iter(["pid", "name"]):
        try:
            # Check if process CWD is the target directory
            if os.path.abspath(proc.cwd()) == target_dir:
                found_processes.append(
                    (proc.info["pid"], proc.info["name"], "Working Directory")
                )

            # Check if process has open files in the directory
            for file in proc.open_files():
                if file.path.startswith(target_dir):
                    found_processes.append(
                        (proc.info["pid"], proc.info["name"], f"Open File: {file.path}")
                    )
                    break

        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue

    return found_processes


# =============================================================================
# Logger Setup
# =============================================================================

logger = SimpleLogger()


# =============================================================================
# Big Data Manager
# =============================================================================

class BigDataManager:
    """
    Manager for big data clusters (Spark, Flink).

    This class provides functionality to:
    - Configure clusters from templates
    - Start and stop clusters
    - Monitor cluster health
    - Manage cluster processes

    Attributes:
        initialized: Whether the manager has been initialized with user inputs
        user_inputs: Namespace containing cluster configuration
        fw_mapping: Framework configuration mapping
    """

    def __init__(self):
        """Initialize the BigDataManager."""
        self._initialized = False
        self._message_spacer = DEFAULT_LOG_SPACER
        self._slurm = SlurmManager()
        self._cluster_name = os.uname().nodename

        # Initialize user inputs namespace
        self._user_inputs = SimpleNamespace(
            fw_name="",
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
        """
        Initialize the manager with user-provided configuration.

        Args:
            props: Dictionary with keys: fw_name, master, workers, master_port,
                   conf_dir, log_dir, fw_mapping
        """
        self._user_inputs.fw_name = str(props.get("fw_name", ""))
        self._user_inputs.master = str(props.get("master", ""))
        self._user_inputs.workers = props.get("workers", [])
        self._user_inputs.master_port = str(props.get("master_port", ""))
        self._user_inputs.conf_dir = str(props.get("conf_dir", ""))
        self._user_inputs.log_dir = str(props.get("log_dir", ""))
        self._fw_mapping = props.get("fw_mapping", {})
        self._initialized = True

    # =====================================================================
    # Configuration Setup
    # =====================================================================

    def setup_config(
        self,
        conf_dest: Optional[str] = None,
        conf_template: Optional[str] = None,
        randomize_ports: bool = False,
    ) -> None:
        """
        Set up cluster configuration from template.

        Args:
            conf_dest: Configuration destination directory
            conf_template: Path to configuration template
            randomize_ports: Whether to randomize master ports

        Raises:
            NotInitializedError: If manager hasn't been initialized
            ClusterConfigurationError: If configuration setup fails
        """
        if not self._initialized:
            raise NotInitializedError(
                "BigDataManager must be initialized before setup_config"
            )

        self._conf_dest = parse_path(conf_dest) if conf_dest else None
        self._conf_template = parse_path(conf_template) if conf_template else None
        self._randomize_ports = randomize_ports

        if not self._slurm.in_slurm_job:
            logger.warning("Not in a SLURM job - some features may not work")
            return

        self._setup_configuration_directory()
        self._initialize_framework_configuration()

    def _setup_configuration_directory(self) -> None:
        """Set up the configuration directory, archiving existing if needed."""
        # Set default destination if not provided
        if not self._conf_dest:
            self._conf_dest = parse_path(
                f"./cluster-conf-{self._slurm.job_id}"
            )

        # Handle existing configuration directory
        if os.path.isdir(self._conf_dest):
            logger.info(f"Archiving existing configuration: '{self._conf_dest}'")
            self._archive_configuration_directory()
        else:
            logger.info(f"Creating configuration directory: '{self._conf_dest}'")
            os.makedirs(self._conf_dest, exist_ok=True)

    def _archive_configuration_directory(self) -> None:
        """Archive existing configuration and remove original."""
        results = find_processes_using_dir(self._conf_dest)

        if results:
            self._print_process_table(results)

        try:
            archive_name = f"{self._conf_dest}_backup_{time.time_ns()}"
            shutil.make_archive(archive_name, "zip", self._conf_dest)
            shutil.rmtree(self._conf_dest)
            logger.info(f"Archived to {archive_name}.zip and removed original")
        except Exception as e:
            logger.error(f"Failed to archive: {e}")
            if results:
                self._print_process_table(results)
                logger.error("Kill processes using: !kill <process_id>")
            raise ClusterConfigurationError(f"Archive failed: {e}")

    @staticmethod
    def _print_process_table(processes: List[Tuple[int, str, str]]) -> None:
        """Print a formatted table of processes."""
        print(f"{'PID':<10} {'Process Name':<25} {'Reason'}")
        print("-" * 60)
        for pid, name, reason in processes:
            print(f"{pid:<10} {name:<25} {reason}")

    def _initialize_framework_configuration(self) -> None:
        """Initialize framework-specific configuration."""
        fw_name = self._user_inputs.fw_name

        # Get template path
        if not self._conf_template:
            env_var = f"{fw_name}_CONF_TEMPLATE"
            self._conf_template = os.environ.get(env_var)
            if not self._conf_template:
                raise ClusterConfigurationError(f"Template not found in {env_var}")

        self._log_configuration_info()
        self._run_framework_configure_script()

        # Set up framework-specific settings
        if fw_name == "SPARK":
            self._setup_spark_configuration()

    def _log_configuration_info(self) -> None:
        """Log configuration information."""
        fw_name = self._user_inputs.fw_name
        logger.info("Environment configuration initialized:")
        logger.info(f"{self._message_spacer}• Framework:        {fw_name}")
        logger.info(f"{self._message_spacer}• Config template:  {self._conf_template}")
        logger.info(f"{self._message_spacer}• Config target:    {self._conf_dest}")

        if fw_name == "SPARK":
            logger.info(f"{self._message_spacer}• Log directory:    {self._get_log_dir()}")

        # Set environment variables
        os.environ[f"MY_{fw_name}_CONF_DEST"] = self._conf_dest
        os.environ[f"MY_{fw_name}_CONF_TEMPLATE"] = self._conf_template

        if fw_name == "SPARK":
            os.environ["PYSPARK_PYTHON"] = sys.executable

    def _run_framework_configure_script(self) -> None:
        """Run the framework configuration script."""
        fw_lower = self._user_inputs.fw_name.lower()

        fw_conf_cmd = (
            f"source framework-configure.sh "
            f"--framework {fw_lower} "
            f"--template {self._conf_template} "
            f"--destination {self._conf_dest}"
        )

        full_cmd = f"{fw_conf_cmd}; env | grep {self._user_inputs.fw_name}"

        logger.info("Initializing configuration from template")
        result = run_bash_command(full_cmd, shell=True)

        if result.failed:
            logger.error(f"Configuration initialization failed: {result.stderr}")
            raise ClusterConfigurationError(result.stderr)

        # Parse environment variables from output
        for line in result.stdout.strip().split("\n"):
            if "=" in line:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

        # Set configuration directory environment variable
        conf_dir_full = f"{self._conf_dest}/{self._user_inputs.fw_name.lower()}"
        os.environ[f"{self._user_inputs.fw_name}_CONF_DIR"] = conf_dir_full

    def _setup_spark_configuration(self) -> None:
        """Set up Spark-specific configuration."""
        slurm_nodes = self._slurm.get_nodes_list()

        # Determine master port
        default_port = self._fw_mapping.get(self._user_inputs.fw_name.upper(), {}).get(
            "default_master_port", DEFAULT_MASTER_PORT_SPARK
        )

        try:
            master_port = int(self._user_inputs.master_port)
        except (ValueError, TypeError):
            master_port = default_port

        if self._randomize_ports:
            job_id_suffix = self._slurm.job_id[-3:] if len(self._slurm.job_id) >= 3 else self._slurm.job_id
            master_port = int(job_id_suffix) + PORT_RANDOMIZATION_BASE
            os.environ[f"{self._user_inputs.fw_name}_MASTER_PORT"] = str(master_port)

        master_host = slurm_nodes[0] if slurm_nodes else "localhost"
        worker_hosts = slurm_nodes

        logger.info("Cluster topology:")
        logger.info(f"{self._message_spacer}• Master:  {master_host}:{master_port}")
        logger.info(f"{self._message_spacer}• Workers: {', '.join(worker_hosts)}")

        # Configure spark-env.sh
        conf_dir = os.environ[f"{self._user_inputs.fw_name}_CONF_DIR"]
        self._write_spark_env_sh(conf_dir, master_host, master_port)

        # Update spark-submit script
        self._update_spark_submit_script(master_host, master_port)

        # Write workers file
        self._write_workers_file(worker_hosts)

        # Log access information
        self._log_spark_access_info(master_host)

    def _write_spark_env_sh(
        self, conf_dir: str, master_host: str, master_port: int
    ) -> None:
        """Write Spark environment configuration."""
        spark_env_path = Path(conf_dir) / "spark-env.sh"
        ld_path_result = run_bash_command("echo $LD_LIBRARY_PATH", shell=True)
        ld_path = ld_path_result.stdout if ld_path_result.success else ""

        with open(spark_env_path, "a") as f:
            f.write(f"export LD_LIBRARY_PATH={ld_path}\n")
            f.write(f"export {self._user_inputs.fw_name}_MASTER_PORT={master_port}\n")
            f.write(f"export SPARK_MASTER_HOST={master_host}\n")

    def _update_spark_submit_script(self, master_host: str, master_port: int) -> None:
        """Update the spark-submit script with correct master URL."""
        conf_dir = os.environ.get(f"{self._user_inputs.fw_name}_CONF_DIR", "")
        spark_submit_path = Path(conf_dir) / "spark-submit"

        if spark_submit_path.exists():
            cmd = (
                f"sed -i 's!\\(spark://\\)[a-zA-Z0-9]*:[0-9]*!"
                f"\\1{master_host}:{master_port}!' {spark_submit_path}"
            )
            run_bash_command(cmd, shell=True)

    def _write_workers_file(self, worker_hosts: List[str]) -> None:
        """Write the workers configuration file."""
        conf_dir = os.environ.get(f"{self._user_inputs.fw_name}_CONF_DIR", "")
        workers_path = Path(conf_dir) / "workers"

        with open(workers_path, "w") as f:
            for worker in worker_hosts:
                f.write(f"{worker}\n")

    def _log_spark_access_info(self, master_host: str) -> None:
        """Log information for accessing Spark web UIs."""
        cluster_domain = os.uname().nodename
        user = getpass.getuser()

        logger.info(
            "Spark master URL: spark://"
            f"{master_host}:{self._user_inputs.master_port}"
        )
        logger.info("Access Spark GUI using port forwarding:")
        logger.info(
            f"{self._message_spacer}ssh {user}@login1.{cluster_domain} "
            f"-L 4040:{master_host}:4040 "
            f"-L 8080:{master_host}:8080 "
            f"-L 8081:{master_host}:8081"
        )
        logger.info("Then access:")
        logger.info(f"{self._message_spacer}• http://localhost:4040")
        logger.info(f"{self._message_spacer}• http://localhost:8080")
        logger.info(f"{self._message_spacer}• http://localhost:8081")

    # =====================================================================
    # Configuration Getters
    # =====================================================================

    def get_worker_hosts(self) -> List[str]:
        """Get the list of worker hosts."""
        return list(self._user_inputs.workers)

    def get_master_host(self) -> str:
        """Get the master host."""
        return self._user_inputs.master

    def get_master_port(self) -> str:
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

    def get_fw_cluster_processes(self, all_procs: bool = False) -> Tuple[str, ...]:
        """
        Get framework-specific process names.

        Args:
            all_procs: Whether to include all process types

        Returns:
            Tuple of process name strings
        """
        if not self._fw_mapping:
            return ()

        fw_config = self._fw_mapping.get(self._user_inputs.fw_name.upper(), {})

        master_proc = fw_config.proc_master
        worker_proc = fw_config.proc_worker

        if all_procs:
            other_procs = fw_config.proc_other
            return (master_proc, worker_proc, *other_procs)

        return (master_proc, worker_proc)

    # =====================================================================
    # Cluster Status
    # =====================================================================

    def is_cluster_up(self) -> bool:
        """
        Check if the cluster is running.

        Returns:
            True if master and all workers are running
        """
        if not self._initialized:
            return False

        processes = self.get_fw_cluster_processes()
        if len(processes) < 2:
            return False

        master_proc, worker_proc = processes[0], processes[1]
        current_user = getpass.getuser()
        expected_workers = len(self.get_worker_hosts())

        found_master = False
        worker_count = 0

        for proc in psutil.process_iter(["username", "cmdline"]):
            try:
                if proc.info["username"] != current_user:
                    continue

                cmdline = proc.info["cmdline"] or []
                if not cmdline:
                    continue

                cmdline_str = " ".join(cmdline)

                # Log for debugging
                if master_proc in cmdline_str or worker_proc in cmdline_str:
                    logger.debug(f"Checking PID {proc.pid}: {cmdline_str}")

                if master_proc in cmdline_str:
                    found_master = True

                if worker_proc in cmdline_str:
                    for worker in self.get_worker_hosts():
                        if worker in cmdline_str:
                            worker_count += 1
                            break

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        workers_up = worker_count >= expected_workers
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
        """
        Start the big data cluster.

        Returns:
            True if cluster started successfully

        Raises:
            NotInitializedError: If manager hasn't been initialized
            ClusterOperationError: If start operation fails
        """
        if not self._initialized:
            raise NotInitializedError("Manager must be initialized before starting cluster")

        # Stop existing cluster if running
        if self.is_cluster_up():
            logger.info(f"{self._user_inputs.fw_name} cluster already running - stopping first")
            self.stop_cluster()

        logger.info(f"Starting {self._user_inputs.fw_name} cluster")

        fw_config = self._fw_mapping.get(self._user_inputs.fw_name.upper(), {})
        cmd_script = fw_config.get("start_proc_cmd")

        if not cmd_script:
            raise ClusterOperationError(f"Unsupported framework: {self._user_inputs.fw_name}")

        log_path = self.get_cluster_log_file()
        full_cmd = f"{cmd_script} > {log_path} 2>&1"

        logger.debug(f"Running: {full_cmd}")
        result = run_bash_command(full_cmd, shell=True)

        if result.failed:
            logger.error(f"Failed to start cluster: {result.stderr}")
            return False

        logger.debug(result.stdout)

        if self.wait_for_cluster_init(timeout=1000):
            logger.info(f"{self._user_inputs.fw_name} cluster started successfully")
            return True

        logger.error(f"Failed to start {self._user_inputs.fw_name} cluster")
        return False

    def stop_cluster(self) -> bool:
        """
        Stop the big data cluster.

        Returns:
            True if cluster stopped successfully

        Raises:
            NotInitializedError: If manager hasn't been initialized
        """
        if not self._initialized:
            raise NotInitializedError("Manager must be initialized before stopping cluster")

        logger.info(f"Stopping {self._user_inputs.fw_name} cluster")

        fw_config = self._fw_mapping.get(self._user_inputs.fw_name.upper(), {})
        cmd_script = fw_config.get("stop_proc_cmd")

        if not cmd_script:
            logger.error(f"No stop command for framework: {self._user_inputs.fw_name}")
            return False

        try:
            log_path = self.get_cluster_log_file()
            run_bash_command(f"nohup {cmd_script} >> {log_path} 2>&1", shell=True)

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
        """
        Forcefully terminate all cluster-related processes.

        This is a last resort when graceful shutdown fails.
        """
        required_procs = list(self.get_fw_cluster_processes(all_procs=True))

        if not required_procs or all(not p for p in required_procs):
            logger.error(f"No process names defined for {self._user_inputs.fw_name}")
            return

        found_procs = self._find_cluster_processes(required_procs)

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

    # =====================================================================
    # Metrics
    # =====================================================================

    def show_metrics(self) -> None:
        """Display cluster metrics using ProcessMonitor."""
        from .process_monitor import ProcessMonitor

        processes = self.get_fw_cluster_processes(all_procs=True)
        monitor = ProcessMonitor(processes)
        monitor.show()
