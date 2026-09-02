"""
SLURM job management utilities.

This module provides functionality to interact with SLURM workload manager,
query job information, and manage cluster resources.
"""

from __future__ import annotations

import logging
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import socket
from datetime import datetime
from .utils import run_bash_command

logger = logging.getLogger(__name__)


# =============================================================================
# Utility Functions
# =============================================================================

def get_local_cpu_count() -> int:
    """Get the number of CPUs available on the local machine."""
    return os.cpu_count() or 1


def get_local_memory_mb() -> int:
    """Get total memory of the local machine in MB."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable"):
                    return int(line.split()[1]) // 1024  # kB -> MB
    except Exception as e:
        raise Exception(
            f"\"MemAvailable\" doesn't exist on kernels < 3.14 \n{e}")
    return 0


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class JobResources:
    """SLURM job resource information."""

    node_list: list
    cpus_per_task: int
    tasks_per_node: int
    memory_per_cpu: int  # In MB
    memory_per_node: Optional[int] = None  # In MB, if explicitly set

    @property
    def cpus_per_node(self) -> int:
        """Calculate total CPUs per node."""
        return self.cpus_per_task * self.tasks_per_node

    @property
    def total_cpus(self) -> int:
        """Calculate total CPUs across all nodes."""
        return self.cpus_per_node * self.node_count

    @property
    def memory_per_node_effective(self) -> int:
        """Calculate effective memory per node (in MB)."""
        if self.memory_per_node is not None:
            return self.memory_per_node
        return self.memory_per_cpu * self.cpus_per_node

    @property
    def total_memory(self) -> int:
        """Calculate total memory across all nodes (in MB)."""
        return self.memory_per_node_effective * self.node_count

    @property
    def node_count(self) -> int:
        return len(self.node_list)


# =============================================================================
# SLURM Manager
# =============================================================================


class SlurmManager:
    """Manager for interacting with SLURM workload manager."""

    def __init__(self, allow_outside_job: bool = False):
        """Initialize the SlurmManager."""

        self._in_slurm_job = self._is_in_slurm_job()

        if not self._in_slurm_job and not allow_outside_job:
            raise Exception(
                "No active SLURM job found. "
                "SlurmManager must be initialized inside a SLURM job."
            )

        self._job_context = self._get_slurm_env_context(
            local_machine=not self._in_slurm_job)
        self._user = os.environ.get("USER")
        self._job_id = self._job_context.get("SLURM_JOB_ID")
        self._job_info_raw = self._fetch_job_info()
        self._job_info = self._get_job_info()
        # self._job_name = self._job_info["name"]
        self._start_time = self._job_info["start_time"]["number"]
        self._partition = self._job_info["partition"]
        self._login_node = self._get_default_login_host()
        self._resources = self._parse_job_resources()
        self._set_missing_slurm_env_context()

    @property
    def in_slurm_job(self) -> bool:
        return self._in_slurm_job

    @property
    def user(self) -> str:
        return self._user

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def job_info(self) -> Dict[str, Any]:
        return self._job_info if self._job_info else {}

    @property
    def login_node(self) -> str:
        return self._login_node

    @property
    def resources(self) -> JobResources:
        return self._resources

    def _is_in_slurm_job(self) -> bool:  # try to make it dependent on other
        """
        Check if currently running inside a SLURM job.
        """
        _SLURM_JOB_ID_VARS = ("SLURM_JOB_ID", "SLURM_JOBID")
        for var in _SLURM_JOB_ID_VARS:
            val = os.environ.get(var, "").strip()
            if val.isdigit():
                return True
        return False

    def _get_slurm_env_context(self, local_machine: bool = False) -> Dict[str, str]:
        """Get all SLURM environment variables."""

        if local_machine:
            cpus = get_local_cpu_count()
            mem_mb = get_local_memory_mb()
            os.environ.setdefault("SLURM_JOB_ID", "localhost")
            os.environ.setdefault("SLURM_JOBID", "localhost")
            os.environ.setdefault("SLURM_JOB_NAME", "local-machine")
            os.environ.setdefault(
                "SLURM_JOB_USER", os.environ.get("USER") or "unknown")
            os.environ.setdefault("SLURM_SUBMIT_DIR", os.getcwd())
            os.environ.setdefault("SLURM_NNODES", "1")
            os.environ.setdefault("SLURM_NTASKS", "1")
            os.environ.setdefault("SLURM_CPUS_PER_TASK", str(cpus))
            os.environ.setdefault("SLURM_MEM_PER_NODE", str(mem_mb))
            os.environ.setdefault("SLURM_JOB_NODELIST",
                                  socket.gethostname() or "localhost")
            os.environ.setdefault("SLURM_JOB_NUM_NODES", "1")
            os.environ.setdefault("SLURM_JOB_START_TIME",
                                  datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

        # Extract and format the relevant variables
        return {
            key: value
            for key, value in os.environ.items()
            if key.startswith("SLURM_")
        }

    def _fetch_job_info(self) -> Dict[str, Any]:
        """Fetch job information from SLURM."""

        if not self._in_slurm_job:
            cpus = get_local_cpu_count()
            mem_mb = get_local_memory_mb()
            hostname = socket.gethostname() or "localhost"
            return {
                "jobs": [
                    {
                        "job_id": self._job_id,
                        "name": "local-machine",
                        "partition": "local",
                        "user_name": self._user,
                        "node_count": {"set": True, "infinite": False, "number": 1},
                        "cpus_per_task": {"set": True, "infinite": False, "number": cpus},
                        "tasks": {"set": True, "infinite": False, "number": 1},
                        "memory_per_cpu": {"set": False, "infinite": False, "number": mem_mb // max(cpus, 1)},
                        "memory_per_node": {"set": True, "infinite": False, "number": mem_mb},
                        "start_time": {"set": True, "infinite": False, "number": self._job_context.get("SLURM_JOB_START_TIME", "")},
                        "job_resources": {"nodes": [hostname]},
                        "nodes": hostname,
                    }
                ]
            }
        else:
            # Use scontrol for specific job, squeue for queue overview
            if self._job_id:
                cmd = ["scontrol", "show", "job", self._job_id, "--json"]
            else:
                cmd = ["squeue", "--json"]

            res = run_bash_command(cmd, shell=False, timeout=60)
            if res.returncode:
                raise RuntimeError(res.stderr.strip())

            out = res.stdout.strip()
            try:
                return json.loads(out)
            except json.JSONDecodeError:
                raise Exception(f"Command returned invalid JSON: {out}")

    def _parse_job_resources(self) -> JobResources:
        """Parse job resources from SLURM job info."""
        if not self._job_info:
            return JobResources(
                node_list=[],
                cpus_per_task=0,
                tasks_per_node=0,
                memory_per_cpu=0
            )

        job = self._job_info

        # Parse memory per node if explicitly set
        mem_per_node_data = job.get("memory_per_node", {})
        mem_per_node = int(mem_per_node_data["number"]) if mem_per_node_data.get(
            "set") else None

        return JobResources(
            node_list=self._get_nodes_list(),
            cpus_per_task=int(job["cpus_per_task"]["number"]),
            tasks_per_node=int(job["tasks"]["number"]),
            memory_per_cpu=int(job["memory_per_cpu"]["number"]),
            memory_per_node=mem_per_node,
        )

    def _get_default_login_host(self):
        fqdn = socket.getfqdn().strip()
        parts = fqdn.split('.', 2)
        if len(parts) > 2:
            return f"login1.{parts[1]}.{parts[2]}"
        return "localhost"

    def _get_job_info(self):
        if self._in_slurm_job:
            for job_i in self._job_info_raw["jobs"]:
                if int(job_i["job_id"]) == int(self.job_id):
                    return job_i
        else:
            return self._job_info_raw["jobs"][0]

    def _set_missing_slurm_env_context(self):
        os.environ.setdefault("SLURM_MEM_PER_NODE",
                              f"{self.resources.memory_per_node_effective}")
        os.environ.setdefault("SLURM_CPUS_PER_NODE",
                              f"{self.resources.cpus_per_node}")
        os.environ.setdefault(
            "SLURM_MEM_TOTAL", f"{self.resources.total_memory}")
        os.environ.setdefault("SLURM_CPUS_TOTAL",
                              f"{self.resources.total_cpus}")

    def _get_nodes_list(self) -> List[str]:
        """Get the list of nodes allocated to this job."""

        # if not self._job_info_raw or "jobs" not in self._job_info_raw:
        #     return []
        if not self._job_info:
            return []

        # nodes = self._job_info_raw["jobs"][0].get(
        #     "job_resources", {}).get("nodes", [])
        nodes = self._job_info.get(
            "job_resources", {}).get("nodes", [])
        if isinstance(nodes, list):
            return nodes
        return [nodes] if nodes else []

    # In-built Methods

    def __repr__(self) -> str:
        """Return a formatted string representation."""
        status = "Active" if self._in_slurm_job else "Inactive"

        # Format job info
        if isinstance(self._job_info_raw, dict):
            pretty_job_info = json.dumps(self._job_info_raw, indent=2)
        else:
            pretty_job_info = str(self._job_info_raw)

        return (
            f"=== SlurmManager (Job ID: {self._job_id}) ===\n"
            f"Status: {status}\n"
            f"--- Environment Context ---\n"
            f"Nodes: {self.resources.node_count} - {self.resources.node_list}\n"
            f"Partition: {self._partition}\n"
            f"--- Job Resources ---\n"
            f"Total CPUs: {self.resources.total_cpus}\n"
            f"Total Memory: {self.resources.total_memory} MB\n"
            f"--- Full Job Info ---\n"
            f"{pretty_job_info}\n"
            f"=========================================="
        )
