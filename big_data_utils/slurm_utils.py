"""
SLURM job management utilities.

This module provides functionality to interact with SLURM workload manager,
query job information, and manage cluster resources.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


# =============================================================================
# Exceptions
# =============================================================================

class SlurmException(Exception):
    """Base exception for SLURM-related errors."""
    pass


class NotInSlurmJobError(SlurmException):
    """Raised when SlurmManager is initialized outside an active SLURM job."""
    pass


class SlurmCommandError(SlurmException):
    """Raised when a SLURM command fails."""
    pass


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class JobResources:
    """SLURM job resource information."""

    node_count: int
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


@dataclass
class SlurmJobInfo:
    """Structured SLURM job information."""
    user: str
    job_id: str
    status: str
    nodes: List[str] = field(default_factory=list)
    resources: Optional[JobResources] = None
    partition: Optional[str] = None
    raw_info: Dict[str, Any] = field(default_factory=dict, repr=False)


# =============================================================================
# Utility Functions
# =============================================================================

def is_in_slurm_job() -> bool:
    """
    Check if currently running inside a SLURM job.

    Returns:
        True if SLURM_JOB_ID environment variable is set, False otherwise
    """
    return "SLURM_JOB_ID" in os.environ


def get_slurm_env_context() -> Dict[str, str]:
    """
    Get all SLURM environment variables.

    Extracts and returns all environment variables starting with "SLURM_",
    with the prefix removed for cleaner keys.

    Returns:
        Dictionary of SLURM environment variables
    """
    return {
        key.replace("SLURM_", ""): value
        for key, value in os.environ.items()
        if key.startswith("SLURM_")
    }


# =============================================================================
# SLURM Manager
# =============================================================================

class SlurmManager:
    """
    Manager for interacting with SLURM workload manager.

    This class provides methods to query job information, manage allocations,
    and interact with SLURM commands. It must be initialized within an active
    SLURM job.

    Attributes:
        job_id: The SLURM job ID
        job_context: Dictionary of SLURM environment variables
        job_info: Raw job information from SLURM
    """

    def __init__(self, allow_outside_job: bool = False):
        """
        Initialize the SlurmManager.

        Args:
            allow_outside_job: If True, allow initialization outside SLURM job

        Raises:
            NotInSlurmJobError: If not in a SLURM job and allow_outside_job is False
        """
        self._in_slurm_job = is_in_slurm_job()

        if not self._in_slurm_job and not allow_outside_job:
            raise NotInSlurmJobError(
                "No active SLURM job found. "
                "SlurmManager must be initialized inside a SLURM job."
            )

        if self._in_slurm_job:
            self._job_context = get_slurm_env_context()
            self._user = os.environ.get("USER")
            self._job_id = self._job_context.get("JOB_ID", "unknown")
            self._job_info_raw = self._fetch_job_info()
        else:
            self._job_context = {}
            self._job_id = "none"
            self._job_info_raw = {}

        self._resources: Optional[JobResources] = None
        self._structured_info: Optional[SlurmJobInfo] = None

    @property
    def in_slurm_job(self) -> bool:
        """Check if running in a SLURM job."""
        return self._in_slurm_job

    @property
    def user(self) -> str:
        """Get the SLURM job ID."""
        return self._user

    @property
    def job_id(self) -> str:
        """Get the SLURM job ID."""
        return self._job_id

    @property
    def job_context(self) -> Dict[str, str]:
        """Get SLURM environment context."""
        return self._job_context.copy()

    @property
    def job_info(self) -> Dict[str, Any]:
        """Get raw job information."""
        return self._job_info_raw.copy() if self._job_info_raw else {}

    # =====================================================================
    # Private Methods
    # =====================================================================

    def _run_command(self, cmd: List[str]) -> str:
        """
        Execute a shell command and return stdout.

        Args:
            cmd: Command and arguments as a list

        Returns:
            Command stdout as string

        Raises:
            SlurmCommandError: If command fails
        """
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            raise SlurmCommandError(f"Command failed: {' '.join(cmd)}\nError: {e.stderr.strip()}")
        except FileNotFoundError:
            raise SlurmCommandError(f"SLURM command not found: {cmd[0]}. Please ensure SLURM is installed or use mock SLURM environment.")

    def _fetch_job_info(self) -> Dict[str, Any]:
        """
        Fetch job information from SLURM.

        Returns:
            Parsed JSON job information
        """
        if self._job_id == "unknown":
            return {}

        # Use scontrol for specific job, squeue for queue overview
        if self._job_id:
            cmd = ["scontrol", "show", "job", self._job_id, "--json"]
        else:
            cmd = ["squeue", "--json"]

        try:
            output = self._run_command(cmd)
            if "{" in output:
                return json.loads(output)
            return {"raw_output": output}
        except (json.JSONDecodeError, SlurmCommandError) as e:
            return {"error": str(e)}

    def _parse_job_resources(self) -> JobResources:
        """
        Parse job resources from SLURM job info.

        Returns:
            JobResources dataclass with parsed resource information
        """
        if not self._job_info_raw or "jobs" not in self._job_info_raw:
            return JobResources(node_count=0, cpus_per_task=0, tasks_per_node=0, memory_per_cpu=0)

        job = self._job_info_raw["jobs"][0]

        # Parse memory per node if explicitly set
        mem_per_node_data = job.get("memory_per_node", {})
        mem_per_node = int(mem_per_node_data["number"]) if mem_per_node_data.get("set") else None

        return JobResources(
            node_count=int(job["node_count"]["number"]),
            cpus_per_task=int(job["cpus_per_task"]["number"]),
            tasks_per_node=int(job["tasks"]["number"]),
            memory_per_cpu=int(job["memory_per_cpu"]["number"]),
            memory_per_node=mem_per_node,
        )

    def _ensure_resources(self) -> JobResources:
        """Ensure resources are parsed and cached."""
        if self._resources is None:
            self._resources = self._parse_job_resources()
        return self._resources

    # =====================================================================
    # Public API - Job Control
    # =====================================================================

    def cancel_job(self, job_id: Optional[str] = None) -> str:
        """
        Cancel a specific job.

        Args:
            job_id: Job ID to cancel (defaults to current job)

        Returns:
            Command output or error message
        """
        target_id = job_id or self._job_id
        if target_id == "none" or target_id == "unknown":
            return "Error: No job ID available"

        try:
            return self._run_command(["scancel", str(target_id)])
        except SlurmCommandError as e:
            return str(e)

    # =====================================================================
    # Public API - Resource Queries
    # =====================================================================

    def get_nodes_list(self) -> List[str]:
        """
        Get the list of nodes allocated to this job.

        Returns:
            List of node hostnames
        """
        if not self._job_info_raw or "jobs" not in self._job_info_raw:
            return []

        nodes = self._job_info_raw["jobs"][0].get("job_resources", {}).get("nodes", [])
        if isinstance(nodes, list):
            return nodes
        return [nodes] if nodes else []

    def get_total_nodes(self) -> int:
        """Get total number of nodes allocated to the job."""
        return self._ensure_resources().node_count

    def get_cpus_per_task(self) -> int:
        """Get CPUs allocated per task."""
        return self._ensure_resources().cpus_per_task

    def get_tasks_per_node(self) -> int:
        """Get tasks allocated per node."""
        return self._ensure_resources().tasks_per_node

    def get_cpus_per_node(self) -> int:
        """Get total CPUs per node."""
        return self._ensure_resources().cpus_per_node

    def get_memory_per_cpu(self) -> int:
        """Get memory per CPU in MB."""
        return self._ensure_resources().memory_per_cpu

    def get_memory_per_node(self) -> int:
        """Get memory per node in MB."""
        return self._ensure_resources().memory_per_node_effective

    def get_total_cpus(self) -> int:
        """Get total CPUs across all nodes."""
        return self._ensure_resources().total_cpus

    def get_total_memory(self) -> int:
        """Get total memory across all nodes in MB."""
        return self._ensure_resources().total_memory

    def get_partition(self) -> str:
        """Get the partition/job class."""
        return self._job_context.get("JOB_PARTITION", "N/A")

    # =====================================================================
    # Representation
    # =====================================================================

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
            f"Nodes: {self._job_context.get('NNODES', 'N/A')}\n"
            f"Partition: {self._job_context.get('JOB_PARTITION', 'N/A')}\n"
            f"--- Job Resources ---\n"
            f"Total CPUs: {self.get_total_cpus()}\n"
            f"Total Memory: {self.get_total_memory()} MB\n"
            f"Node List: {', '.join(self.get_nodes_list())}\n"
            f"--- Full Job Info ---\n"
            f"{pretty_job_info}\n"
            f"=========================================="
        )
