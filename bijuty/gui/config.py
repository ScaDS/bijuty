"""
Framework configuration data classes and registry.

This module keeps dataclass definitions, constants, and the framework registry
completely separate from widget factories and HTML generators.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import os


# =============================================================================
# Constants
# =============================================================================

COLOR_SCHEME = {
    "master_bg": "#9ac3f4",
    "master_text": "#1565c0",
    "master_dark": "#1565c0",
    "worker_bg": "#8ff898",
    "worker_text": "#307032",
    "worker_dark": "#4caf50",
}

# =============================================================================
# Data Classes
# =============================================================================


@dataclass(frozen=True)
class FrameworkConfig:
    """Configuration for a big data framework."""

    name: str
    proc_master: str
    proc_worker: str
    logo_url: str
    worker_file: str
    default_master_port: int
    rest_api_port: int
    default_resources: Optional[Dict[str, int]] = None
    proc_other: Optional[List[str]] = None
    web_ui_links: Optional[List[Tuple[str, str]]] = None

    """List of (port, title) tuples for framework web UIs."""

    @property
    def default_template(self) -> str:
        return os.path.join(
            os.path.dirname(
                __file__), "..", "framework_template", self.name_lower
        )

    @property
    def name_upper(self) -> str:
        """Return the framework name in uppercase."""
        return self.name.upper()

    @property
    def name_lower(self) -> str:
        """Return the framework name in lowercase."""
        return self.name.lower()


@dataclass
class ResourceAllocation:
    """Resource allocation configuration for cluster components."""

    driver_memory: int = 1000
    worker_memory: int = 1000
    executor_memory: int = 1000
    driver_cpu: int = 1
    worker_cpu: int = 1
    executor_cpu: int = 1

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for visualization."""
        return {
            "drv_mem": self.driver_memory,
            "wrk_mem": self.worker_memory,
            "exe_mem": self.executor_memory,
            "drv_cpu": self.driver_cpu,
            "wrk_cpu": self.worker_cpu,
            "exe_cpu": self.executor_cpu,
        }


# =============================================================================
# Framework Registry
# =============================================================================

FRAMEWORK_REGISTRY: Dict[str, FrameworkConfig] = {
    "SPARK": FrameworkConfig(
        name="SPARK",
        proc_master={
            "title": "Master",
            "pattern": "org.apache.spark.deploy.master.Master --host",
        },
        proc_worker={
            "title": "Worker",
            "pattern": "org.apache.spark.deploy.worker.Worker --webui-port",
        },
        proc_other=[
            {"title": "SparkSubmit", "pattern": "org.apache.spark.deploy.SparkSubmit"},
            {"title": "Executor",
                "pattern": "org.apache.spark.executor.CoarseGrainedExecutorBackend"},
            {"title": "Scheduler",
                "pattern": "org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend"},
        ],
        logo_url="https://spark.apache.org/images/spark-logo-back.png",
        worker_file="workers",
        default_master_port=7077,
        rest_api_port=8080,
        default_resources={
            "mem_driver": 1000,
            "mem_worker": 1000,
            "mem_executor": 1000,
            "cpu_driver": 1,
            "cpu_worker": 1,
            "cpu_executor": 1,
        },
        web_ui_links=[
            ("8080", "Master UI"),
            ("8081", "Worker UI"),
            ("4040", "Application UI"),
        ],
    ),
    "FLINK": FrameworkConfig(
        name="FLINK",
        proc_master={
            "title": "Master",
            "pattern": "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint"
        },
        proc_worker={
            "title": "Worker",
            "pattern": "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"
        },
        proc_other=[
            {"title": "TaskManager",
                "pattern": "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"},
        ],
        logo_url="https://flink.apache.org/img/logo/png/200/flink_squirrel_200_color.png",
        worker_file="workers",
        default_master_port=6123,
        rest_api_port=8081,
        default_resources={
            "mem_driver": 1000,
            "mem_worker": 1000,
            "mem_executor": 1000,
            "cpu_driver": 1,
            "cpu_worker": 1,
            "cpu_executor": 1,
        },
        web_ui_links=[
            ("8081", "Flink UI"),
        ],
    ),
}
