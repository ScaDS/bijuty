"""Flink application metric collector with interactive Plotly visualization.

Connects to the Flink REST API (port 8081 by default) to fetch job-level
metrics and renders them via the shared
:py:class:`~bijuty.metric_plotter.MetricDashboard`.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

from ..gui.widgets import WidgetFactory
from .dashboard import MetricDashboard
from ..slurm_utils import SlurmManager


# =============================================================================
# Constants
# =============================================================================

DEFAULT_HISTORY_SIZE = 40
DEFAULT_REFRESH_INTERVAL = 5.0
MIN_REFRESH_INTERVAL = 1.0
MAX_REFRESH_INTERVAL = 60.0


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class FlinkMetricsSnapshot:
    """Snapshot of Flink job metrics at a single point in time."""

    total_jobs: int
    running_jobs: int
    failed_jobs: int
    finished_jobs: int
    cancelled_jobs: int
    total_tasks: int
    running_tasks: int
    failed_tasks: int
    finished_tasks: int
    cancelled_tasks: int
    total_slots: int
    used_slots: int
    free_slots: int
    task_managers: int
    total_memory_mb: float
    total_network_memory_mb: float
    total_jvm_memory_mb: float
    timestamp: float


@dataclass
class FlinkMetricsHistory:
    """Rolling history for Flink job metrics."""

    total_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    running_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    failed_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    finished_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    cancelled_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    running_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    failed_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    finished_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    cancelled_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_slots: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    used_slots: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    free_slots: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    task_managers: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_memory_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_network_memory_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_jvm_memory_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    timestamp: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )

    def append(self, metrics: FlinkMetricsSnapshot) -> None:
        self.total_jobs.append(metrics.total_jobs)
        self.running_jobs.append(metrics.running_jobs)
        self.failed_jobs.append(metrics.failed_jobs)
        self.finished_jobs.append(metrics.finished_jobs)
        self.cancelled_jobs.append(metrics.cancelled_jobs)
        self.total_tasks.append(metrics.total_tasks)
        self.running_tasks.append(metrics.running_tasks)
        self.failed_tasks.append(metrics.failed_tasks)
        self.finished_tasks.append(metrics.finished_tasks)
        self.cancelled_tasks.append(metrics.cancelled_tasks)
        self.total_slots.append(metrics.total_slots)
        self.used_slots.append(metrics.used_slots)
        self.free_slots.append(metrics.free_slots)
        self.task_managers.append(metrics.task_managers)
        self.total_memory_mb.append(metrics.total_memory_mb)
        self.total_network_memory_mb.append(metrics.total_network_memory_mb)
        self.total_jvm_memory_mb.append(metrics.total_jvm_memory_mb)
        self.timestamp.append(metrics.timestamp)


# =============================================================================
# Collector
# =============================================================================

class FlinkMetricCollector:
    """Collects Flink job metrics via the REST API."""

    HISTORY: int = DEFAULT_HISTORY_SIZE

    def __init__(
        self,
        base_url: Optional[str] = None,
        history_size: int = DEFAULT_HISTORY_SIZE,
        slurm_info: Optional[SlurmManager] = None,
    ) -> None:
        self.timeout = 10
        self._slurm = slurm_info or SlurmManager(allow_outside_job=True)
        self._base_url = self._resolve_url(base_url)
        self._history_size = history_size
        self.history: Dict[str, FlinkMetricsHistory] = {}
        self._current_job_id: Optional[str] = None

    # ------------------------------------------------------------------
    # URL resolution
    # ------------------------------------------------------------------

    def _resolve_url(self, provided_url: Optional[str]) -> str:
        if provided_url:
            return provided_url.rstrip("/")

        user = self._slurm.user or "unknown"
        proxy_url = f"https://jupyterhub.hpc.tu-dresden.de/user/{user}/proxy/8081"
        if self._check_url(proxy_url):
            return proxy_url

        localhost_url = "http://localhost:8081"
        if self._check_url(localhost_url):
            return localhost_url

        try:
            nodes = self._slurm.resources.node_list
            if nodes:
                direct_url = f"http://{nodes[0]}:8081"
                if self._check_url(direct_url):
                    return direct_url
        except Exception:
            pass

        return "http://localhost:8081"

    def _check_url(self, url: str) -> bool:
        try:
            requests.get(f"{url}/overview", timeout=2)
            return True
        except Exception:
            return False

    def _get(self, endpoint: str) -> Any:
        url = f"{self._base_url}{endpoint}"
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Fetching logic
    # ------------------------------------------------------------------

    def _fetch_running_job_id(self) -> Optional[str]:
        try:
            jobs = self._get("/jobs")
            for job in jobs.get("jobs", []):
                if job.get("status") == "RUNNING":
                    return job.get("id")
            return None
        except Exception:
            return None

    def _compute_snapshot(self) -> FlinkMetricsSnapshot | None:
        try:
            overview = self._get("/overview")
            jobs = self._get("/jobs")
            taskmanagers = self._get("/taskmanagers")

            job_list = jobs.get("jobs", [])
            statuses = {}
            for job in job_list:
                st = job.get("status", "UNKNOWN")
                statuses[st] = statuses.get(st, 0) + 1

            total_jobs = len(job_list)
            running_jobs = statuses.get("RUNNING", 0)
            failed_jobs = statuses.get("FAILED", 0)
            finished_jobs = statuses.get("FINISHED", 0)
            cancelled_jobs = statuses.get("CANCELLED", 0)

            tasks = overview.get("tasks", {})
            total_tasks = tasks.get("total", 0)
            running_tasks = tasks.get("running", 0)
            failed_tasks = tasks.get("failed", 0)
            finished_tasks = tasks.get("finished", 0)
            cancelled_tasks = tasks.get("canceled", 0)

            slots = overview.get("slots-total", 0)
            used_slots = overview.get("slots-used", 0)
            free_slots = slots - used_slots
            tm_count = len(taskmanagers.get("taskmanagers", []))

            total_mem = overview.get("total-memory", 0) / (1024 ** 2)
            net_mem = overview.get("total-network-memory", 0) / (1024 ** 2)
            jvm_mem = overview.get("total-jvm-memory", 0) / (1024 ** 2)

            return FlinkMetricsSnapshot(
                total_jobs=total_jobs,
                running_jobs=running_jobs,
                failed_jobs=failed_jobs,
                finished_jobs=finished_jobs,
                cancelled_jobs=cancelled_jobs,
                total_tasks=total_tasks,
                running_tasks=running_tasks,
                failed_tasks=failed_tasks,
                finished_tasks=finished_tasks,
                cancelled_tasks=cancelled_tasks,
                total_slots=slots,
                used_slots=used_slots,
                free_slots=free_slots,
                task_managers=tm_count,
                total_memory_mb=total_mem,
                total_network_memory_mb=net_mem,
                total_jvm_memory_mb=jvm_mem,
                timestamp=time.time(),
            )
        except Exception:
            return None

    def collect(self) -> Dict[str, Dict[str, Any]]:
        """Fetch latest metrics and update rolling history."""
        results: Dict[str, Dict[str, Any]] = {}

        if not self._current_job_id:
            self._current_job_id = self._fetch_running_job_id()

        job_id = self._current_job_id or "cluster"
        if job_id not in self.history:
            self.history[job_id] = FlinkMetricsHistory()

        snapshot = self._compute_snapshot()
        if snapshot is None:
            self._current_job_id = None
            return results

        self.history[job_id].append(snapshot)
        results[job_id] = {
            "found": True,
            "history": self.history[job_id],
            "job_id": job_id,
            "app_id": job_id,  # alias for dashboard compatibility
        }
        return results


# =============================================================================
# Monitor
# =============================================================================

class FlinkMetricMonitor(MetricDashboard):
    """Interactive Jupyter widget for monitoring Flink job metrics."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
        slurm_info: Optional[SlurmManager] = None,
    ) -> None:
        self._slurm_info = slurm_info
        collector = FlinkMetricCollector(base_url=base_url, slurm_info=slurm_info)
        extra = [
            WidgetFactory.create_styled_button_redirect(
                description="Flink Web UI",
                url=collector._base_url,
            )
        ]
        super().__init__(
            collector=collector,
            refresh_interval=refresh_interval,
            min_refresh=MIN_REFRESH_INTERVAL,
            max_refresh=MAX_REFRESH_INTERVAL,
            refresh_step=1.0,
            extra_header_widgets=extra,
        )

    def set_monitor(self, user_input: Dict = None, base_url=None):
        if base_url:
            self._base_url = base_url
        else:
            if user_input:
                self._base_url = f"http://{user_input.master}:8081"
            else:
                raise Exception("Please provide base url")

        self.collector = FlinkMetricCollector(base_url=self._base_url, slurm_info=self._slurm_info)

    def _get_metric_display_name(self, metric: str) -> str:
        display_map = {
            "total_jobs": "Total Jobs",
            "running_jobs": "Running Jobs",
            "failed_jobs": "Failed Jobs",
            "finished_jobs": "Finished Jobs",
            "cancelled_jobs": "Cancelled Jobs",
            "total_tasks": "Total Tasks",
            "running_tasks": "Running Tasks",
            "failed_tasks": "Failed Tasks",
            "finished_tasks": "Finished Tasks",
            "cancelled_tasks": "Cancelled Tasks",
            "total_slots": "Total Slots",
            "used_slots": "Used Slots",
            "free_slots": "Free Slots",
            "task_managers": "Task Managers",
            "total_memory_mb": "Memory (MB)",
            "total_network_memory_mb": "Network Memory (MB)",
            "total_jvm_memory_mb": "JVM Memory (MB)",
        }
        return display_map.get(metric, metric)
