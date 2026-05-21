"""Spark application metric collector with interactive Plotly visualization.

Connects to the Spark REST API (port 4040) to fetch application-level metrics
(jobs, stages, tasks, executors, shuffle I/O) and renders them via the shared
:py:class:`~bijuty.metric_plotter.MetricDashboard`.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime

import requests

from .gui_components import WidgetFactory
from .metric_plotter import MetricDashboard
from .slurm_utils import SlurmManager
from .utils import debug_write_to_file

# =============================================================================
# Constants
# =============================================================================

DEFAULT_HISTORY_SIZE = 40
DEFAULT_REFRESH_INTERVAL = 1.0
MIN_REFRESH_INTERVAL = 1.0
MAX_REFRESH_INTERVAL = 60.0

# =============================================================================
# Configurable metric list
# =============================================================================
# Add or remove metric names here to control which metrics are displayed.
# All metrics are collected regardless; this list only affects plotting.
ENABLED_METRICS = [
    "total_memory_mb",
    "total_shuffle_read_mb",
    "total_shuffle_write_mb",
    "total_gc_time_ms",
    "jvm_heap_used_mb",
]


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class SparkMetricsSnapshot:
    """Snapshot of Spark application metrics at a single point in time."""

    active_jobs: int
    completed_jobs: int
    failed_jobs: int
    active_stages: int
    completed_stages: int
    failed_stages: int
    active_tasks: int
    completed_tasks: int
    failed_tasks: int
    executor_count: int
    total_cores: int
    total_memory_mb: float
    total_input_mb: float
    total_shuffle_read_mb: float
    total_shuffle_write_mb: float
    total_gc_time_ms: float
    jvm_heap_used_mb: float
    timestamp: str


@dataclass
class SparkMetricsHistory:
    """Rolling history for Spark application metrics."""

    active_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    completed_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    failed_jobs: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    active_stages: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    completed_stages: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    failed_stages: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    active_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    completed_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    failed_tasks: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    executor_count: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_cores: deque[int] = field(
        default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_memory_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_input_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_shuffle_read_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_shuffle_write_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    total_gc_time_ms: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    jvm_heap_used_mb: deque[float] = field(
        default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )
    timestamp: deque[str] = field(
        default_factory=lambda: deque([""] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE)
    )

    def append(self, metrics: SparkMetricsSnapshot) -> None:
        self.active_jobs.append(metrics.active_jobs)
        self.completed_jobs.append(metrics.completed_jobs)
        self.failed_jobs.append(metrics.failed_jobs)
        self.active_stages.append(metrics.active_stages)
        self.completed_stages.append(metrics.completed_stages)
        self.failed_stages.append(metrics.failed_stages)
        self.active_tasks.append(metrics.active_tasks)
        self.completed_tasks.append(metrics.completed_tasks)
        self.failed_tasks.append(metrics.failed_tasks)
        self.executor_count.append(metrics.executor_count)
        self.total_cores.append(metrics.total_cores)
        self.total_memory_mb.append(metrics.total_memory_mb)
        self.total_input_mb.append(metrics.total_input_mb)
        self.total_shuffle_read_mb.append(metrics.total_shuffle_read_mb)
        self.total_shuffle_write_mb.append(metrics.total_shuffle_write_mb)
        self.total_gc_time_ms.append(metrics.total_gc_time_ms)
        self.jvm_heap_used_mb.append(metrics.jvm_heap_used_mb)
        self.timestamp.append(metrics.timestamp)



# =============================================================================
# Collector
# =============================================================================

class SparkMetricCollector:
    """Collects Spark application metrics via the REST API."""

    HISTORY: int = DEFAULT_HISTORY_SIZE

    def __init__(
        self,
        base_url: Optional[str] = None,
        history_size: int = DEFAULT_HISTORY_SIZE,
    ) -> None:
        self.timeout = 10
        self._base_url = base_url
        self._history_size = history_size
        self.history: Dict[str, SparkMetricsHistory] = {}
        self._current_app_id: Optional[str] = None

    def _get(self, endpoint: str) -> List[Dict] | Dict:
        url = f"{self._base_url}/api/v1{endpoint}"
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Fetching logic
    # ------------------------------------------------------------------

    def _fetch_current_app_id(self) -> Optional[str]:
        try:
            apps = self._get("/applications")
            if not apps:
                return None
            for app in apps:
                completed = app.get("attempts", [{}])[0].get("completed", True)
                if not completed:
                    return app["id"]
            return apps[-1]["id"]
        except Exception:
            return None

    def _compute_snapshot(self, app_id: str) -> SparkMetricsSnapshot | None:
        try:
            jobs = self._get(f"/applications/{app_id}/jobs")
            stages = self._get(f"/applications/{app_id}/stages")
            executors = self._get(f"/applications/{app_id}/executors")

            active_jobs = sum(1 for j in jobs if j.get("status") == "RUNNING")
            completed_jobs = sum(1 for j in jobs if j.get("status") == "SUCCEEDED")
            failed_jobs = sum(1 for j in jobs if j.get("status") == "FAILED")

            active_stages = sum(1 for s in stages if s.get("status") == "ACTIVE")
            completed_stages = sum(1 for s in stages if s.get("status") == "COMPLETE")
            failed_stages = sum(1 for s in stages if s.get("status") == "FAILED")

            active_tasks = sum(j.get("numActiveTasks", 0) for j in jobs)
            completed_tasks = sum(j.get("numCompletedTasks", 0) for j in jobs)
            failed_tasks = sum(j.get("numFailedTasks", 0) for j in jobs)

            executor_count = len(executors)
            total_cores = sum(e.get("totalCores", 0) for e in executors)
            total_memory = sum(e.get("memoryUsed", 0) for e in executors) / (1024 ** 2)
            total_input = sum(e.get("totalInputBytes", 0) for e in executors) / (1024 ** 2)
            total_shuffle_read = (
                sum(e.get("totalShuffleRead", 0) for e in executors) / (1024 ** 2)
            )
            total_shuffle_write = (
                sum(e.get("totalShuffleWrite", 0) for e in executors) / (1024 ** 2)
            )
            total_gc_time = sum(e.get("totalGCTime", 0) for e in executors)
            jvm_heap_used = 0
            for e in executors:
                peak = e.get("peakMemoryMetrics", {})
                if peak:
                    jvm_heap_used += peak.get("JVMHeapMemory", 0)
                else:
                    mem = e.get("memoryMetrics", {})
                    if mem:
                        jvm_heap_used += mem.get("usedOnHeapStorageMemory", 0)

            return SparkMetricsSnapshot(
                active_jobs=active_jobs,
                completed_jobs=completed_jobs,
                failed_jobs=failed_jobs,
                active_stages=active_stages,
                completed_stages=completed_stages,
                failed_stages=failed_stages,
                active_tasks=active_tasks,
                completed_tasks=completed_tasks,
                failed_tasks=failed_tasks,
                executor_count=executor_count,
                total_cores=total_cores,
                total_memory_mb=total_memory,
                total_input_mb=total_input,
                total_shuffle_read_mb=total_shuffle_read,
                total_shuffle_write_mb=total_shuffle_write,
                total_gc_time_ms=total_gc_time,
                jvm_heap_used_mb=jvm_heap_used / (1024 ** 2),
                timestamp=datetime.now().strftime("%H:%M:%S"),
            )
        except Exception:
            return None

    def collect(self) -> Dict[str, Dict[str, Any]]:
        """Fetch latest metrics and update rolling history."""
        results: Dict[str, Dict[str, Any]] = {}

        if not self._current_app_id:
            self._current_app_id = self._fetch_current_app_id()

        if not self._current_app_id:
            return results

        app_id = self._current_app_id
        if app_id not in self.history:
            self.history[app_id] = SparkMetricsHistory()

        snapshot = self._compute_snapshot(app_id)
        if snapshot is None:
            # App may have finished; try finding the next one next cycle
            self._current_app_id = None
            return results

        self.history[app_id].append(snapshot)
        results[app_id] = {
            "found": True,
            "history": self.history[app_id],
            "app_id": app_id,
        }
        return results


# =============================================================================
# Monitor
# =============================================================================

class SparkMetricMonitor(MetricDashboard):
    """Interactive Jupyter widget for monitoring Spark application metrics."""

    def __init__(
        self,
        base_url = None,
        user_input: Dict = None,
        slurm_info: SlurmManager = None,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
        enabled_metrics: Optional[List[str]] = None,
    ) -> None:
        self._user_input = user_input
        
        if base_url:
            self._base_url = base_url
        else:
            self._base_url = None
        
        self.enabled_metrics = enabled_metrics or ENABLED_METRICS
        
        self.collector = SparkMetricCollector(base_url=self._base_url)
        self._extra = [
            WidgetFactory.create_styled_button_redirect(
                description="Spark App UI",
                url=self.collector._base_url,
            )
        ]
        super().__init__(
            collector=self.collector,
            refresh_interval=refresh_interval,
            min_refresh=MIN_REFRESH_INTERVAL,
            max_refresh=MAX_REFRESH_INTERVAL,
            refresh_step=0.5,
            extra_header_widgets=self._extra,
        )
    
    def set_monitor(self, user_input: Dict = None, base_url=None):
        if base_url:
            self._base_url = base_url
        else:
            if user_input:
                self._base_url = f"http://{user_input.master}:4040"
            else:
                raise Exception("Please provide base url")
        
        self.collector = SparkMetricCollector(base_url=self._base_url)
    
    def _render_metrics(self, metrics: Dict[str, Dict[str, Any]]) -> None:
        if not metrics:
            return

        full_history_keys = [
            k
            for k in vars(next(iter(metrics.values()))["history"])
            if k != "timestamp" and not k.startswith("_")
        ]
        history_keys = [k for k in full_history_keys if k in self.enabled_metrics]

        if not history_keys:
            return

        with self._dashboard.hold_trait_notifications():
            for plot_id, data in metrics.items():
                if plot_id not in self._process_plots:
                    self._create_plot(plot_id, history_keys)

                proc_info = self._process_plots[plot_id]
                proc_info["latest_data"] = data
                self._update_plot(plot_id, data, history_keys)

    def _get_metric_display_name(self, metric: str) -> str:
        display_map = {
            "active_jobs": "Active Jobs",
            "completed_jobs": "Completed Jobs",
            "failed_jobs": "Failed Jobs",
            "active_stages": "Active Stages",
            "completed_stages": "Completed Stages",
            "failed_stages": "Failed Stages",
            "active_tasks": "Active Tasks",
            "completed_tasks": "Completed Tasks",
            "failed_tasks": "Failed Tasks",
            "executor_count": "Executors",
            "total_cores": "Total Cores",
            "total_memory_mb": "Memory (MB)",
            "total_input_mb": "Input (MB)",
            "total_shuffle_read_mb": "Shuffle Read (MB)",
            "total_shuffle_write_mb": "Shuffle Write (MB)",
            "total_gc_time_ms": "GC Time (ms)",
            "jvm_heap_used_mb": "JVM Heap Used (MB)",
        }
        return display_map.get(metric, metric)
