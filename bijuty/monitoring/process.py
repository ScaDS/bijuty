"""Process monitoring module for tracking system resource usage.

This module provides classes for collecting process metrics
(CPU usage, memory consumption, thread count, I/O statistics).
The visualization layer has been moved to :py:mod:`~bijuty.metric_plotter`.
"""

from __future__ import annotations

import getpass
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from datetime import datetime

import psutil

from ..gui.widgets import WidgetFactory
from .dashboard import MetricDashboard
from ..slurm_utils import SlurmManager

if TYPE_CHECKING:
    from collections.abc import Sequence

# Constants
BYTES_TO_MB = 1024 * 1024
DEFAULT_HISTORY_SIZE = 40
DEFAULT_REFRESH_INTERVAL = 1.0
MIN_REFRESH_INTERVAL = 1.5
MAX_REFRESH_INTERVAL = 10.0

# Metrics that appear in the dashboard by default.
# Remove or add keys here to control plotting without touching dataclasses.
ENABLED_METRICS = [
    "cpu",
    "mem_pct",
    "mem_rss",
    "mem_vms",
    "threads",
    "io_read",
    "io_write",
]


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ProcessMetricsSnapshot:
    """Snapshot of process metrics at a single point in time.

    Attributes:
        cpu_percent: CPU usage percentage.
        memory_percent: Memory usage as percentage of total system memory.
        memory_rss_mb: Resident set size in megabytes.
        memory_vms_mb: Virtual memory size in megabytes.
        num_threads: Number of threads used by the process.
        io_read_mb: Bytes read by the process in megabytes.
        io_write_mb: Bytes written by the process in megabytes.
        timestamp: Unix timestamp when the snapshot was taken.
    """

    cpu_percent: float
    memory_percent: float
    memory_rss_mb: float
    memory_vms_mb: float
    num_threads: int
    io_read_mb: float
    io_write_mb: float
    timestamp: float


@dataclass
class ProcessMetricsHistory:
    """Historical data for a process's metrics.

    Maintains fixed-size deques for each metric type, storing the most recent
    measurements up to the configured history size.
    """

    cpu: deque[float] = field(
        default_factory=lambda: deque(
            [0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )
    mem_pct: deque[float] = field(
        default_factory=lambda: deque(
            [0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )
    mem_rss: deque[float] = field(
        default_factory=lambda: deque(
            [0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )
    mem_vms: deque[float] = field(
        default_factory=lambda: deque(
            [0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )
    threads: deque[int] = field(
        default_factory=lambda: deque(
            [0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )
    io_read: deque[float] = field(
        default_factory=lambda: deque(
            [0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )
    io_write: deque[float] = field(
        default_factory=lambda: deque(
            [0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )
    timestamp: deque[float] = field(
        default_factory=lambda: deque(
            [0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE
        )
    )

    def append(self, metrics: ProcessMetricsSnapshot) -> None:
        """Append a new metrics snapshot to the history."""
        self.cpu.append(metrics.cpu_percent)
        self.mem_pct.append(metrics.memory_percent)
        self.mem_rss.append(metrics.memory_rss_mb)
        self.mem_vms.append(metrics.memory_vms_mb)
        self.threads.append(metrics.num_threads)
        self.io_read.append(metrics.io_read_mb)
        self.io_write.append(metrics.io_write_mb)
        self.timestamp.append(metrics.timestamp)


# =============================================================================
# Collector
# =============================================================================

class ProcessMetricCollector:
    """Collects system metrics for specified processes.

    This class scans the system for processes matching the given process names
    and maintains a rolling history of their resource usage metrics.

    Args:
        process_names: List of process name patterns to monitor.
        history_size: Maximum number of data points to retain per metric.
            Defaults to DEFAULT_HISTORY_SIZE.

    Example:
        >>> collector = ProcessMetricCollector(
        ...     [{"title": "Spark Master", "pattern": "org.apache.spark.deploy.master.Master"}]
        ... )
        >>> metrics = collector.collect()
    """

    HISTORY: int = DEFAULT_HISTORY_SIZE

    def __init__(
        self,
        process_names: Sequence[str | dict] | None = None,
        history_size: int = DEFAULT_HISTORY_SIZE,
    ) -> None:
        normalized: list[dict] = []
        for item in (process_names if process_names is not None else []):
            if isinstance(item, str):
                normalized.append({"title": item, "pattern": item})
            else:
                normalized.append(item)
        self.process_names = normalized
        self.user = getpass.getuser()
        self._history_size = history_size
        self.history: dict[str, ProcessMetricsHistory] = {
            name.get("title", name.get("pattern", "unknown")): ProcessMetricsHistory()
            for name in self.process_names
        }

    def _match_process(self, proc: psutil.Process) -> dict | None:
        """Check if a process matches any of the monitored process names."""
        try:
            try:
                status = proc.status()
                if status in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                    return None
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                return None

            with proc.oneshot():
                proc_user = proc.username()
                if proc_user != self.user:
                    return None
                cmdline = " ".join(proc.cmdline())

            for proc_i in self.process_names:
                pattern = proc_i.get("pattern")
                if pattern and pattern in cmdline:
                    return proc_i
        except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
            print("Could not access process %s info: %s", proc.pid, e)
        except Exception as e:
            print("Unexpected error accessing process %s: %s", proc.pid, e)

        return None

    def _extract_metrics(self, proc: psutil.Process) -> ProcessMetricsSnapshot | None:
        """Extract metrics from a process."""
        try:
            with proc.oneshot():
                # cpu = psutil.cpu_percent(percpu=True)[proc.cpu_num()]
                cpu = proc.cpu_percent(interval=DEFAULT_REFRESH_INTERVAL)
                mem = proc.memory_info()
                mem_pct = proc.memory_percent()
                threads = proc.num_threads()
                io_counters = proc.io_counters()

            return ProcessMetricsSnapshot(
                cpu_percent=cpu,
                memory_percent=mem_pct,
                memory_rss_mb=mem.rss / BYTES_TO_MB,
                memory_vms_mb=mem.vms / BYTES_TO_MB,
                num_threads=threads,
                io_read_mb=io_counters.read_bytes / BYTES_TO_MB,
                io_write_mb=io_counters.write_bytes / BYTES_TO_MB,
                timestamp=datetime.now().strftime("%H:%M:%S")

            )
        except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
            print("Failed to extract metrics from process %s: %s", proc.pid, e)
        except Exception as e:
            print(
                "Unexpected error extracting metrics from process %s: %s", proc.pid, e
            )

        return None

    def collect(self) -> dict[str, dict[str, Any]]:
        """Collect metrics from all matching processes."""
        results: dict[str, dict[str, Any]] = {}
        if not self.process_names:
            return results

        try:
            for proc in psutil.process_iter(["pid", "name"]):
                proc_found = self._match_process(proc)
                if proc_found is None:
                    continue

                metrics = self._extract_metrics(proc)
                if metrics is None:
                    continue

                proc_found_name = proc_found.get("title")
                self.history[proc_found_name].append(metrics)
                results[proc_found_name] = {
                    "found": True,
                    "history": self.history[proc_found_name],
                    "proc_info": proc,
                }
        except Exception:
            raise

        return results


# =============================================================================
# Monitor
# =============================================================================

class ProcessMonitor(MetricDashboard):
    """Interactive Jupyter widget for monitoring process resources."""

    def __init__(
        self,
        slurm_info: SlurmManager = None,
        process_names: Sequence[str] | None = None,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
        enabled_metrics: Sequence[str] | None = None,
    ) -> None:
        self.process_names = list(process_names) if process_names is not None else []
        self.enabled_metrics = list(enabled_metrics) if enabled_metrics is not None else ENABLED_METRICS
        self.user = getpass.getuser()
        if slurm_info:
            self._slurm_man = slurm_info
        else:
            self._slurm_man = SlurmManager(allow_outside_job=True)

        collector = ProcessMetricCollector(self.process_names)
        # extra = [
        #     WidgetFactory.create_styled_button_redirect(
        #         description="Pika web interface",
        #         url=(
        #             f"https://pika.zih.tu-dresden.de/user/live/job/bash/"
        #             f"{self._slurm_man.job_name}/{self._slurm_man.start_time}/"
        #             f"{self._slurm_man.partition}"
        #         ),
        #     )
        # ]

        super().__init__(
            collector=collector,
            refresh_interval=refresh_interval,
            min_refresh=MIN_REFRESH_INTERVAL,
            max_refresh=MAX_REFRESH_INTERVAL,
            refresh_step=0.5,
            # extra_header_widgets=extra,
        )

    def set_process_names(self, process_names: Sequence[dict]) -> None:
        """Update the process names to monitor."""
        self.process_names = list(process_names)
        self.collector = ProcessMetricCollector(self.process_names)

    def _render_metrics(self, metrics: dict[str, dict[str, Any]]) -> None:
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
        """Get the display name for a metric key."""
        display_map = {
            "cpu": "CPU Usage (%)",
            "mem_pct": "Memory (%)",
            "mem_rss": "Memory (MB)",
            "mem_vms": "Virtual Memory (MB)",
            "threads": "Threads Count",
            "io_read": "IO Read (Mb/s)",
            "io_write": "IO Write (Mb/s)",
        }
        return display_map.get(metric, metric)
