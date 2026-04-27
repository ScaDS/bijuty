"""Process monitoring module for tracking system resource usage.

This module provides classes for collecting and visualizing process metrics
including CPU usage, memory consumption, thread count, and I/O statistics.
"""

from __future__ import annotations

import getpass
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import ipywidgets as widgets
import psutil
from IPython.display import display
import plotly.graph_objects as go
from plotly.subplots import make_subplots

if TYPE_CHECKING:
    from collections.abc import Sequence


logger = logging.getLogger(__name__)

# Constants
BYTES_TO_MB = 1024 * 1024
DEFAULT_HISTORY_SIZE = 40
DEFAULT_REFRESH_INTERVAL = 2.0
MIN_REFRESH_INTERVAL = 0.5
MAX_REFRESH_INTERVAL = 10.0
PLOT_HEIGHT_PER_PROCESS = 400
PLOT_WIDTH = 2000


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

    Attributes:
        cpu: CPU usage history.
        mem_pct: Memory percentage history.
        mem_rss: Resident set size history (MB).
        mem_vms: Virtual memory size history (MB).
        threads: Thread count history.
        io_read: I/O read history (MB).
        io_write: I/O write history (MB).
        timestamp: Measurement timestamps.
    """

    cpu: deque[float] = field(default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))
    mem_pct: deque[float] = field(default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))
    mem_rss: deque[float] = field(default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))
    mem_vms: deque[float] = field(default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))
    threads: deque[int] = field(default_factory=lambda: deque([0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))
    io_read: deque[float] = field(default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))
    io_write: deque[float] = field(default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))
    timestamp: deque[float] = field(default_factory=lambda: deque([0.0] * DEFAULT_HISTORY_SIZE, maxlen=DEFAULT_HISTORY_SIZE))

    def append(self, metrics: ProcessMetricsSnapshot) -> None:
        """Append a new metrics snapshot to the history.

        Args:
            metrics: The metrics snapshot to append.
        """
        self.cpu.append(metrics.cpu_percent)
        self.mem_pct.append(metrics.memory_percent)
        self.mem_rss.append(metrics.memory_rss_mb)
        self.mem_vms.append(metrics.memory_vms_mb)
        self.threads.append(metrics.num_threads)
        self.io_read.append(metrics.io_read_mb)
        self.io_write.append(metrics.io_write_mb)
        self.timestamp.append(metrics.timestamp)


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
        ...     ["org.apache.spark.deploy.master.Master"]
        ... )
        >>> metrics = collector.collect()
    """

    HISTORY: int = DEFAULT_HISTORY_SIZE

    def __init__(
        self,
        process_names: Sequence[str] | None = None,
        history_size: int = DEFAULT_HISTORY_SIZE,
    ) -> None:
        self.process_names = list(process_names) if process_names is not None else []
        
        self.user = getpass.getuser()
        self._history_size = history_size
        self.history: dict[str, ProcessMetricsHistory] = {
            name: ProcessMetricsHistory() for name in self.process_names
        }

    def _match_process(self, proc: psutil.Process) -> str | None:
        """Check if a process matches any of the monitored process names.

        Args:
            proc: The process to check.

        Returns:
            The matching process name pattern, or None if no match.
        """
        print(f"[DEBUG] _match_process entered with pid={proc.pid}")
        # Quick check: skip kernel threads and invalid processes
        if proc.pid <= 2:
            print(f"[DEBUG] Skipping pid {proc.pid} (<=2)")
            return None
        try:
            print(f"[DEBUG] Checking status of pid {proc.pid}")
            # Check status first - skip zombies and other problematic states
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
            # Debug first few processes
            if proc.pid % 1000 == 0:  # Sample some PIDs
                print(f"[DEBUG] Checking PID {proc.pid}: user={proc_user}, cmdline[:100]={cmdline[:100]}")
            for name in self.process_names:
                if name in cmdline:
                    print(f"[DEBUG] Match found! name='{name}' in cmdline of PID {proc.pid}")
                    return name
        except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
            logger.debug("Could not access process %s info: %s", proc.pid, e)
        except Exception as e:
            logger.warning("Unexpected error accessing process %s: %s", proc.pid, e)

        return None

    def _extract_metrics(self, proc: psutil.Process) -> ProcessMetricsSnapshot | None:
        """Extract metrics from a process.

        Args:
            proc: The process to extract metrics from.

        Returns:
            A ProcessMetricsSnapshot containing the extracted metrics,
            or None if extraction failed.
        """
        try:
            with proc.oneshot():
                cpu = proc.cpu_percent(interval=None)
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
                timestamp=time.time(),
            )
        except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
            logger.debug("Failed to extract metrics from process %s: %s", proc.pid, e)
        except Exception as e:
            logger.warning("Unexpected error extracting metrics from process %s: %s", proc.pid, e)

        return None

    def collect(self) -> dict[str, dict[str, Any]]:
        """Collect metrics from all matching processes.

        Iterates through all system processes, matches them against the
        configured process names, and updates the history with current metrics.

        Returns:
            A dictionary mapping process names to their metric data.
            Each value contains 'found' (bool) and 'history' (ProcessMetricsHistory).
        """
        results: dict[str, dict[str, Any]] = {}
        print(f"[DEBUG] collect() called, process_names={self.process_names}")
        if not self.process_names:
            print("[DEBUG] No process_names configured, returning empty results")
            return results
        try:
            proc_count = 0
            match_count = 0
            print("[DEBUG] Starting process_iter()")
            for proc in psutil.process_iter(['pid', 'name']):
                proc_count += 1
                print(f"[DEBUG] Got process object #{proc_count}")
                if proc_count % 500 == 0:
                    print(f"[DEBUG] Processed {proc_count} processes so far...")
                print(f"[DEBUG] About to call _match_process on proc")
                name = self._match_process(proc)
                print(f"[DEBUG] _match_process returned: {name}")
                if name is None:
                    continue
                match_count += 1
                print(f"[DEBUG] Matched process: {name} (PID: {proc.pid})")

                metrics = self._extract_metrics(proc)
                if metrics is None:
                    print(f"[DEBUG] Failed to extract metrics for {name}")
                    continue

                self.history[name].append(metrics)
                results[name] = {"found": True, "history": self.history[name]}
                print(f"[DEBUG] Collected metrics for {name}: cpu={metrics.cpu_percent:.1f}%")
            print(f"[DEBUG] Total processed: {proc_count}, matched: {match_count}, results: {list(results.keys())}")
        except Exception as e:
            print(f"[DEBUG] Exception in collect(): {e}")
            logger.exception("Failed to collect process metrics")
            raise
        return results


class ProcessMonitor:
    """Interactive Jupyter widget for monitoring process resources.

    Provides a dashboard with start/stop controls and real-time plots
    of process metrics. Can monitor multiple processes simultaneously.

    Args:
        process_names: List of process name patterns to monitor.
        refresh_interval: Seconds between metric updates. Defaults to 2.0.

    Example:
        >>> monitor = ProcessMonitor(
        ...     ["org.apache.spark.deploy.master.Master"],
        ...     refresh_interval=2.0,
        ... )
        >>> monitor.show()
    """

    def __init__(
        self,
        process_names: Sequence[str] | None = None,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
    ) -> None:
        self.process_names = list(process_names) if process_names is not None else []
        self.user = getpass.getuser()
        self.refresh_interval = refresh_interval
        self.collector = ProcessMetricCollector(self.process_names)
        self.running = False
        self._plot_widget: go.FigureWidget | None = None

        # Initialize UI controls
        self._btn_start = widgets.Button(
            description="▶  Start",
            button_style="success",
            layout=widgets.Layout(width="120px"),
        )
        self._btn_start.on_click(self._on_start)

        self._btn_stop = widgets.Button(
            description="■  Stop",
            button_style="danger",
            layout=widgets.Layout(width="120px"),
            disabled=True,
        )
        self._btn_stop.on_click(self._on_stop)

        self._interval_slider = widgets.FloatSlider(
            value=self.refresh_interval,
            min=MIN_REFRESH_INTERVAL,
            max=MAX_REFRESH_INTERVAL,
            step=0.5,
            description="Interval (sec):",
            style={"description_width": "90px"},
            layout=widgets.Layout(width="340px"),
        )
        self._interval_slider.observe(self._on_interval_change, names="value")

        self._controls = widgets.HBox([
            self._btn_start,
            self._btn_stop,
            self._interval_slider,
        ])
        self._dashboard = widgets.VBox([self._controls])

    def show(self) -> None:
        """Display the monitoring dashboard in the Jupyter notebook."""
        display(self._dashboard)

    def get_ui(self) -> widgets.VBox:
        """Get the dashboard widget for embedding in other UIs.

        Returns:
            The dashboard VBox widget containing controls and plot.
        """
        return self._dashboard

    def set_process_names(self, process_names: Sequence[str]) -> None:
        """Update the process names to monitor.

        Args:
            process_names: List of process name patterns to monitor.
        """
        self.process_names = list(process_names)
        self.collector = ProcessMetricCollector(self.process_names)
        self._plot_widget = None  # Reset plot to use new processes

    def _start_collecting(self) -> None:
        """Start the metrics collection in a background thread."""
        from multiprocessing import Process

        if self.running:
            return
        self.running = True
        self._btn_start.disabled = True
        self._btn_stop.disabled = False
        # collect_thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._collect_process = Process(target=self._collect_loop,daemon=True)
        self._collect_process.start()
        #collect_thread.start()
        

    def _stop_collecting(self) -> None:
        """Stop the metrics collection."""
        self.running = False
        self._btn_start.disabled = False
        self._btn_stop.disabled = True

    def _collect_loop(self) -> None:
        """Main collection loop running in background thread."""
        print(f"[DEBUG] _collect_loop started, running={self.running}")
        while self.running:
            print(f"[DEBUG] collecting metrics, running={self.running}")
            metrics = self.collector.collect()
            print(f"[DEBUG] Rendering metrics, running={self.running}")
            self._render_metrics(metrics)
            time.sleep(int(self.refresh_interval))
        print(f"[DEBUG] _collect_loop EXITED, running={self.running}")


    def _on_start(self,b) -> None:
        """Handle start button click.

        Args:
            button: The button widget that triggered the event.
        """
        self._start_collecting()

    def _on_stop(self,b) -> None:
        """Handle stop button click.

        Args:
            button: The button widget that triggered the event.
        """
        self._stop_collecting()

    def _on_interval_change(self, change: dict[str, Any]) -> None:
        """Handle refresh interval slider change.

        Args:
            change: Dictionary containing the change event data.
        """
        self.refresh_interval = change["new"]

    def _get_metric_deque_value(self, history: ProcessMetricsHistory, metric: str) -> list[float | int]:
        """Get the current values for a specific metric from history.

        Args:
            history: The process metrics history.
            metric: The metric name to retrieve.

        Returns:
            List of metric values.
        """
        attr_map = {
            "cpu": "cpu",
            "mem_pct": "mem_pct",
            "mem_rss": "mem_rss",
            "mem_vms": "mem_vms",
            "threads": "threads",
            "io_read": "io_read",
            "io_write": "io_write",
        }
        attr_name = attr_map[metric]
        return list(getattr(history, attr_name))

    def _render_metrics(self, metrics: dict[str, dict[str, Any]]) -> None:
        """Render or update the metrics visualization.

        Args:
            metrics: Dictionary of process metrics from collector.
        """
        print(f"[DEBUG] Rendering metric.")

        if not metrics:
            return

        # Get list of metric keys (excluding timestamp)
        history_keys = [
            k for k in vars(next(iter(metrics.values()))["history"])
            if k != "timestamp"
        ]
        
        if self._plot_widget is None:
            self._create_plot(metrics, history_keys)
        else:
            self._update_plot(metrics, history_keys)

    def _create_plot(
        self,
        metrics: dict[str, dict[str, Any]],
        history_keys: list[str],
    ) -> None:
        """Create the initial plot widget.

        Args:
            metrics: Dictionary of process metrics.
            history_keys: List of metric keys to display.
        """
        num_procs = len(metrics)
        num_metrics = len(history_keys)

        # Create subplot grid
        fig = make_subplots(
            rows=num_procs,
            cols=num_metrics,
            subplot_titles=[f"{m}" for _ in metrics for m in history_keys],
        )

        # Convert to interactive FigureWidget
        self._plot_widget = go.FigureWidget(fig)
        self._plot_widget.layout.height = PLOT_HEIGHT_PER_PROCESS * num_procs
        self._plot_widget.layout.width = PLOT_WIDTH
        self._plot_widget.layout.margin = dict(l=20, r=20, t=40, b=20)
        self._plot_widget.layout.showlegend = False

        # Add empty lines for each metric
        for proc_idx, (proc_name, _) in enumerate(metrics.items()):
            for metric_idx, _ in enumerate(history_keys):
                self._plot_widget.add_scatter(
                    y=[],
                    row=proc_idx + 1,
                    col=metric_idx + 1,
                    mode="lines",
                    name=f"{proc_name}",
                )

        # Add plot to dashboard
        self._dashboard.children = [self._controls, self._plot_widget]

    def _update_plot(
        self,
        metrics: dict[str, dict[str, Any]],
        history_keys: list[str],
    ) -> None:
        """Update existing plot with new data.

        Args:
            metrics: Dictionary of process metrics.
            history_keys: List of metric keys to update.
        """
        if self._plot_widget is None:
            return

        # Batch update prevents redraw until all data is updated
        with self._plot_widget.batch_update():
            trace_idx = 0
            for proc_data in metrics.values():
                history = proc_data["history"]
                for metric in history_keys:
                    values = self._get_metric_deque_value(history, metric)
                    self._plot_widget.data[trace_idx].y = values
                    trace_idx += 1
