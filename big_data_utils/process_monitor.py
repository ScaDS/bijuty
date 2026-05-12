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
from IPython.display import display, clear_output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from multiprocessing import Process
from .utils import debug_write_to_file
# import plotly.io as pio
# pio.renderers.default = "browser"

#from .utils import #logger

if TYPE_CHECKING:
    from collections.abc import Sequence

# Constants
BYTES_TO_MB = 1024 * 1024
DEFAULT_HISTORY_SIZE = 40
DEFAULT_REFRESH_INTERVAL = 2.0
MIN_REFRESH_INTERVAL = 0.5
MAX_REFRESH_INTERVAL = 10.0
PLOT_HEIGHT_PER_PROCESS = 300
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
        process_names: Sequence[dict] | None = None,
        history_size: int = DEFAULT_HISTORY_SIZE,
    ) -> None:
        self.process_names = list(process_names) if process_names is not None else []
        # print("In collector\n",self.process_names)
        
        self.user = getpass.getuser()
        self._history_size = history_size
        self.history: dict[str, ProcessMetricsHistory] = {
            name.get("title"): ProcessMetricsHistory() for name in self.process_names
        }

    def _match_process(self, proc: psutil.Process) -> str | None:
        """Check if a process matches any of the monitored process names.

        Args:
            proc: The process to check.

        Returns:
            The matching process name pattern, or None if no match.
        """
        #print(f"[DEBUG] _match_process entered with pid={proc.pid}")
        # Quick check: skip kernel threads and invalid processes
        # if proc.pid <= 2:
        #     print(f"[DEBUG] Skipping pid {proc.pid} (<=2)")
        #     return None
        try:
            #print(f"[DEBUG] Checking status of pid {proc.pid}")
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
            # if proc.pid % 1000 == 0:  # Sample some PIDs
            #     print(f"[DEBUG] Checking PID {proc.pid}: user={proc_user}, cmdline[:100]={cmdline[:100]}")
            for proc_i in self.process_names:
                pattern = proc_i.get("pattern")
                if pattern in cmdline:
                    ##logger.debug(f"Match found! name='{name}' in cmdline of PID {proc.pid}")
                    return proc_i
        except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
            #logger.debug("Could not access process %s info: %s", proc.pid, e)
            print("Could not access process %s info: %s", proc.pid, e)
        except Exception as e:
            #logger.warning("Unexpected error accessing process %s: %s", proc.pid, e)
            print("Unexpected error accessing process %s: %s", proc.pid, e)

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
            #logger.debug("Failed to extract metrics from process %s: %s", proc.pid, e)
            print("Failed to extract metrics from process %s: %s", proc.pid, e)
        except Exception as e:
            #logger.warning("Unexpected error extracting metrics from process %s: %s", proc.pid, e)
            print("Unexpected error extracting metrics from process %s: %s", proc.pid, e)

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
        ##logger.debug(f"collect() called, process_names={self.process_names}")
        if not self.process_names:
            #logger.debug("No process_names configured, returning empty results")
            return results
        try:
            #proc_count = 0
            match_count = 0
            ##logger.debug("Starting process_iter()")
            for proc in psutil.process_iter(['pid', 'name']):
                proc_found = self._match_process(proc)
                if proc_found is None:
                    continue
                match_count += 1
                metrics = self._extract_metrics(proc)
                if metrics is None:
                    #logger.debug(f"Failed to extract metrics for {name}")
                    continue
                proc_found_name = proc_found.get("title") 
                self.history[proc_found_name].append(metrics)
                results[proc_found_name] = {"found": True, "history": self.history[proc_found_name], "proc_info":proc}
                ##logger.debug(f"Collected metrics for {name}: cpu={metrics.cpu_percent:.1f}%")
            ##logger.debug(f"Total processed: {proc_count}, matched: {match_count}, results: {list(results.keys())}")
        except Exception as e:
            #logger.error(f"Exception in collect(): {e}")
            #logger.exception("Failed to collect process metrics")
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
        self._process_plots: dict[str, dict[str, Any]] = {}

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

        self._plot_widget = widgets.VBox([],layout=widgets.Layout(
            width='100%',
            margin='5px 0px',
            display='flex',
            flex_flow='column',
        ))

        self._dashboard = widgets.VBox([self._controls,self._plot_widget])

    def show(self) -> None:
        """Display the monitoring dashboard in the Jupyter notebook."""
        display(self._dashboard)

    def get_ui(self) -> widgets.VBox:
        """Get the dashboard widget for embedding in other UIs.

        Returns:
            The dashboard VBox widget containing controls and plot.
        """
        return self._dashboard
    
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
        # print("In monitor, setting process\n",self.process_names)
        self.collector = ProcessMetricCollector(self.process_names)
        #self._plot_widget = {}  # Reset plot to use new processes

    def _start_collecting(self) -> None:
        """Start the metrics collection in a background thread."""
        if len(self.process_names) == 0:
            #logger.debug("Process name list is empty")
            return
        
        if self.running:
            return
        self.running = True
        self._btn_start.disabled = True
        self._btn_stop.disabled = False
        self._collect_process_stop_event = threading.Event()
        self._collect_process = threading.Thread(target=self._collect_loop, daemon=True)
        # self._collect_process = Process(target=self._collect_loop,daemon=True)
        
        self._collect_process.start()
        #collect_thread.start()
        

    def _stop_collecting(self) -> None:
        """Stop the metrics collection."""
        self.running = False
        self._btn_start.disabled = False
        self._btn_stop.disabled = True
        self._collect_process_stop_event.set()
        self._collect_process.join()
        
    def _collect_loop(self) -> None:
        """Main collection loop running in background thread."""
        while self.running:
            metrics = self.collector.collect()
            self._render_metrics(metrics)
            time.sleep(self.refresh_interval)


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

    def _get_metric_display_name(self, metric: str) -> str:
        """Get the display name for a metric key.

        Args:
            metric: The metric key.

        Returns:
            The human-readable display name for the metric.
        """
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

    def _render_metrics(self, metrics: dict[str, dict[str, Any]]) -> None:
        """Render or update the metrics visualization.

        Args:
            metrics: Dictionary of process metrics from collector.
        """
        

        if not metrics:
            return

        # Get list of metric keys (excluding timestamp)
        history_keys = [
            k for k in vars(next(iter(metrics.values()))["history"])
            if k != "timestamp"
        ]
        
        with self._dashboard.hold_trait_notifications():
            for proc_name, data in metrics.items():
                if proc_name not in self._process_plots:
                    self._create_plot(proc_name, history_keys)
                
                proc_info = self._process_plots[proc_name]
                proc_info['latest_data'] = data
                self._update_plot(proc_name, data, history_keys)

    def _calculate_stats(self, values: list[float | int]) -> tuple[float, float, float]:
        """Calculate min, max, and mean from a list of values.

        Args:
            values: List of numeric values.

        Returns:
            Tuple of (min, max, mean) values.
        """
        if not values:
            return 0.0, 0.0, 0.0
        return min(values), max(values), sum(values) / len(values)

    def _get_stat_trace_indices(self, proc_idx: int, num_metrics: int) -> list[int]:
        """Get trace indices for stat traces (mean, max, min) for a given process.

        Args:
            proc_idx: Index of the process (0-based).
            num_metrics: Number of metrics per process.

        Returns:
            List of trace indices corresponding to stat traces.
        """
        indices = []
        base = proc_idx * num_metrics * 4
        for metric_idx in range(num_metrics):
            # Each metric has 4 traces: data, mean, max, min
            # Stat traces are at positions 1, 2, 3 within each metric group
            indices.extend([base + metric_idx * 4 + 1, base + metric_idx * 4 + 2, base + metric_idx * 4 + 3])
        return indices

    def _build_process_figure(
        self,
        proc_name: str,
        active_metrics: list[str],
    ) -> go.FigureWidget:
        """Build a FigureWidget with subplots for active metrics only.

        Args:
            proc_name: Name of the process.
            active_metrics: List of currently active metric names.

        Returns:
            A new FigureWidget with min/max/mean overlays.
        """
        num_metrics = len(active_metrics)

        fig = make_subplots(
            rows=1,
            cols=num_metrics,
            subplot_titles=[self._get_metric_display_name(m) for m in active_metrics],
        )

        # Convert to interactive FigureWidget
        proc_plot: go.FigureWidget = go.FigureWidget(fig)

        proc_plot.layout.height = PLOT_HEIGHT_PER_PROCESS
        #proc_plot.layout.width = PLOT_WIDTH
        proc_plot.layout.margin = dict(l=50, r=20, t=50, b=50)
        proc_plot.layout.showlegend = False
        proc_plot.update_layout(autosize=True)

        for metric_idx, metric in enumerate(active_metrics):
            row = 1
            col = metric_idx + 1

            base_trace = dict(row=row, col=col, x=[], y=[], mode="lines")

            # Main Data Trace (solid line)
            proc_plot.add_scatter(
                **base_trace,
                name=f"data_{metric}",
                line=dict(width=2),
            )

            # Mean Trace (dashed, red, semi-transparent)
            proc_plot.add_scatter(
                **base_trace,
                name=f"mean_{metric}",
                visible=True,
                legendgroup="mean",
                line=dict(dash='dash', width=1, color='rgba(255, 0, 0, 0.5)'),
            )

            # Max Trace (dotted, green, semi-transparent)
            proc_plot.add_scatter(
                **base_trace,
                name=f"max_{metric}",
                visible=True,
                legendgroup="max",
                line=dict(dash='dot', width=1, color='rgba(0, 128, 0, 0.4)'),
            )

            # Min Trace (dotted, blue, semi-transparent)
            proc_plot.add_scatter(
                **base_trace,
                name=f"min_{metric}",
                visible=True,
                legendgroup="min",
                line=dict(dash='dot', width=1, color='rgba(0, 0, 255, 0.4)'),
            )

        # Add toggle button for stats
        stat_indices = [i for i, t in enumerate(proc_plot.data) if "data_" not in t.name]
        proc_plot.update_layout(
            updatemenus=[dict(
                type="buttons",
                xanchor="left",
                yanchor="top",
                x=0,
                y=1.5,
                direction="right",
                font=dict(size=10, color="black"),
                # pad={"r": 1, "l": 1, "t": 1, "b":1},
                buttons=[
                    dict(
                        label="Stats Off",
                        method="restyle",
                        args=[{"visible": [False], "showlegend": [False]}, stat_indices],
                    ),
                    dict(
                        label="Stats On",
                        method="restyle",
                        args=[{"visible": [True], "showlegend": [True]}, stat_indices],
                    ),
                ],
            )]
        )


        # Style axes
        proc_plot.update_xaxes(title_text="Time", tickfont_size=9)
        proc_plot.update_yaxes(tickfont_size=9)

        return proc_plot

    def _create_plot(
        self,
        proc_name: str,
        history_keys: list[str],
    ) -> None:
        """Create the initial plot widget for a process with metric checkboxes.

        Args:
            proc_name: Name of the process.
            history_keys: List of metric keys to display.
        """

        #title_wdg: widgets.Text = widgets.Text(proc_name)
        title_wdg: widgets.HTML = widgets.HTML(
            f"""<div class="plot_row_title">{proc_name}</div>"""
        )
        # title_wdg.add_class("plot_row_title")

        checkboxes: dict[str, widgets.Checkbox] = {}
        for metric in history_keys:
            wdg_checkbox = widgets.Checkbox(
                value=True,
                description=self._get_metric_display_name(metric),
                indent=False
            )
            wdg_checkbox.add_class("plot_row_check_box")
            checkboxes[metric] = wdg_checkbox

        wdg_metric_selector: widgets.HBox = widgets.HBox(list(checkboxes.values()))
        wdg_metric_selector.add_class("plot_row_metric_selector")

        def _rebuild() -> None:
            active_metrics = [m for m in history_keys if checkboxes[m].value]

            proc_plot = self._build_process_figure(proc_name, active_metrics)

            plot_info = self._process_plots[proc_name]
            plot_info['figure'] = proc_plot
            plot_info['active_metrics'] = active_metrics
            plot_info['container'].children = [title_wdg,wdg_metric_selector, proc_plot]

            # Populate with latest data if available
            if plot_info.get('latest_data'):
                self._update_plot(proc_name, plot_info['latest_data'], history_keys)

        for cb in checkboxes.values():
            cb.observe(lambda change: _rebuild(), names='value')

        active_metrics = list(history_keys)
        proc_plot = self._build_process_figure(proc_name, active_metrics)
        
        container: widgets.VBox = widgets.VBox(
            [title_wdg, wdg_metric_selector, proc_plot],
            # layout=widgets.Layout(width="100%", border="1px solid #eee", margin="5px", background_color="white"),
        )
        container.add_class("plot_row") # Check style.css

        self._process_plots[proc_name] = {
            'container': container,
            'figure': proc_plot,
            'checkboxes': checkboxes,
            'history_keys': history_keys,
            'active_metrics': active_metrics,
        }
        self._plot_widget.children += (container,)

    def _update_plot(
        self,
        proc_name: str,
        proc_metric_data: dict,
        history_keys: list[str],
    ) -> None:
        """Update existing plot with new data including min/max/mean.

        Args:
            proc_name: Process name that needs to be updated.
            proc_metric_data: Metrics data from collector.
            history_keys: List of metric keys to update.
        """
        if len(self._plot_widget.children) == 0:
            return

        # Look up current plot and its active metrics
        proc_info = self._process_plots[proc_name]
        proc_fig = proc_info['figure']

        # Build a name -> trace index map for this figure
        trace_name_to_idx = {t.name: i for i, t in enumerate(proc_fig.data)}

        timestamps = list(proc_metric_data["history"].timestamp)
        history = proc_metric_data["history"]

        with proc_fig.batch_update():
            for metric in proc_info.get('active_metrics', history_keys):
                values = self._get_metric_deque_value(history, metric)
                if not values:
                    continue

                # Calculate statistics
                y_mean = [sum(values) / len(values)] * len(values) if values else []
                y_max = [max(values)] * len(values) if values else []
                y_min = [min(values)] * len(values) if values else []

                # Update Main Line
                idx = trace_name_to_idx.get(f"data_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = values

                # Update Mean Line
                idx = trace_name_to_idx.get(f"mean_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = y_mean

                # Update Max Line
                idx = trace_name_to_idx.get(f"max_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = y_max

                # Update Min Line
                idx = trace_name_to_idx.get(f"min_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = y_min