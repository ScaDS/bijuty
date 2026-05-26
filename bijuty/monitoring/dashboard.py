"""Common plotting dashboard for framework metric monitors.

Provides a reusable ``MetricDashboard`` base class that renders
real-time Plotly subplots with metric checkboxes, start/stop controls,
and rolling history tracking.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

import ipywidgets as widgets
import plotly.graph_objects as go
from IPython.display import display
from plotly.subplots import make_subplots

from ..gui.widgets import WidgetFactory


# =============================================================================
# Constants
# =============================================================================

DEFAULT_HISTORY_SIZE = 40
DEFAULT_REFRESH_INTERVAL = 2.0
MIN_REFRESH_INTERVAL = 0.5
MAX_REFRESH_INTERVAL = 10.0
PLOT_HEIGHT = 300


# =============================================================================
# Base Dashboard
# =============================================================================

class MetricDashboard:
    """Interactive Jupyter widget for monitoring application/process metrics.

    Subclasses provide a ``collector`` object (with a ``collect()`` method)
    and override ``_get_metric_display_name()``.
    """

    def __init__(
        self,
        collector: Any,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
        min_refresh: float = MIN_REFRESH_INTERVAL,
        max_refresh: float = MAX_REFRESH_INTERVAL,
        refresh_step: float = 0.5,
        plot_height: int = PLOT_HEIGHT,
        extra_header_widgets: Optional[List[widgets.Widget]] = None,
    ) -> None:
        self.collector = collector
        self.refresh_interval = refresh_interval
        self._min_refresh = min_refresh
        self._max_refresh = max_refresh
        self._refresh_step = refresh_step
        self._plot_height = plot_height
        self.running = False
        self._process_plots: Dict[str, Dict[str, Any]] = {}

        # Controls
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
            min=self._min_refresh,
            max=self._max_refresh,
            step=self._refresh_step,
            description="Interval (sec):",
            style={"description_width": "90px"},
            layout=widgets.Layout(width="340px"),
        )
        self._interval_slider.observe(self._on_interval_change, names="value")

        self._controls = widgets.HBox(
            [self._btn_start, self._btn_stop, self._interval_slider]
        )

        self._plot_widget = widgets.VBox(
            [],
            layout=widgets.Layout(
                width="100%",
                margin="5px 0px",
                display="flex",
                flex_flow="column",
            ),
        )

        self._dashboard = self._build_dashboard(extra_header_widgets)

    def _build_dashboard(
        self, extra_header_widgets: Optional[List[widgets.Widget]] = None
    ) -> widgets.VBox:
        header_children: List[widgets.Widget] = [self._controls]
        if extra_header_widgets:
            header_children.extend(extra_header_widgets)

        return widgets.VBox(
            [
                widgets.HBox(
                    header_children,
                    layout=widgets.Layout(justify_content="space-between"),
                ),
                self._plot_widget,
            ]
        )

    def show(self) -> None:
        """Display the monitoring dashboard."""
        display(self._dashboard)

    def get_ui(self) -> widgets.VBox:
        """Return the dashboard widget for embedding."""
        return self._dashboard

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _on_start(self, b: widgets.Button) -> None:
        self._start_collecting()

    def _on_stop(self, b: widgets.Button) -> None:
        self._stop_collecting()

    def _on_interval_change(self, change: Dict[str, Any]) -> None:
        self.refresh_interval = change["new"]

    # ------------------------------------------------------------------
    # Collection loop
    # ------------------------------------------------------------------

    def _start_collecting(self) -> None:
        if self.running:
            return
        self.running = True
        self._btn_start.disabled = True
        self._btn_stop.disabled = False
        self._collect_process_stop_event = threading.Event()
        self._collect_process = threading.Thread(target=self._collect_loop, daemon=True)
        self._collect_process.start()

    def _stop_collecting(self) -> None:
        self.running = False
        self._btn_start.disabled = False
        self._btn_stop.disabled = True
        self._collect_process_stop_event.set()
        self._collect_process.join()

    def _collect_loop(self) -> None:
        while self.running:
            metrics = self.collector.collect()
            self._render_metrics(metrics)
            time.sleep(self.refresh_interval)

    # ------------------------------------------------------------------
    # Metric helpers (subclasses may override)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_metric_deque_value(history: Any, metric: str) -> List[float | int]:
        return list(getattr(history, metric))

    def _get_metric_display_name(self, metric: str) -> str:
        """Return the human-readable label for a metric key.

        Subclasses **must** override this method.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render_metrics(self, metrics: Dict[str, Dict[str, Any]]) -> None:
        if not metrics:
            return

        history_keys = [
            k
            for k in vars(next(iter(metrics.values()))["history"])
            if k != "timestamp" and not k.startswith("_")
        ]

        with self._dashboard.hold_trait_notifications():
            for plot_id, data in metrics.items():
                if plot_id not in self._process_plots:
                    self._create_plot(plot_id, history_keys)

                proc_info = self._process_plots[plot_id]
                proc_info["latest_data"] = data
                self._update_plot(plot_id, data, history_keys)

    # ------------------------------------------------------------------
    # Plot building
    # ------------------------------------------------------------------

    def _build_process_figure(
        self, plot_id: str, active_metrics: List[str]
    ) -> go.FigureWidget:
        num_metrics = len(active_metrics)

        fig = make_subplots(
            rows=1,
            cols=num_metrics,
            subplot_titles=[self._get_metric_display_name(m) for m in active_metrics],
        )

        proc_plot: go.FigureWidget = go.FigureWidget(fig)
        proc_plot.layout.height = self._plot_height
        proc_plot.layout.margin = dict(l=50, r=20, t=50, b=50)
        proc_plot.layout.showlegend = False
        proc_plot.update_layout(autosize=True)

        for metric_idx, metric in enumerate(active_metrics):
            row, col = 1, metric_idx + 1
            base_trace = dict(row=row, col=col, x=[], y=[], mode="lines")

            # Main line
            proc_plot.add_scatter(
                **base_trace,
                name=f"data_{metric}",
                line=dict(width=2),
            )
            # Mean
            proc_plot.add_scatter(
                **base_trace,
                name=f"mean_{metric}",
                visible=True,
                legendgroup="mean",
                line=dict(dash="dash", width=1, color="rgba(255, 0, 0, 0.5)"),
            )
            # Max
            proc_plot.add_scatter(
                **base_trace,
                name=f"max_{metric}",
                visible=True,
                legendgroup="max",
                line=dict(dash="dot", width=1, color="rgba(0, 128, 0, 0.4)"),
            )
            # Min
            proc_plot.add_scatter(
                **base_trace,
                name=f"min_{metric}",
                visible=True,
                legendgroup="min",
                line=dict(dash="dot", width=1, color="rgba(0, 0, 255, 0.4)"),
            )

        # Stats toggle
        stat_indices = [
            i for i, t in enumerate(proc_plot.data) if "data_" not in t.name
        ]
        proc_plot.update_layout(
            updatemenus=[
                dict(
                    type="buttons",
                    xanchor="left",
                    yanchor="top",
                    x=0,
                    y=1.5,
                    direction="right",
                    font=dict(size=10, color="black"),
                    buttons=[
                        dict(
                            label="Stats Off",
                            method="restyle",
                            args=[
                                {"visible": [False], "showlegend": [False]},
                                stat_indices,
                            ],
                        ),
                        dict(
                            label="Stats On",
                            method="restyle",
                            args=[
                                {"visible": [True], "showlegend": [True]},
                                stat_indices,
                            ],
                        ),
                    ],
                )
            ]
        )

        proc_plot.update_xaxes(title_text="Time", tickfont_size=9)
        proc_plot.update_yaxes(tickfont_size=9)
        return proc_plot

    def _create_plot(self, plot_id: str, history_keys: List[str]) -> None:
        title_wdg = widgets.HTML(
            f"""<div class="plot_row_title">{plot_id}</div>"""
        )

        checkboxes: Dict[str, widgets.Checkbox] = {}
        for metric in history_keys:
            cb = widgets.Checkbox(
                value=True,
                description=self._get_metric_display_name(metric),
                indent=False,
            )
            cb.add_class("plot_row_check_box")
            checkboxes[metric] = cb

        selector = widgets.HBox(list(checkboxes.values()))
        selector.add_class("plot_row_metric_selector")

        def _rebuild() -> None:
            active_metrics = [m for m in history_keys if checkboxes[m].value]
            proc_plot = self._build_process_figure(plot_id, active_metrics)

            plot_info = self._process_plots[plot_id]
            plot_info["figure"] = proc_plot
            plot_info["active_metrics"] = active_metrics
            plot_info["container"].children = [title_wdg, selector, proc_plot]

            if plot_info.get("latest_data"):
                self._update_plot(plot_id, plot_info["latest_data"], history_keys)

        for cb in checkboxes.values():
            cb.observe(lambda change: _rebuild(), names="value")

        active_metrics = list(history_keys)
        proc_plot = self._build_process_figure(plot_id, active_metrics)

        container = widgets.VBox([title_wdg, selector, proc_plot])
        container.add_class("plot_row")

        self._process_plots[plot_id] = {
            "container": container,
            "figure": proc_plot,
            "checkboxes": checkboxes,
            "history_keys": history_keys,
            "active_metrics": active_metrics,
        }
        self._plot_widget.children += (container,)

    def _update_plot(
        self,
        plot_id: str,
        proc_metric_data: Dict[str, Any],
        history_keys: List[str],
    ) -> None:
        if len(self._plot_widget.children) == 0:
            return

        proc_info = self._process_plots[plot_id]
        proc_fig = proc_info["figure"]
        trace_name_to_idx = {t.name: i for i, t in enumerate(proc_fig.data)}
        timestamps = list(proc_metric_data["history"].timestamp)
        history = proc_metric_data["history"]

        with proc_fig.batch_update():
            for metric in proc_info.get("active_metrics", history_keys):
                values = self._get_metric_deque_value(history, metric)
                if not values:
                    continue

                y_mean = [sum(values) / len(values)] * len(values) if values else []
                y_max = [max(values)] * len(values) if values else []
                y_min = [min(values)] * len(values) if values else []

                idx = trace_name_to_idx.get(f"data_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = values

                idx = trace_name_to_idx.get(f"mean_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = y_mean

                idx = trace_name_to_idx.get(f"max_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = y_max

                idx = trace_name_to_idx.get(f"min_{metric}")
                if idx is not None:
                    proc_fig.data[idx].x = timestamps
                    proc_fig.data[idx].y = y_min
