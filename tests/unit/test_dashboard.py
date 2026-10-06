"""Unit tests for :mod:`bijuty.monitoring.dashboard`.

The dashboard owns the metric refresh loop (the "listener" that pulls from a
collector and pushes into the plot widgets). These tests stay hermetic by
faking the collector, the background thread and the plotly figure builder;
they never start a real thread or require ``anywidget``.
"""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import ipywidgets as widgets
import pytest

import bijuty.monitoring.dashboard as dashboard_mod
from bijuty.monitoring.dashboard import (
    DEFAULT_REFRESH_INTERVAL,
    MetricDashboard,
)
from bijuty.monitoring.process import ProcessMetricsHistory


class DummyDashboard(MetricDashboard):
    """Concrete dashboard exposing a deterministic metric display name."""

    def _get_metric_display_name(self, metric: str) -> str:
        return metric.upper()


class FakeFigure(widgets.VBox):
    """Minimal widget stand-in for a plotly ``FigureWidget``.

    It is a real ``ipywidgets`` widget so it can be mounted in the plot
    container, while exposing the tiny ``data``/``batch_update`` surface the
    dashboard touches during rendering.
    """

    def __init__(self):
        super().__init__(children=())
        self.data = []

    def batch_update(self):
        return contextlib.nullcontext()


@pytest.fixture
def dashboard() -> DummyDashboard:
    return DummyDashboard(collector=MagicMock())


class TestDashboardConstruction:
    def test_defaults_and_controls(self, dashboard):
        assert dashboard.refresh_interval == DEFAULT_REFRESH_INTERVAL
        assert dashboard.running is False
        assert isinstance(dashboard.get_ui(), widgets.VBox)
        assert len(dashboard.get_ui().children) == 2

    def test_extra_header_widgets_are_embedded(self):
        extra = widgets.HTML("extra")

        dash = DummyDashboard(collector=MagicMock(),
                              extra_header_widgets=[extra])

        header = dash.get_ui().children[0]
        assert extra in header.children

    def test_base_display_name_is_abstract(self):
        base = MetricDashboard(collector=MagicMock())

        with pytest.raises(NotImplementedError):
            base._get_metric_display_name("cpu")


class TestRefreshInterval:
    def test_interval_observer_updates_refresh_interval(self, dashboard):
        dashboard._interval_slider.value = 4.5

        assert dashboard.refresh_interval == 4.5

    def test_on_interval_change_reads_new_value(self, dashboard):
        dashboard._on_interval_change({"new": 3.0})

        assert dashboard.refresh_interval == 3.0


class TestCollectionLifecycle:
    @pytest.fixture
    def fake_thread(self, monkeypatch):
        created = {}

        class _FakeThread:
            def __init__(self, target=None, daemon=None, **kwargs):
                self.target = target
                self.daemon = daemon
                self.started = False
                self.joined = False
                created["thread"] = self

            def start(self):
                self.started = True

            def join(self):
                self.joined = True

        monkeypatch.setattr(dashboard_mod.threading, "Thread", _FakeThread)
        return created

    def test_start_collecting_toggles_state_and_buttons(
            self, dashboard, fake_thread):
        dashboard._start_collecting()

        assert dashboard.running is True
        assert dashboard._btn_start.disabled is True
        assert dashboard._btn_stop.disabled is False
        assert fake_thread["thread"].started is True
        assert fake_thread["thread"].target == dashboard._collect_loop

    def test_start_is_idempotent(self, dashboard, fake_thread):
        dashboard._start_collecting()
        first = fake_thread["thread"]

        dashboard._start_collecting()

        assert fake_thread["thread"] is first
        assert dashboard.running is True

    def test_stop_collecting_resets_state(self, dashboard, fake_thread):
        dashboard._start_collecting()

        dashboard._stop_collecting()

        assert dashboard.running is False
        assert dashboard._btn_start.disabled is False
        assert dashboard._btn_stop.disabled is True
        assert fake_thread["thread"].joined is True
        assert dashboard._collect_process_stop_event.is_set() is True


class TestRendering:
    def test_render_metrics_noop_for_empty_payload(self, dashboard):
        dashboard._render_metrics({})

        assert dashboard._plot_widget.children == ()

    def test_update_plot_noop_when_no_plots(self, dashboard):
        # Must return before dereferencing missing plot state.
        dashboard._update_plot("Master", {"history": ProcessMetricsHistory()}, [])

        assert dashboard._process_plots == {}

    def test_render_metrics_creates_and_registers_plot(self, dashboard):
        history = ProcessMetricsHistory()
        history.cpu[-1] = 12.0
        payload = {"Master": {"history": history, "found": True}}

        with patch.object(dashboard, "_build_process_figure",
                          return_value=FakeFigure()) as build:
            dashboard._render_metrics(payload)

        build.assert_called_once()
        assert "Master" in dashboard._process_plots
        assert dashboard._process_plots["Master"]["latest_data"] is payload["Master"]
        assert len(dashboard._plot_widget.children) == 1

        history_keys = dashboard._process_plots["Master"]["history_keys"]
        assert "cpu" in history_keys
        assert "timestamp" not in history_keys

    def test_render_metrics_updates_existing_plot(self, dashboard):
        history = ProcessMetricsHistory()
        dashboard._process_plots["Master"] = {"latest_data": None}

        with patch.object(dashboard, "_update_plot") as update:
            dashboard._render_metrics({
                "Master": {"history": history, "found": True},
            })

        update.assert_called_once()
        assert dashboard._process_plots["Master"]["latest_data"]["found"] is True
