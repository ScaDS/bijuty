"""Integration tests exercising the GUI orchestration end to end."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import ipywidgets as widgets
import pytest

import bijuty.gui.main as mainmod
from bijuty.gui.config import FRAMEWORK_REGISTRY

pytestmark = pytest.mark.integration


@pytest.fixture
def gui(monkeypatch, fake_slurm):
    """A fully launched :class:`GUIMain` using a deterministic SLURM double."""

    monkeypatch.setattr(mainmod, "SlurmManager", lambda *a, **k: fake_slurm)
    monkeypatch.setattr(mainmod, "fetch_image", lambda url: b"")

    instance = mainmod.GUIMain()
    instance.launch_gui_config(display_gui=False)
    return instance


class TestGUILaunch:
    def test_launch_returns_container_with_expected_sections(self, gui):
        container = gui.main_container

        assert isinstance(container, widgets.VBox)
        assert len(container.children) == 5
        assert gui.widgets["output_area"] is not None

    def test_default_framework_is_spark(self, gui):
        assert gui.get_selected_framework_name() == "SPARK"
        assert gui.selected_framework is FRAMEWORK_REGISTRY["SPARK"]

    def test_resource_defaults_come_from_slurm_double(self, gui):
        assert gui.get_selected_master_host() == "node1"
        assert gui.get_selected_workers() == ["node1"]
        assert gui.get_selected_master_port() == "7077"

    def test_config_destination_uses_framework_name(self, gui):
        destination = gui.get_selected_config_destination()

        assert destination.endswith(os.path.join("", "spark"))
        assert gui.get_selected_log_dir().endswith(os.path.join("spark", "log"))

    def test_default_template_is_selected(self, gui):
        assert gui.is_default_config_template() is True

    def test_viz_proportions_expose_all_keys(self, gui):
        props = gui._get_viz_proportions()

        assert set(props) == {
            "master_node", "worker_node", "total_mem_val", "drv_mem_val",
            "wrk_mem_val", "exe_mem_val", "total_cpu_val", "drv_cpu_val",
            "wrk_cpu_val", "exe_cpu_val",
        }
        assert props["master_node"] == "node1"
        assert props["total_cpu_val"] == "4"

    def test_process_visualization_renders_without_error(self, gui):
        gui._update_process_viz()

        assert "Error" not in gui.wdg_viz_display.value
        assert "Slurm Job" in gui.wdg_viz_display.value

    def test_cluster_info_widget_rendered(self, gui):
        gui._update_cluster_info()

        assert "Cluster Status" in gui.widgets["cluster_info"].value


class TestRandomizeMasterPort:
    def test_randomize_port_uses_selected_master_host(self, gui, monkeypatch):
        recorded = {}

        def fake_find_first_available_port(**kwargs):
            recorded.update(kwargs)
            return 7078

        monkeypatch.setattr(
            mainmod, "find_first_available_port", fake_find_first_available_port)
        gui.widgets["randomize_port"].value = True

        assert gui.get_selected_master_port() == "7078"
        assert recorded == {"start_port": 7077, "host": "node1"}

    def test_randomize_flink_port_uses_selected_master_host(self, gui, monkeypatch):
        recorded = {}

        def fake_find_first_available_port(**kwargs):
            recorded.update(kwargs)
            return 6124

        monkeypatch.setattr(
            mainmod, "find_first_available_port", fake_find_first_available_port)
        gui.widgets["framework"].value = "FLINK"
        gui.widgets["randomize_port"].value = True

        assert gui.get_selected_master_port() == "6124"
        assert recorded == {"start_port": 6123, "host": "node1"}


class TestFrameworkSwitching:
    def test_switching_to_flink_updates_derived_values(self, gui):
        gui.widgets["framework"].value = "FLINK"

        assert gui.get_selected_framework_name() == "FLINK"
        assert gui.get_selected_master_port() == "6123"
        assert gui.get_selected_config_destination().endswith("flink")

    def test_framework_home_label_updates(self, gui):
        gui.widgets["framework"].value = "FLINK"

        checkbox = gui.widgets["framework_home"].children[0]
        assert checkbox.description == "Use custom FLINK_HOME"


class TestClusterButtonToggles:
    def test_toggle_all_disabled(self, gui):
        gui._toggle_cluster_buttons(all_disabled=True)

        assert gui.widgets["start_cluster"].disabled is True
        assert gui.widgets["stop_cluster"].disabled is True

    def test_toggle_specific_flags(self, gui):
        gui._toggle_cluster_buttons(start_disabled=True, stop_disabled=False)

        assert gui.widgets["start_cluster"].disabled is True
        assert gui.widgets["stop_cluster"].disabled is False


class TestMultiFrameworkManager:
    def test_add_and_close_tabs(self, monkeypatch):
        import bijuty.gui.multi_framework_manager as mfm

        class FakeGUI:
            def __init__(self):
                self.main_container = widgets.HTML("tab")

            def launch_gui_config(self, display_gui=False):
                return self.main_container

        monkeypatch.setattr(mfm, "GUIMain", FakeGUI)

        manager = mfm.MultiFrameworkManager()
        root = manager.build_widget()

        assert root is manager._root_layout
        assert manager.get_tab_count() == 1
        assert len(manager._tabs.children) == 1

        manager._on_add_tab(None)
        assert manager.get_tab_count() == 2
        assert len(manager._tabs.children) == 2
        assert manager._tabs.selected_index == 1

        manager._on_close_tab(None)
        assert manager.get_tab_count() == 1

        # The last remaining tab is never closed.
        manager._on_close_tab(None)
        assert manager.get_tab_count() == 1

    def test_get_gui_returns_instance(self, monkeypatch):
        import bijuty.gui.multi_framework_manager as mfm

        class FakeGUI:
            def __init__(self):
                self.main_container = widgets.HTML("tab")

            def launch_gui_config(self, display_gui=False):
                return self.main_container

        monkeypatch.setattr(mfm, "GUIMain", FakeGUI)
        manager = mfm.MultiFrameworkManager()
        manager.build_widget()

        assert isinstance(manager.get_gui(0), FakeGUI)


class TestObserverDrivenState:
    """Observers must keep derived widget ranges and flags in sync.

    These exercise the callback wiring attached in
    :meth:`GUIMain._attach_widget_observers` and
    :meth:`GUIMain._setup_dynamic_ranges` without re-launching the GUI.
    """

    def test_driver_cpu_change_updates_worker_cpu_max(self, gui):
        # FakeSlurm exposes cpus_per_node == 4.
        gui.widgets["driver_cpu"].value = 3

        assert gui.widgets["worker_cpu"].max == 1

    def test_worker_cpu_change_updates_executor_cpu_max(self, gui):
        gui.widgets["worker_cpu"].value = 2

        assert gui.widgets["executor_cpu"].max == 2

    def test_driver_memory_change_updates_worker_memory_max(self, gui):
        # FakeSlurm memory_per_node_effective == 8000 MB.
        gui.widgets["driver_memory"].value = 3000

        assert gui.widgets["worker_memory"].max == 5000

    def test_worker_memory_change_updates_executor_memory_max(self, gui):
        gui.widgets["worker_memory"].value = 2000

        assert gui.widgets["executor_memory"].max == 2000

    def test_parameter_change_marks_configuration_stale(self, gui):
        gui.is_config_set = True

        gui.widgets["destination"].value = "/tmp/elsewhere"

        assert gui.is_config_set is False

    def test_master_host_change_refreshes_cluster_info(self, gui, monkeypatch):
        monkeypatch.setattr(gui.bdm, "is_cluster_up", lambda: True)

        gui.widgets["master_host"].value = "node2"

        assert "node2" in gui.widgets["cluster_info"].value


class TestClusterButtonHandlers:
    """Click handlers must update button state and swallow engine failures."""

    @pytest.fixture(autouse=True)
    def _isolate_handler_side_effects(self, gui, monkeypatch):
        # Avoid writing config to disk and starting real monitor threads.
        monkeypatch.setattr(gui, "_set_environment", lambda: None)
        monkeypatch.setattr(
            gui, "_start_stop_metric_dashboard", lambda start=False: None)
        gui.bdm = MagicMock()
        return gui

    def test_start_success_enables_stop_button(self, gui):
        gui.bdm.start_cluster.return_value = {"cluster": "up"}

        gui._on_start_cluster_clicked(None)

        gui.bdm.start_cluster.assert_called_once_with()
        assert gui.widgets["start_cluster"].disabled is True
        assert gui.widgets["stop_cluster"].disabled is False

    def test_start_failure_is_handled_and_controls_restored(self, gui):
        gui.bdm.start_cluster.side_effect = RuntimeError("engine unreachable")

        gui._on_start_cluster_clicked(None)

        assert gui.widgets["start_cluster"].disabled is False
        assert gui.widgets["stop_cluster"].disabled is True

    def test_stop_success_reenables_start_button(self, gui):
        gui.bdm.stop_cluster.return_value = {"cluster": "down"}

        gui._on_stop_cluster_clicked(None)

        gui.bdm.stop_cluster.assert_called_once_with()
        assert gui.widgets["start_cluster"].disabled is False
        assert gui.widgets["stop_cluster"].disabled is True

    def test_stop_failure_is_handled_and_controls_restored(self, gui):
        gui.bdm.stop_cluster.side_effect = RuntimeError("engine unreachable")

        gui._on_stop_cluster_clicked(None)

        assert gui.widgets["start_cluster"].disabled is True
        assert gui.widgets["stop_cluster"].disabled is False
