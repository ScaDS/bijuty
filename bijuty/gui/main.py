"""
GUI utilities for configuring and managing big data frameworks (Spark, Flink).

This module provides the main GUI orchestration, event handling, and environment
setup for big data clusters using ipywidgets.
"""

from __future__ import annotations

import os
import socket
import traceback
from typing import Any, Dict, List, Optional
import io
from contextlib import redirect_stdout, redirect_stderr
import logging

import ipywidgets as widgets
from IPython.display import clear_output, display

from ..big_data_manager import BigDataManager
from .config import FRAMEWORK_REGISTRY
from .env_setup import GUIEnvSetup
from .html import HTMLGenerator
from .widgets import (
    WidgetFactory,
    VBox,
    HBox,
    create_placeholder_logo,
    fetch_image,
)
from ..slurm_utils import SlurmManager
from ..monitoring.process import ProcessMonitor
from ..monitoring.spark import SparkMetricMonitor
from ..monitoring.flink import FlinkMetricMonitor
from ..utils import get_file_content

logger = logging.getLogger(__name__)

# =============================================================================
# Main GUI Class
# =============================================================================

class GUIMain(GUIEnvSetup):
    """
    GUI utilities for configuring and managing big data frameworks.

    This class provides an interactive Jupyter notebook interface for:
    - Selecting big data frameworks (Spark, Flink)
    - Configuring cluster resources (CPU, Memory)
    - Visualizing resource allocation
    - Starting and stopping clusters
    """

    def __init__(self, default_framework: str | None = None):
        """Initialize the GUI utilities.

        Args:
            default_framework: Optional framework name to pre-select.
        """
        self.is_config_set = False
        self.user = os.environ.get("USER", "unknown")
        self.cluster_name = socket.getfqdn().strip()

        # Initialize managers
        self.slurm_info = SlurmManager()
        self.bdm = BigDataManager()
        self.process_monitor = ProcessMonitor(slurm_info=self.slurm_info)

        # Widget containers
        self.widgets: Dict[str, Any] = {}
        self._last_cluster_result: Optional[Any] = None

        # Display widget
        self.wdg_viz_display = widgets.HTML()

        # Main container (exposed for embedding in other UIs)
        self.main_container: widgets.Widget | None = None

        # Default framework override
        self._default_framework = default_framework

        # Debug flag
        self._debug_set_slurm_true = False
        if self._debug_set_slurm_true:
            self.slurm_info.in_slurm_job = True

    def _get_spark_monitor(self) -> SparkMetricMonitor:
        if not hasattr(self, "_spark_monitor"):
            self._spark_monitor = SparkMetricMonitor()
        return self._spark_monitor

    def _get_flink_monitor(self) -> FlinkMetricMonitor:
        if not hasattr(self, "_flink_monitor"):
            self._flink_monitor = FlinkMetricMonitor()
        return self._flink_monitor

    def _get_framework_monitor(self):
        fw_name = self.get_selected_framework_name()
        if fw_name and fw_name.lower() == "flink":
            return self._get_flink_monitor()
        return self._get_spark_monitor()

    def _update_metric_dashboard_widget(self) -> None:
        md = self.widgets.get("metric_dashboard")
        if md is not None:
            fw_monitor = self._get_framework_monitor()
            children = list(md.children)
            if len(children) >= 4:
                children[3] = fw_monitor.get_ui()
                md.children = tuple(children)

    # =========================================================================
    # Public API
    # =========================================================================

    def launch_gui_config(self, display_gui: bool = True) -> VBox:
        """Launch the main configuration GUI.

        Args:
            display_gui: If True, automatically display the GUI. If False,
                build and store the widget without displaying it.

        Returns:
            The top-level VBox widget containing the full GUI.
        """
        self._create_widgets()
        self._attach_widget_observers()
        self._setup_visualization_triggers()
        self._update_cluster_info()
        self.main_container = self._assemble_gui()
        if display_gui:
            display(self.main_container)
        return self.main_container

    def update_process_viz(self, change: Optional[Any] = None) -> None:
        """Update the process visualization display."""
        try:
            props = self._get_viz_proportions()
            html_template = HTMLGenerator.generate_viz_template(props, self.slurm_info)
            self.wdg_viz_display.value = html_template
        except Exception as e:
            self.wdg_viz_display.value = f"<div style='color: red;'>Error: {str(e)}</div>"

    def _log(self, message: str = "", msg_type: str = "info", wrap_function_stdout: bool = False, func: callable = None) -> None:
        """Log a message to the output area.

        Args:
            message: The message to log.
            msg_type: The type of message - "info", "error", or "debug".
            wrap_function_stdout: To output all stdout/stderr of the called function.
            func: The function to execute and capture output from (used with wrap_function_stdout=True).
        """
        if wrap_function_stdout:
            if func is None:
                raise ValueError("func must be provided when wrap_function_stdout=True")

            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()

            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = func()

            stdout_output = stdout_capture.getvalue()
            stderr_output = stderr_capture.getvalue()

            with self.widgets["output_area"]:
                if stdout_output:
                    logger.info(stdout_output)
                if stderr_output:
                    logger.error(stderr_output)
            return result
        else:
            with self.widgets["output_area"]:
                if msg_type == "error":
                    logger.error(message)
                elif msg_type == "debug":
                    logger.debug(message)
                else:
                    logger.info(message)

    def _set_environment(self) -> None:
        """Set up the environment when the load button is clicked."""
        self._log("Load button clicked!", "debug")

        #self._set_load_button_processing()

        #self.widgets["output_area"].clear_output()
        self._log(f"Setting environment for {self.get_selected_framework_name()}...")

        try:
            self._initialize_framework_config()
            self._update_environment(fw_name=self.get_selected_framework_name())
            self._initialize_big_data_manager()
            self._reinitalize_dashboard()
        except Exception as e:
            self._handle_setup_error(e)
        # finally:
        #     self.widgets["load_button"].disabled = False

    def _reinitalize_dashboard(self) -> None:
        self.process_monitor.set_process_names(self.bdm.get_fw_cluster_processes(all_procs=True))
        fw_monitor = self._get_framework_monitor()
        fw_monitor.set_monitor(user_input=self.bdm._user_inputs)
        self._update_metric_dashboard_widget()

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def selected_framework(self):
        """Get the currently selected framework configuration."""
        fw_name = self.get_selected_framework_name()
        return FRAMEWORK_REGISTRY[fw_name]

    # =========================================================================
    # Widget Creation Methods
    # =========================================================================

    def _create_widgets(self) -> None:
        """Create all GUI widgets."""
        self.widgets["header_config"] = self._create_header("Cluster Configurator")
        self.widgets["header_viz"] = self._create_header("Resource Allocation Overview")

        self.widgets["framework"] = self._create_framework_widget()
        self.widgets["logo"] = self._create_logo_widget()

        self.widgets["framework_home"] = self._create_framework_home_widget()

        self.widgets["template"] = self._create_template_widget()
        self.widgets["destination"] = self._create_destination_widget()
        self.widgets["master_host"] = self._create_master_host_widget()
        self.widgets["worker_hosts"] = self._create_worker_hosts_widget()

        self.widgets["driver_cpu"] = self._create_driver_cpu_widget()
        self.widgets["worker_cpu"] = self._create_worker_cpu_widget()
        self.widgets["executor_cpu"] = self._create_executor_cpu_widget()

        self.widgets["driver_memory"] = self._create_driver_memory_widget()
        self.widgets["worker_memory"] = self._create_worker_memory_widget()
        self.widgets["executor_memory"] = self._create_executor_memory_widget()

        self.widgets["randomize_port"] = self._create_randomize_port_widget()
        # self.widgets["load_button"] = self._create_load_button()
        self.widgets["output_area"] = self._create_output_area()

        self.widgets["start_cluster"] = self._create_start_cluster_button()
        self.widgets["stop_cluster"] = self._create_stop_cluster_button()

        self.widgets["metric_dashboard"] = self._create_metric_dashboard()
        self.widgets["framework_gui"] = self._create_framework_web_gui()
        self.widgets["cluster_info"] = self._create_cluster_info_widget()

    def _create_framework_web_gui(self) -> HBox:
        """Create a panel with simple buttons to open framework web UIs.

        When running remotely, displays SSH port-forwarding instructions so the
        user can open the links in a local browser.

        Returns:
            A VBox widget containing buttons and optional port-forward instructions.
        """
        fw_name = self.get_selected_framework_name()
        base_url = f"http://localhost"
        is_remote = self.slurm_info.in_slurm_job
        master_host = self.get_selected_master_host()

        rows: List[widgets.Widget] = []
        fw_config = FRAMEWORK_REGISTRY.get(fw_name)
        web_ui_links = fw_config.web_ui_links if fw_config else None

        def make_link_row(url: str, title: str) -> VBox:
            btn_widget = WidgetFactory.create_styled_button_redirect(
                description=f"Open {title}",
                url=url,
            )
            btn_widget = WidgetFactory.update_widget_state(btn_widget,disable=True)

            return btn_widget
        if web_ui_links:
            for port, title in web_ui_links:
                url = f"{base_url}:{port}/"
                rows.append(make_link_row(url, title))
        else:
            rows.append(
                widgets.HTML(
                    value='<div style="padding:10px;">No UI links available. Start the cluster to view web interfaces.</div>'
                )
            )

        if is_remote and web_ui_links:
            # Build port-forward instructions dynamically from registry
            ssh_parts = " ".join(
                f"-L {port}:{master_host}:{port}" for port, _ in web_ui_links
            )
            ssh_cmd = f"ssh {self.slurm_info.user}@{self.slurm_info.login_node} {ssh_parts} <jump-host>"

            instructions_html = HTMLGenerator.generate_ssh_instructions(ssh_cmd)
            instructions_widget = widgets.HTML(value=instructions_html)
            instructions_widget = WidgetFactory.update_widget_state(instructions_widget,disable=True)

        return VBox(
            [
                # self._create_header("Framework Web Interface"),
                widgets.HBox(rows, layout=widgets.Layout(width="100%", padding="8px", align_items="center", justify_content="center")),
                instructions_widget

            ],layout=widgets.Layout(width="50%", padding="8px")
        )

    def _create_cluster_info_widget(self) -> widgets.HTML:
        """Create an empty widget displaying current master and worker nodes."""
        return widgets.HTML()

    def _update_cluster_info(self, change: Optional[Any] = None) -> None:
        """Update the cluster info widget with current master, worker nodes and status."""
        info_widget = self.widgets.get("cluster_info")
        if info_widget is None:
            return
        running = self.bdm.is_cluster_up()
        status_color = "#28a745" if running else "#dc3545"
        status_text = "Running" if running else "Stopped"

        master = self.get_selected_master_host() if running else "-"
        master_port = self.get_selected_master_port() if running else "-"
        workers = self.get_selected_workers() if running else "-"
        workers_str = ", ".join(workers) if (workers and running) else "-"
        framework = self.get_selected_framework_name().lower()
        if framework in ["spark", "flink"]:
            info_widget.value = HTMLGenerator.generate_framework_cluster_info(
                framework=framework,
                status_color=status_color,
                status_text=status_text,
                master=master,
                master_port=master_port,
                workers_str=workers_str,
                is_config_set=self.is_config_set,
            )



    def _create_header(self, title: str) -> widgets.HTML:
        """Create the GUI header widget."""
        return widgets.HTML(HTMLGenerator.generate_header(title))

    def _create_framework_widget(self) -> widgets.Dropdown:
        """Create the framework selection widget."""
        framework_list = list(FRAMEWORK_REGISTRY.keys())
        default_value = self._default_framework if self._default_framework in framework_list else framework_list[1]
        return WidgetFactory.create_dropdown(
            options=framework_list,
            value=default_value,
            description="Framework:",
        )

    def _create_logo_widget(self) -> widgets.Widget:
        """Create the framework logo display widget."""
        try:
            fw_name = self.get_selected_framework_name()
            if fw_name is None:
                return create_placeholder_logo()

            logo_url = self.selected_framework.logo_url
            img_content = fetch_image(logo_url)

            return widgets.Image(
                value=img_content,
                # format="svg+xml",
                width=50,
                height=50,
            )
        except Exception as e:
            self._log(f"Error loading logo: {e}",msg_type="error")
            return create_placeholder_logo()

    def _create_framework_home_widget(self) -> VBox:
        """Create checkbox and path input for FRAMEWORK_HOME (SPARK/FLINK)."""
        fw_name = self.get_selected_framework_name()
        label_text = f"Use custom {fw_name}_HOME"

        checkbox = WidgetFactory.create_checkbox(
            value=False,
            description=label_text,
        )

        path_input = WidgetFactory.create_text(
            value="",
            description=f"",
            disabled=True,
            layout=widgets.Layout(
                margin="0px 0px 0px 210px",
                width="calc(100% - 210px)"
            )
        )

        def toggle_path_input(change: Dict[str, Any]) -> None:
            path_input.disabled = not change["new"]

        checkbox.observe(toggle_path_input, names="value")

        return widgets.VBox([checkbox, path_input])

    def _create_template_widget(self) -> VBox:
        """Create the template selection widget."""
        checkbox = WidgetFactory.create_checkbox(
            value=True,
            description="Use default template",
        )
        text = WidgetFactory.create_text(
            value="default",
            description="Path to config template:",
            disabled=checkbox.value,
        )

        def toggle_default(change: Dict[str, Any]) -> None:
            text.disabled = change["new"]

        checkbox.observe(toggle_default, names="value")
        return VBox([checkbox, text])

    def _create_destination_widget(self) -> widgets.Text:
        """Create the config destination path widget."""
        return WidgetFactory.create_text(
            value=os.getcwd(),
            description="Config destination path:",
        )

    def _create_master_host_widget(self) -> widgets.Dropdown:
        """Create the master host selection widget."""
        nodes = self.slurm_info.get_nodes_list()
        return WidgetFactory.create_dropdown(
            options=nodes,
            value=nodes[0] if nodes else None,
            description="Master Host:",
        )

    def _create_worker_hosts_widget(self) -> VBox:
        """Create the worker host selection widget."""
        node_options = self.slurm_info.get_nodes_list()
        checkboxes = [
            widgets.Checkbox(value=False, description=node, indent=False)
            for node in node_options
        ]
        # Check 1st box by default
        checkboxes[0].value = True

        checkbox_container = VBox(
            checkboxes,
            layout=widgets.Layout(max_height="200px", overflow_y="auto", border="1px solid #ddd")
        )

        label = widgets.HTML(value="<b>Worker Hosts:</b>")
        selected_display = widgets.HTML(value="<i>None selected</i>")

        def update_selection(change: Dict[str, Any]) -> None:
            selected = [cb.description for cb in checkboxes if cb.value]
            selected_display.value = f"<b>Selected:</b> {', '.join(selected) if selected else '<i>None</i>'}"
            self.update_process_viz() # Trigger added to update process vizualization

        for cb in checkboxes:
            cb.observe(update_selection, names="value")

        return VBox([label, checkbox_container, selected_display])

    def _create_driver_cpu_widget(self) -> widgets.IntSlider:
        """Create the driver CPU slider widget."""
        fw_name = self.get_selected_framework_name() or "SPARK"
        try:
            default_cpu_worker = FRAMEWORK_REGISTRY[fw_name].default_resources.get("cpu_worker", 1)
        except:
            default_cpu_worker = 1

        return WidgetFactory.create_slider(
            value=1,
            min_val=1,
            max_val=self.slurm_info.get_cpus_per_node() - default_cpu_worker,
            description="Coordinator Cores:",
        )

    def _create_worker_cpu_widget(self) -> widgets.IntSlider:
        """Create the worker CPU slider widget."""
        return WidgetFactory.create_slider(
            value=1,
            min_val=1,
            max_val=self.slurm_info.get_cpus_per_node() - self.get_selected_driver_cpu(),
            description="Cores Pool / Node:",
            tooltip='The number of CPU cores assigned to each node. This determines maximum cores to be made available for compute units on one node.\n- Spark: SPARK_WORKER_CORES\n- Flink: taskmanager.cpu.cores',
        )

    def _create_executor_cpu_widget(self) -> widgets.IntSlider:
        """Create the executor Cores slider widget."""
        return WidgetFactory.create_slider(
            value=1,
            min_val=1,
            max_val=self.get_selected_worker_cpu(),
            step=1,
            description="Cores / Compute Units:",
            tooltip="""The number of CPU cores assigned to each individual compute unit from the total pool set in "Core Pool per Node." This determines how many parallel compute units can be initialized on each node.\n- Spark: SPARK_EXECUTOR_CORES\n- Flink: taskmanager.numberOfTaskSlots""",

        )

    def _create_driver_memory_widget(self) -> widgets.IntSlider:
        """Create the driver memory slider widget."""
        fw_name = self.get_selected_framework_name() or "SPARK"
        default_resources = FRAMEWORK_REGISTRY[fw_name].default_resources
        value = default_resources.get("mem_driver", 1000)
        mem_worker = default_resources.get("mem_worker", 1000)

        return WidgetFactory.create_slider(
            value=value,
            min_val=value,
            max_val=self.slurm_info.get_memory_per_node() - mem_worker,
            step=128,
            description="Coordinator Memory (MB):",
        )

    def _create_worker_memory_widget(self) -> widgets.IntSlider:
        """Create the worker memory slider widget."""
        fw_name = self.get_selected_framework_name() or "SPARK"
        value = FRAMEWORK_REGISTRY[fw_name].default_resources.get("mem_driver", 1000)

        return WidgetFactory.create_slider(
            value=value,
            min_val=value,
            max_val=self.slurm_info.get_memory_per_node() - self.get_selected_driver_memory_val(),
            step=128,
            description="Memory Pool / Node (MB):",
        )

    def _create_executor_memory_widget(self) -> widgets.IntSlider:
        """Create the executor memory slider widget."""
        fw_name = self.get_selected_framework_name() or "SPARK"
        value = FRAMEWORK_REGISTRY[fw_name].default_resources.get("mem_driver", 1000)

        return WidgetFactory.create_slider(
            value=value,
            min_val=value,
            max_val=self.get_selected_worker_memory_val(),
            step=128,
            description="Memory / Compute Unit (MB):",
        )

    def _create_randomize_port_widget(self) -> widgets.Checkbox:
        """Create the randomize port checkbox widget."""
        return WidgetFactory.create_checkbox(
            value=False,
            description="Randomize Master Port",
        )

    def _create_start_cluster_button(self) -> widgets.Button:
        """Create the start cluster button widget."""
        button = WidgetFactory.create_styled_button(
            description="Start Cluster",
            layout_overrides={"width": "40%", "color": "white"},
        )
        # button.disabled = not self.is_config_set
        button.disabled = False
        button.on_click(self._on_start_cluster_clicked)
        return button

    def _create_stop_cluster_button(self) -> widgets.Button:
        """Create the stop cluster button widget."""
        button = WidgetFactory.create_styled_button(
            description="Stop Cluster",
            style_overrides={"button_color": "red", "color": "white"},
            layout_overrides={"width": "40%"},
        )
        button.disabled = not self.is_config_set
        button.on_click(self._on_stop_cluster_clicked)
        return button

    def _create_metric_dashboard(self) -> widgets.Box:
        """Create the metric dashboard widget."""
        fw_monitor = self._get_framework_monitor()
        return widgets.VBox(
            [
                widgets.HTML("<div>Process Metrics</div>"),
                self.process_monitor.get_ui(),
                widgets.HTML("<div>Framework Metrics</div>"),
                fw_monitor.get_ui(),
            ]
        )

    def _create_output_area(self) -> widgets.Output:
        output_widget = widgets.Output(
            layout=widgets.Layout(
                height="200px",
                max_height="200px",
                overflow="auto",
                border="2px solid #555555",
            )
        )
        # Add custom class for CSS targeting
        output_widget.add_class("log")
        output_widget.add_class("id-log")
        return output_widget


    # =========================================================================
    # Event Handlers
    # =========================================================================

    def _attach_widget_observers(self) -> None:
        """Attach observers to widgets for interactivity."""
        observable_widgets = [
            self.widgets["framework"],
            self.widgets["logo"],
            self.widgets["template"],
            self.widgets["destination"],
            self.widgets["master_host"],
            self.widgets["worker_hosts"],
            self.widgets["worker_hosts"].children[0],
            self.widgets["driver_cpu"],
            self.widgets["worker_cpu"],
            self.widgets["executor_cpu"],
            self.widgets["driver_memory"],
            self.widgets["worker_memory"],
            self.widgets["executor_memory"],
            self.widgets["randomize_port"],
        ]

        for widget in observable_widgets:
            widget.observe(self._on_parameters_changed, names="value")

        # Observers to refresh cluster info label
        self.widgets["master_host"].observe(self._update_cluster_info, names="value")
        for cb in self.widgets["worker_hosts"].children[0].children:
            cb.observe(self._update_cluster_info, names="value")

        # Set up dynamic range updates
        self._setup_dynamic_ranges()

    def _setup_dynamic_ranges(self) -> None:
        """Set up dynamic widget range interdependencies."""
        # CPU ranges
        self.widgets["worker_cpu"].max = (
            self.slurm_info.get_cpus_per_node() - self.get_selected_driver_cpu()
        )
        self.widgets["driver_cpu"].observe(self._update_worker_cpu_range, names="value")

        self.widgets["executor_cpu"].max = self.widgets["worker_cpu"].value
        self.widgets["worker_cpu"].observe(self._update_executor_cpu_range, names="value")

        # Memory ranges
        self.widgets["worker_memory"].max = (
            self.slurm_info.get_memory_per_node() - self.get_selected_driver_memory_val()
        )
        self.widgets["driver_memory"].observe(self._update_worker_memory_max, names="value")
        self.widgets["worker_memory"].observe(self._update_executor_memory_max, names="value")

    def _setup_visualization_triggers(self) -> None:
        """Set up widgets that trigger visualization updates."""
        trigger_widgets = [
            self.widgets["master_host"],
            self.widgets["worker_hosts"],
            self.widgets["driver_cpu"],
            self.widgets["worker_cpu"],
            self.widgets["executor_cpu"],
            self.widgets["driver_memory"],
            self.widgets["worker_memory"],
            self.widgets["executor_memory"],
        ]

        for widget in trigger_widgets:
            widget.observe(self.update_process_viz, names="value")

        # Initial visualization
        self.update_process_viz()

    def _on_parameters_changed(self, change: Dict[str, Any]) -> None:
        """Handle parameter changes."""
        self.is_config_set = False
        self._update_framework_home_labels(change)
        fw_logo_wdg = self.widgets["logo"]
        fw_logo_wdg.value = self._create_logo_widget().value
        if change.get("owner") is self.widgets.get("framework"):
            self._update_metric_dashboard_widget()

    def _update_worker_cpu_range(self, change: Dict[str, Any]) -> None:
        """Update worker CPU range based on driver CPU."""
        self.widgets["worker_cpu"].max = self.slurm_info.get_cpus_per_node() - change["new"]

    def _update_executor_cpu_range(self, change: Dict[str, Any]) -> None:
        """Update executor CPU range based on worker CPU."""
        self.widgets["executor_cpu"].max = change["new"]

    def _update_worker_memory_max(self, change: Dict[str, Any]) -> None:
        """Update worker memory max based on driver memory."""
        self.widgets["worker_memory"].max = (
            self.slurm_info.get_memory_per_node() - change["new"]
        )

    def _update_executor_memory_max(self, change: Dict[str, Any]) -> None:
        """Update executor memory max based on worker memory."""
        tmp = change["new"]
        self.widgets["executor_memory"].max = tmp

    def _on_start_cluster_clicked(self, _: widgets.Button) -> None:
        """Handle start cluster button click."""
        self._toggle_cluster_buttons(all_disabled=True)
        try:
            with self.widgets["output_area"]:
                self._set_environment()
                self._last_cluster_result = self.bdm.start_cluster()
            self._toggle_framework_gui(disabled=False)
            self.row3.enable()
            self._toggle_cluster_buttons(start_disabled=True, stop_disabled=False)
            self._toggle_framework_gui(disabled=False)
        except Exception as e:
            tb = traceback.format_exc()
            self._log(f"Failed to start cluster:{e}\n{tb}",msg_type="error")
            self._toggle_cluster_buttons(start_disabled=True, stop_disabled=False)
        self._update_cluster_info()

    def _on_stop_cluster_clicked(self, _: widgets.Button) -> None:
        """Handle stop cluster button click."""
        self._toggle_cluster_buttons(all_disabled=True)
        try:
            #self._last_cluster_result = self._log(wrap_function_stdout=True, func=self.bdm.stop_cluster)
            with self.widgets["output_area"]:
                self._last_cluster_result = self.bdm.stop_cluster()
                self._toggle_cluster_buttons(start_disabled=False, stop_disabled=True)
                self._toggle_framework_gui(disabled=True)
                self.row3.disable()
        except Exception as e:
            self._log(f"Failed to stop cluster:{e}",msg_type="error")
            self._toggle_cluster_buttons(start_disabled=True, stop_disabled=False)
        self._update_cluster_info()

    def _toggle_cluster_buttons(
        self,
        start_disabled: Optional[bool] = None,
        stop_disabled: Optional[bool] = None,
        all_disabled: Optional[bool] = None,
    ) -> None:
        """Toggle cluster button states."""
        if all_disabled is not None:
            self.widgets["start_cluster"].disabled = all_disabled
            self.widgets["stop_cluster"].disabled = all_disabled
            return

        if start_disabled is not None:
            self.widgets["start_cluster"].disabled = start_disabled
        if stop_disabled is not None:
            self.widgets["stop_cluster"].disabled = stop_disabled

    def _toggle_framework_gui(self,disabled: bool) -> None:
        # """Toggle cluster button states."""
        fw_gui_btns = self.widgets["framework_gui"].children[0]
        for wdg_i in fw_gui_btns.children:
            wdg_i = WidgetFactory.update_widget_state(wdg_i,disable=disabled)
        if disabled:
            self.widgets["framework_gui"].disable()
        else:
            self.widgets["framework_gui"].enable()

    def _update_framework_home_labels(self, change: Dict[str, Any]) -> None:
        """Update framework home widget labels when framework changes."""
        try:
            fw_name = self.get_selected_framework_name().upper()
            framework_home_widget = self.widgets.get("framework_home")

            if framework_home_widget and len(framework_home_widget.children) >= 2:
                checkbox = framework_home_widget.children[0]
                path_input = framework_home_widget.children[1]

                # Update checkbox description
                checkbox.update_label(f"Use custom {fw_name}_HOME")

        except Exception as e:
            logger.error(e)

    # =========================================================================
    # GUI Assembly
    # =========================================================================

    def _assemble_gui(self) -> VBox:
        """Assemble the complete GUI widget tree and return it."""
        config_container = self._create_config_container()
        viz_container = self._create_viz_container()

        self.row1 = widgets.HBox(
            [config_container, viz_container],
            layout=widgets.Layout(
                display="flex",
                flex_flow="row",
                width="100%",
                max_width="100%",
                overflow="hidden",
                # height="800px",
                justify_content="space-around",
                margin="0px",
                height="fit-content",
                max_height="750"
            ),
        )

        buttons_box = HBox([
                self.widgets["start_cluster"],
                self.widgets["stop_cluster"]
            ], layout=widgets.Layout(
                width="80%",
                justify_content="center",
                margin="0px auto"
            ))

        cluster_info_box = VBox(
            [buttons_box, self.widgets["cluster_info"]],
            layout=widgets.Layout(width="50%")
        )

        cluster_widget = HBox([
            cluster_info_box,
            widgets.HTML(value="<div style='border-left: 1px solid #808080; height: 90%; display: inline-block; margin: auto auto;'></div>"),
            self.widgets["framework_gui"],
            ]
        )
        self._toggle_framework_gui(disabled=True)

        self.row2 = VBox([self._create_header(title="Cluster Management"), cluster_widget],
            layout=widgets.Layout(
                display="flex",
                flex_flow="column",
                #margin="0px",
                justify_content="space-around",
            ),
        )
        self.row2.add_class("sub-container")



        # self.row3:VBox= VBox([
        #     self.widgets["framework_gui"],
        # ])
        # self.row3.add_class("sub-container")


        self.row3:VBox = VBox([
            self._create_header(title="Performance Metric"),
            self.widgets["metric_dashboard"]
        ])
        self.row3.add_class("sub-container")
        self.row3.disable()

        # Inject CSS for text wrapping in output area
        html_header_content = f"""
        <style>
        {get_file_content(os.path.join(os.path.dirname(__file__), "..", "style.css"))}
        </style>
        <script>
        var el = document.querySelector('.id-log');
        if (el) el.scrollTop = el.scrollHeight;
        </script>
        """

        self.style_widget = widgets.HTML(value=html_header_content)

        main_container : VBox = VBox(
            [self.style_widget, self.row1, self.row2,
            #  self.row3,
             self.row3, self.widgets["output_area"]],
            layout=widgets.Layout(
                display="flex",
                flex_flow="column",
                width="100%",
                max_width="100%"
            ),
        )
        main_container.add_class("main-container")
        main_container.add_class("id-main-container")
        return main_container

    def _create_config_container(self) -> VBox:
        """Create the configuration panel container."""
        config_widgets = [
            self.widgets["header_config"],
            self.widgets["logo"],
            self.widgets["framework"],
            self.widgets["framework_home"],
            self.widgets["template"],
            self.widgets["destination"],
            self.widgets["master_host"],
            self.widgets["worker_hosts"],
            self.widgets["driver_cpu"],
            self.widgets["worker_cpu"],
            self.widgets["executor_cpu"],
            self.widgets["driver_memory"],
            self.widgets["worker_memory"],
            self.widgets["executor_memory"],
            self.widgets["randomize_port"],
        ]
        self.widgets["config_wdg"] = config_widgets

        wdg:VBox = VBox(
            config_widgets,
            layout=widgets.Layout(
                width="50%",
                display="flex",
                flex_flow="column",
                align_items="stretch",
                align_content="stretch",
            ),
        )
        wdg.add_class("sub-container")
        return wdg

    def _create_viz_container(self) -> VBox:
        """Create the visualization panel container."""
        wdg = VBox(
            [self.widgets["header_viz"], self.wdg_viz_display],
            layout=widgets.Layout(
                width="50%",
                padding="40px",
                # display="flex",
                # flex_flow="column",
                # align_items="stretch"
            ),
        )
        wdg.add_class("sub-container")
        return wdg


    # =========================================================================
    # Value Getters
    # =========================================================================

    def get_selected_framework_name(self) -> Optional[str]:
        """Get the selected framework name."""
        return self.widgets["framework"].value

    def get_selected_workers(self) -> List[str]:
        """Get the selected worker hosts."""
        worker_widget = self.widgets["worker_hosts"]
        checkboxes = worker_widget.children[1].children
        return [cb.description for cb in checkboxes if cb.value]

    def get_selected_master_port(self) -> str:
        """Get the selected master port."""
        if self.widgets["randomize_port"].value:
            if self.get_selected_framework_name().lower() == "spark":
                return str(self._find_first_available_port(start_port=7077))
            elif self.get_selected_framework_name().lower() == "flink":
                return str(self._find_first_available_port(start_port=6123))
        return str(self.selected_framework.default_master_port)

    def get_selected_master_host(self) -> str:
        """Get the selected master host."""
        return self.widgets["master_host"].value

    def get_selected_driver_cpu(self) -> int:
        """Get the selected driver CPU."""
        return self.widgets["driver_cpu"].value

    def get_selected_worker_cpu(self) -> int:
        """Get the selected worker CPU."""
        return self.widgets["worker_cpu"].value

    def get_selected_worker_memory_val(self) -> int:
        """Get the selected worker memory value (integer)."""
        return int(self.widgets["worker_memory"].value)

    def get_selected_worker_memory(self) -> str:
        """Get the selected worker memory with 'm' suffix."""
        return f"{self.get_selected_worker_memory_val()}m"

    def get_selected_executor_cpu(self) -> int:
        """Get the selected executor CPU."""
        return self.widgets["executor_cpu"].value

    def get_selected_executor_memory_val(self) -> int:
        """Get the selected executor memory value (integer)."""
        return int(self.widgets["executor_memory"].value)

    def get_selected_executor_memory(self) -> str:
        """Get the selected executor memory with 'm' suffix."""
        return f"{self.get_selected_executor_memory_val()}m"

    def get_selected_driver_memory_val(self) -> int:
        """Get the selected driver memory value (integer)."""
        return int(self.widgets["driver_memory"].value)

    def get_selected_driver_memory(self) -> str:
        """Get the selected driver memory with 'm' suffix."""
        return f"{self.get_selected_driver_memory_val()}m"

    def get_selected_config_destination(self) -> str:
        """Get the selected config destination path."""
        dest = self.widgets["destination"].value
        fw_name = self.get_selected_framework_name()
        conf_dest = os.path.join(dest, fw_name.lower())
        return conf_dest

    def is_default_config_template(self) -> bool:
        template_widget = self.widgets["template"]
        return template_widget.children[0].is_checked()

    def get_selected_config_template(self) -> str:
        """Get the selected config template path."""
        template_widget = self.widgets["template"]
        use_default = template_widget.children[0].value

        if use_default:
            fw_name = self.get_selected_framework_name()
            return os.environ.get(f"{fw_name}_CONF_TEMPLATE", "")
        return template_widget.children[1].value

    def get_selected_local_dirs(self) -> str:
        """Get the selected local directories."""
        return f"/tmp/{self.user}/cluster-conf-{self.slurm_info.job_id}/spark/local"

    def get_selected_worker_dir(self) -> str:
        """Get the selected worker directory."""
        return f"/tmp/{self.user}/cluster-conf-{self.slurm_info.job_id}/spark/work"

    def get_selected_log_dir(self) -> str:
        """Get the selected log directory."""
        return f"{self.get_selected_config_destination()}/log"

    def get_selected_pid_dir(self) -> str:
        """Get the selected PID directory."""
        return f"{self.get_selected_config_destination()}/pid"

    def _is_custom_framework_home_enabled(self) -> bool:
        """Check if custom framework home is enabled."""
        framework_home_widget = self.widgets.get("framework_home")
        if framework_home_widget and framework_home_widget.children:
            return framework_home_widget.children[0].value
        return False

    def get_selected_framework_home(self) -> str:
        """Get the custom framework home path."""
        if self._is_custom_framework_home_enabled():
            framework_home_widget = self.widgets.get("framework_home")
            if framework_home_widget and len(framework_home_widget.children) > 1:
                return framework_home_widget.children[1].value
        elif f"{self.get_selected_framework_name().upper()}_HOME" in os.environ.keys():
            return os.environ[f"{self.get_selected_framework_name()}_HOME"]
        else:
            raise ValueError(
                "Framework is not set. Either provide a custom path or set the "
                f"'{self.get_selected_framework_name().upper()}_HOME' environment variable"
            )

    # =========================================================================
    # Visualization Helpers
    # =========================================================================

    def _get_viz_proportions(self) -> Dict[str, str]:
        """Calculate visualization proportions for resources."""
        # Get resource values
        drv_mem = float(self.get_selected_driver_memory_val())
        wrk_mem = float(self.get_selected_worker_memory_val())
        exe_mem = float(self.get_selected_executor_memory_val())

        drv_cpu = float(self.get_selected_driver_cpu())
        wrk_cpu = float(self.get_selected_worker_cpu())
        exe_cpu = float(self.get_selected_executor_cpu())

        # Get node capacities
        node_mem_capacity = self.slurm_info.get_memory_per_node()
        node_cpu_capacity = self.slurm_info.get_cpus_per_node()

        return {
            "master_node": self.get_selected_master_host(),
            "worker_node": self.get_selected_workers(),
            "total_mem_val": f"{int(node_mem_capacity)}",
            "drv_mem_val": f"{int(drv_mem)}",
            "wrk_mem_val": f"{int(wrk_mem)}",
            "exe_mem_val": f"{int(exe_mem)}",
            "total_cpu_val": f"{int(node_cpu_capacity)}",
            "drv_cpu_val": f"{int(drv_cpu)}",
            "wrk_cpu_val": f"{int(wrk_cpu)}",
            "exe_cpu_val": f"{int(exe_cpu)}",
        }

    # =========================================================================
    # Environment Setup
    # =========================================================================

    # def _set_load_button_processing(self) -> None:
    #     """Set load button to processing state."""
    #     self.widgets["load_button"].disabled = True
    #     self.widgets["load_button"].description = "Processing..."
    #     self.widgets["load_button"].button_style = "warning"

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def _find_first_available_port(
        self,
        start_port: int = 7077,
        end_port: int = 9000,
        host: Optional[str] = None,
    ) -> int:
        """Find the first available port in the given range."""
        host = host or socket.gethostname()
        for port in range(start_port, end_port + 1):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.bind((host, port))
                    return port
                except OSError:
                    continue
        raise RuntimeError(f"No available ports found in range {start_port}-{end_port}")
