"""
GUI components for configuring and managing big data frameworks (Spark, Flink).

This module provides reusable GUI components, HTML generators, and widget factories
for building Jupyter notebook-based interfaces using ipywidgets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import os
import ipywidgets as widgets
from traitlets import Bool
from IPython.display import display, Javascript
import re
# from bs4 import BeautifulSoup as bs

import requests

from .slurm_utils import SlurmManager


# =============================================================================
# Constants
# =============================================================================

# Color schemes for visualization
COLOR_SCHEME = {
    "master_bg": "#9ac3f4",
    "master_text": "#1565c0",
    "master_dark": "#1565c0",
    "worker_bg": "#8ff898",
    "worker_text": "#307032",
    "worker_dark": "#4caf50",
}

# Default styling
DEFAULT_LABEL_STYLE = {
    "font_weight": "bold",
    "color": "#333333",
    "font_size": "14px",
    "description_width": "200px",
}

DEFAULT_WIDGET_LAYOUT = widgets.Layout(
    width="100%",
    margin="5px 0px",
    display="flex",
    flex_flow="row",
)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True)
class FrameworkConfig:
    """Configuration for a big data framework."""

    name: str
    start_cmd: str
    stop_cmd: str
    proc_master: str
    proc_worker: str
    logo_url: str
    worker_file: str
    default_master_port: int
    default_resources: Optional[Dict[str, int]] = None
    proc_other: Optional[List[str]] = None
    web_ui_links: Optional[List[Tuple[str, str]]] = None
    """List of (port, title) tuples for framework web UIs."""

    @property
    def default_template(self) -> str:
        return os.path.join(os.path.dirname(__file__),"framework_template",self.name_lower)

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
        start_cmd="sbin/start-all.sh", # relative to framework home dir
        stop_cmd="sbin/stop-all.sh",   # relative to framework home dir
        proc_master={"title":"Master","pattern":"org.apache.spark.deploy.master.Master --host"},
        proc_worker={"title":"Worker","pattern":"org.apache.spark.deploy.worker.Worker --webui-port"},
        proc_other=[
            {"title":"SparkSubmit","pattern":"org.apache.spark.deploy.SparkSubmit"},
            {"title":"Executor","pattern":"org.apache.spark.executor.CoarseGrainedExecutorBackend"},
            {"title":"Scheduler","pattern":"org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend"},
        ],
        logo_url="https://spark.apache.org/images/spark-logo-back.png",
        worker_file="workers",
        default_master_port=7077,
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
        start_cmd="bin/start-cluster.sh",
        stop_cmd="bin/stop-cluster.sh",
        proc_master="org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint",
        proc_worker="org.apache.flink.runtime.taskexecutor.TaskManagerRunner",
        logo_url="https://flink.apache.org/img/logo/png/200/flink_squirrel_200_color.png",
        worker_file="workers",
        default_master_port=8081,
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


# =============================================================================
# HTML Generators
# =============================================================================

class HTMLGenerator:
    """Generates HTML content for visualization components."""

    @staticmethod
    def generate_header(title: str) -> str:
        """Generate HTML header with title."""
        return f"""
        <div style="display: flex; justify-content: center; align-items: center; margin-top: 0px;">
          <h2 style="color: #0f2d56; text-align: center;">{title}</h2>
        </div>
        """
    @staticmethod
    def generate_card_visualization(
        title: str,
        card_type: str,
        resources: str,
        children: list = None,
        color: str = "#016652",
        is_root: bool = True
    ) -> str:
        """
        Generates a hierarchical, recursive HTML visualization for Slurm Jobs, Nodes, and Processes.
        
        :param title: The title of the card (e.g. 'Slurm Job', 'Node 01')
        :param card_type: The badge text (e.g. 'Physical Node', 'Process')
        :param resources: Resource description string (e.g. 'Cores: 8 | Memory: 16384 MB')
        :param children: A list of dicts, where each dict represents a child card structure.
        :param color: Base color (CSS-compatible value, e.g. hex '#016652' or name 'teal').
        :param is_root: Private flag to manage recursive rendering and base CSS styling.
        """
        if children is None:
            children = []

        # Recursively generate HTML for all child nodes
        children_html = ""
        if children:
            child_cards_markup = []
            for child in children:
                # Fall back to parent's color if child doesn't specify its own
                child_color = child.get("color", color)

                child_html = HTMLGenerator.generate_card_visualization(
                    title=child.get("title", "Child Node"),
                    card_type=child.get("card_type", "Process"),
                    resources=child.get("resources", ""),
                    children=child.get("children", []),
                    color=child_color,
                    is_root=False
                )
                child_cards_markup.append(child_html)

            children_html = f"""
            <div class="slurm-card_children">
                {"".join(child_cards_markup)}
            </div>
            """

        # Build individual card block with an inline style to pass the custom primary color
        card_html = f"""
        <div class="slurm-card" style="--slurm-primary: {color};" aria-label="{title} Info">
        <header class="slurm-card_header">
            <h2 class="slurm-card_title">{title}</h2>
            <span class="slurm-card_badge">{card_type}</span>
        </header>

        <p class="slurm-card_resources">
            {resources}
        </p>
        {children_html}
        </div>
        """

        # If this is the top-level card, package it with the shared responsive CSS stylesheet
        if is_root:
            return card_html

        return card_html

    @classmethod
    def generate_viz_template(
        cls,
        props: Dict[str, str],
        slurm_info: SlurmManager,
    ) -> str:
        slurm_cpu_per_node = slurm_info.get_cpus_per_node()
        slurm_cpu_total = slurm_info.get_total_cpus()
        slurm_mem_per_node = slurm_info.get_memory_per_node()
        slurm_mem_total = slurm_info.get_total_memory()
        slurm_node_list = slurm_info.get_nodes_list()
        slurm_children = []
        master_node = props.get("master_node",slurm_node_list[0])
        coordinator_cores = props.get("drv_cpu_val")
        coordinator_mem = props.get("drv_mem_val")

        worker_node = props.get("worker_node",slurm_node_list)
        core_pool = props.get("wrk_cpu_val")
        mem_pool = props.get("wrk_mem_val")
        compute_unit_cores = props.get("exe_cpu_val")
        compute_unit_mem = props.get("exe_mem_val")
        col_slurm_info = "#0292b6"
        col_node_info = "#ad8619"
        col_master_info = "#d11141"
        col_pool_info = "#f37735"
        col_cu_info = "#00b159"


        for node_i in slurm_node_list:
            node_i_info = {
                "title": f"Node ID: {node_i}",
                "card_type": "Physical Node",
                "is_root": False,
                "resources": f"Cores: {slurm_cpu_per_node} | Memory: {slurm_mem_per_node} MB",
                "color": col_node_info,
                "children": []
            }
            if node_i == master_node:
                node_i_info["children"].append({
                    "title": "Coordinator",
                    "card_type": "Process",
                    "is_root": False,
                    "resources": f"Cores: {coordinator_cores} | Memory: {coordinator_mem} MB",
                    "color": col_master_info,
                    "children": []
                })

            for worker_node_i in worker_node:
                if worker_node_i == node_i:
                    node_i_info["children"].append(
                        {
                            "title": "Resource Pool",
                            "card_type": "",
                            "is_root": False,
                            "resources": f"Cores: {core_pool} | Memory: {mem_pool} MB",
                            "color": col_pool_info,
                            "children": [
                                {
                                    "title": "Compute Unit",
                                    "card_type": "Process",
                                    "is_root": False,
                                    "resources": f"Cores: {compute_unit_cores} | Memory: {compute_unit_mem} MB",
                                    "color": col_cu_info,
                                    "children": [],
                                }
                            ],
                        }
                    )
            slurm_children.append(node_i_info)

        slurm_viz = cls.generate_card_visualization(
            title="Slurm Job",
            card_type="Job Allocation",
            is_root=True,
            resources=f"Total Cores: {slurm_cpu_total} | Total Memory: {slurm_mem_total} MB",
            children=slurm_children,
            color=col_slurm_info
        )
        return slurm_viz

# =============================================================================
# Widget Factory
# =============================================================================

class WidgetFactory:
    """Factory for creating standardized widgets."""

    @staticmethod
    def create_styled_button(
        description: str,
        style_overrides: Optional[Dict[str, Any]] = None,
        layout_overrides: Optional[Dict[str, Any]] = None,
        **button_kwargs,
    ) -> widgets.Button:
        """Create a styled button widget."""
        base_style = {
            "button_color": "#4caf50",
            "font_weight": "bold",
            "font_size": "14px",
        }
        base_layout = {
            "width": "120px",
            "height": "40px",
            "margin": "5px",
            "align_self": "center",
        }

        final_style = {**base_style, **(style_overrides or {})}
        final_layout = {**base_layout, **(layout_overrides or {})}

        return widgets.Button(
            description=description,
            style=widgets.ButtonStyle(**final_style),
            layout=widgets.Layout(**final_layout),
            **button_kwargs,
        )
    
    @staticmethod
    def create_styled_button_redirect(
    url: str,
    description: str,
    style_overrides: Optional[Dict[str, Any]] = None,
    layout_overrides: Optional[Dict[str, Any]] = None,
    **button_kwargs,
    ) -> widgets.Button:
        link_html = f"""
            <a href="{url}" target="_blank" style="text-decoration:none;">
                <button class="p-Widget jupyter-widgets jupyter-button widget-button mod-primary" 
                        style="width:160px; height:32px; cursor:pointer;" title="Open {url}">
                    {description}
                </button>
            </a>
            """
        button = widgets.HTML(value=link_html)
        return button
    
    @staticmethod
    def update_widget_state(widget, disable=False):
        # Get current HTML
        current_html = widget.value
        
        # Remove any existing 'disabled' attribute to prevent duplicates
        # This looks for ' disabled' followed by a space or the end of the tag
        clean_html = re.sub(r'\s+disabled(?=[\s>])', '', current_html)
        
        if disable:
            # Insert 'disabled' right before the first '>' of the button tag
            new_html = re.sub(r'(<button[^>]*)(>)', r'\1 disabled\2', clean_html)
        else:
            new_html = clean_html
            
        # Put it back into the widget
        widget.value = new_html
        return widget

    @staticmethod
    def create_slider(
        value: int,
        min_val: int,
        max_val: int,
        description: str,
        tooltip: str = None,
        step: int = 1,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> widgets.IntSlider:
        """Create a standardized IntSlider widget."""
        slider_style = label_style or DEFAULT_LABEL_STYLE
        slider_style["handle_color"] = "blue"
        if not tooltip:
            tooltip = description
        return widgets.IntSlider(
            value=value,
            min=min_val,
            max=max_val,
            step=step,
            description=description,
            style=slider_style,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
            tooltip = tooltip
        )

    @staticmethod
    def create_dropdown(
        options: List[str],
        value: Optional[str],
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> widgets.Dropdown:
        """Create a standardized Dropdown widget."""
        return widgets.Dropdown(
            options=options,
            value=value,
            description=description,
            style=label_style or DEFAULT_LABEL_STYLE,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
        )

    @staticmethod
    def create_text(
        value: str,
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
        disabled: bool = False,
    ) -> widgets.Text:
        """Create a standardized Text widget."""
        return widgets.Text(
            value=value,
            description=description,
            style=label_style or DEFAULT_LABEL_STYLE,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
            disabled=disabled,
        )

    # @staticmethod
    # def create_checkbox(
    #     value: bool,
    #     description: str,
    #     label_style: Optional[Dict[str, str]] = None,
    #     layout: Optional[widgets.Layout] = None,
    # ) -> widgets.Checkbox:
    #     """Create a standardized Checkbox widget."""
    #     cb:widgets.Checkbox = widgets.Checkbox(
    #         value=value,
    #         description=description,
    #         indent=False,
    #         style=label_style or DEFAULT_LABEL_STYLE,
    #         layout=layout or DEFAULT_WIDGET_LAYOUT,
    #     )
    #     cb.add_class("checkbox")
    #     return cb
    
    @staticmethod
    def create_checkbox(
        value: bool,
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> CustomCheckbox:
        """Create a standardized Checkbox widget."""
        return CustomCheckbox(
            value=value,
            description=description,
            indent=False,
            style=label_style or DEFAULT_LABEL_STYLE,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
        )
        


def fetch_image(url: str) -> bytes:
    """Fetch image content from URL."""
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.content
    except Exception:
        return b""


def create_placeholder_logo() -> widgets.HTML:
    """Create a placeholder logo widget."""
    return widgets.HTML(
        value="<div style='width:100px;height:100px;background-color:#eee;"
              "display:flex;align-items:center;justify-content:center;color:#999;'>logo</div>"
    )

# Adding disable/enable functionality to HBox and VBox
class ContainerMixin:
    """Mixin that adds enable/disable functionality to Box widgets."""

    def disable(self):
        """Disable the container by adding the CSS class."""
        self.add_class("disable")

    def enable(self):
        """Enable the container by removing the CSS class."""
        self.remove_class("disable")

    def is_disabled(self) -> bool:
        """Check whether the container is currently disabled."""
        return "disable" in (self._dom_classes or [])

    def toggle(self):
        """Toggle between enabled and disabled states."""
        if self.is_disabled():
            self.enable()
        else:
            self.disable()


class VBox(ContainerMixin, widgets.VBox):
    """VBox extended with enable/disable support."""
    pass


class HBox(ContainerMixin, widgets.HBox):
    """HBox extended with enable/disable support."""
    pass

class CustomCheckbox(widgets.HBox):
    # Define 'value' as a traitlet so it can be observed/linked
    value = Bool(False).tag(sync=True)

    def __init__(self, description="Label", value=False, **kwargs):
        # 1. Create the internal checkbox (without a native description)
        self._checkbox: widgets.Checkbox = widgets.Checkbox(value=value, indent=False)
        self._checkbox.add_class("custom-box-design")
        
        # 2. Create the label widget
        self._label:widgets.Label = widgets.Label(value=f"{description}: ")
        self._label.add_class("custom-box-label")

        
        # 3. Create the CSS widget to override the design
        self._css = widgets.HTML("""
            <style>
                .custom-box-design input[type='checkbox'] {
                    width: 20px;
                    height: 20px;
                    cursor: pointer;
                    accent-color: #007bff;
                    margin-left: 5px;
                }
                .custom-box-design { width: auto !important; }
                .custom-box-label {
                    width: 200px; 
                    justify-content: right;
                }
            </style>
        """)
        
        # 4. Set the children in the reversed order: [Box, Label]
        super().__init__(children=[self._label,self._checkbox, self._css], **kwargs)
        
        # 5. Link the class 'value' to the internal checkbox 'value'
        widgets.link((self._checkbox, 'value'), (self, 'value'))
    
    def update_label(self,label: str):
        self._label.value = f"{label}: "

    @property
    def description(self):
        return self._label.value.rstrip(": ")
    
    # @property
    # def value(self):
    #     return self._checkbox.value
    
    @description.setter
    def description(self, value: str):
        self._label.value = f"{value}: "

    def is_checked(self):
        return self._checkbox.value
        

# class Checkbox(CustomCheckbox,widgets.Checkbox):
#     pass