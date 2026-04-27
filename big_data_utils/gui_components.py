"""
GUI components for configuring and managing big data frameworks (Spark, Flink).

This module provides reusable GUI components, HTML generators, and widget factories
for building Jupyter notebook-based interfaces using ipywidgets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import os
import ipywidgets as widgets
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
    "description_width": "150px",
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
        proc_master="org.apache.spark.deploy.master.Master --host",
        proc_worker="org.apache.spark.deploy.worker.Worker --webui-port",
        proc_other=[
            "org.apache.spark.deploy.SparkSubmit",
            "org.apache.spark.executor.CoarseGrainedExecutorBackend",
            "org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend",
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
        <div style="display: block; justify-content: center; align-items: center; gap: 10px; width: 100%;">
          <h2 style="color: #2196F3; text-align: center;">{title}</h2><hr>
        </div>
        """

    @staticmethod
    def generate_process_style(height: str, background: str, text_color: str) -> str:
        """Generate CSS style for process block."""
        return f"""
            height: calc({height});
            min-height: fit-content;
            background: {background};
            color: {text_color};
            transition: height 0.4s ease;
            position: relative;
            display: flex;
            flex-direction: column;
            align-items: center;
            width: calc(100% - 10px);
        """

    @staticmethod
    def generate_sub_process_style(height: str, background: str, text_color: str) -> str:
        """Generate CSS style for subprocess block."""
        return f"""
            background: {background};
            color: {text_color};
            padding: 0px;
            border-radius: 0px;
            font-size: 11px;
            width: 100%;
            max-width: 100%;
            height: {height};
        """

    @classmethod
    def generate_cpu_process(cls, props: Dict[str, Any]) -> str:
        """Generate CPU process visualization HTML."""
        outer_title = props.get("outer_title", "Outer Title")
        outer_bg = props.get("outer_style_bkg_clr", COLOR_SCHEME["master_bg"])
        outer_text = props.get("outer_style_text_clr", COLOR_SCHEME["master_text"])
        outer_text_content = props.get("outer_text", "")
        outer_height = props.get("outer_height", "100%")

        inner_title = props.get("inner_title", "Inner Title")
        inner_bg = props.get("inner_style_bkg_clr", COLOR_SCHEME["master_dark"])
        inner_text = props.get("inner_style_text_clr", "#ffffff")
        inner_text_content = props.get("inner_text", "")
        inner_height = props.get("inner_height", "100%")

        outer_style = cls.generate_process_style(outer_height, outer_bg, outer_text)
        inner_style = cls.generate_sub_process_style(inner_height, inner_bg, inner_text)

        #outer_value = props.get("outer_title", "Outer Title").split(":")[1] if ":" in props.get("outer_title", "") else ""

        return f"""
        <div style="{outer_style}">
            <div style="position: absolute; left: 0; top: 0; bottom: 0; width: 20px;
                        display: flex; align-items: center; justify-content: center;
                        writing-mode: sideways-lr; text-orientation: mixed;
                        background: {outer_bg}; color: black; font-weight: bold;">
                {outer_title}
            </div>
            <div>{outer_text_content}</div>
            <div style="{inner_style}; display: flex; align-items: center; justify-content: center;">
                <div style="position: absolute; left: 20px; width: 20px;
                            display: flex; align-items: center; justify-content: center;
                            writing-mode: sideways-lr; text-orientation: mixed;
                            background: {inner_bg}; color: {inner_text}; font-weight: bold;">
                    {inner_title}
                </div>
                <div style="padding: 0 45px;">{inner_text_content}</div>
            </div>
        </div>
        """.strip()

    @staticmethod
    def generate_slurm_info(slurm_info: SlurmManager) -> str:
        """Generate Slurm job information HTML."""
        nodes = ",".join(slurm_info.get_nodes_list())
        cpu_cores = slurm_info.get_total_cpus() 
        cpu_per_node = slurm_info.get_cpus_per_node()
        memory = slurm_info.get_total_memory()
        memory_per_node = slurm_info.get_memory_per_node()

        return f"""
        <div style="display: flex; text-align: center; flex-direction: column;">
            <div style="text-align: center; font-weight: bold; margin-bottom: 10px; width: 100%">
                Slurm Job
            </div>
            <div style="display: grid; grid-template-columns: 33% 33% 33%; align-items: center; justify-content: center">
                <div style="color: #666; font-size: 12px; padding: 0px">
                    NODES
                    <div style="font-family: monospace; font-weight: bold; color: #2980b9; align-items: center; justify-content: center;">
                        {nodes}
                    </div>
                </div>
                <div style="color: #666; font-size: 12px; align-items: center; justify-content: center;">
                    CPUs
                    <div style="font-weight: bold;">{cpu_cores} <br>{cpu_per_node}/Node </div>
                </div>
                <div style="color: #666; font-size: 12px; align-items: center; justify-content: center;">
                    MEMORY
                    <div style="font-weight: bold;">{memory} MB <br> {memory_per_node} MB/Node</div>
                </div>
            </div>
        </div>
        """

    @classmethod
    def generate_viz_template(
        cls,
        props: Dict[str, str],
        slurm_info: SlurmManager,
        driver_node_name: str = "Master",
        worker_node_name: str = "Worker",
        driver_process_name: str = "Driver",
        worker_process_name: str = "Executor",
    ) -> str:
        """Generate complete visualization HTML template."""
        cpu_section = cls._generate_resource_section(
            props, "cpu", driver_node_name, worker_node_name, driver_process_name, worker_process_name
        )
        mem_section = cls._generate_resource_section(
            props, "mem", driver_node_name, worker_node_name, driver_process_name, worker_process_name
        )
        
        node_id = slurm_info.get_nodes_list()[0]

        return f"""
        <div style="
            border: 3px solid #444;
            border-radius: 0px;
            width: 95%;
            height: 700px;
            overflow-y: scroll;
            background: #f0f0f0;
            display: flex;
            flex-direction: column;
            padding: 10px;
            font-family: sans-serif;
        ">
            {cls.generate_slurm_info(slurm_info)}

            <div style="
                display: flex;
                flex-direction: row;
                justify-content: space-between;
                width: 100%;
                gap: 0px;
                margin-top: 0px;
                height: fit-content;
                min-height: 300px;
                font-size: 10px;
                border: 2px solid #444;
                border-radius: 0px;
            ">
                
                <div style="position: relative; left: 0; top: 0; bottom: 0; width: 20px;
                        display: flex; align-items: center; justify-content: center;
                        writing-mode: sideways-lr; text-orientation: mixed;
                        background: gray; color: black; font-weight: bold;">
                    {node_id}
                </div>

                <div style="width: 50%; display: flex; flex-direction: column; gap: 0px; height: 100%; align-items: center;">
                    {cpu_section}
                </div>

                <div style="width: 50%; display: flex; flex-direction: column; gap: 0px; height: 100%; align-items: center;">
                    {mem_section}
                </div>
            </div>
        </div>
        """

    @classmethod
    def _generate_resource_section(
        cls,
        props: Dict[str, str],
        resource_type: str,
        driver_node_name: str,
        worker_node_name: str,
        driver_process_name: str,
        worker_process_name: str,
    ) -> str:
        """Generate CPU or memory section for visualization."""
        suffix = resource_type  # 'cpu' or 'mem'

        driver_process = cls.generate_cpu_process({
            "outer_height": props[f"drv_{suffix}_height"],
            "outer_style_bkg_clr": COLOR_SCHEME["master_bg"],
            "outer_style_text_clr": COLOR_SCHEME["master_text"],
            "outer_title": f"{driver_node_name}",
            "outer_text": f"{resource_type.upper()}: {props[f'drv_{suffix}_val']}",
            "inner_height": "100%",
            "inner_style_bkg_clr": COLOR_SCHEME["master_dark"],
            "inner_style_text_clr": "#ffffff",
            "inner_title": driver_process_name,
            "inner_text": f"{resource_type.upper()}: {props[f'drv_{suffix}_val']}",
        })

        worker_process = cls.generate_cpu_process({
            "outer_height": props[f"wrk_{suffix}_height"],
            "outer_style_bkg_clr": COLOR_SCHEME["worker_bg"],
            "outer_style_text_clr": COLOR_SCHEME["worker_text"],
            "outer_title": f"{worker_node_name}",
            "outer_text": f"{resource_type.upper()}: {props[f'wrk_{suffix}_val']}",
            "inner_height": props[f"exe_{suffix}_height"],
            "inner_style_bkg_clr": COLOR_SCHEME["worker_dark"],
            "inner_style_text_clr": "#ffffff",
            "inner_title": worker_process_name,
            "inner_text": f"{resource_type.upper()}: {props[f'exe_{suffix}_val']}",
        })

        return f"{driver_process}{worker_process}"


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
    def create_slider(
        value: int,
        min_val: int,
        max_val: int,
        description: str,
        step: int = 1,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> widgets.IntSlider:
        """Create a standardized IntSlider widget."""
        slider_style = label_style or DEFAULT_LABEL_STYLE
        slider_style["handle_color"] = "blue"
        return widgets.IntSlider(
            value=value,
            min=min_val,
            max=max_val,
            step=step,
            description=description,
            style=slider_style,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
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

    @staticmethod
    def create_checkbox(
        value: bool,
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> widgets.Checkbox:
        """Create a standardized Checkbox widget."""
        return widgets.Checkbox(
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
