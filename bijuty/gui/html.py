from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..slurm_utils import SlurmManager


class HTMLGenerator:
    """Generates HTML content for visualization components."""

    @staticmethod
    def generate_header(title: str) -> str:
        """Generate HTML header with title."""
        return f"""
        <div class="gui-header">
          <h2 class="gui-header-title">{title}</h2>
        </div>
        """

    @staticmethod
    def generate_card_visualization(
        title: str,
        card_type: str,
        resources: str,
        children: list = None,
        color: str = "#016652",
        is_root: bool = True,
    ) -> str:
        """Generates a hierarchical, recursive HTML visualization for Slurm Jobs,
        Nodes, and Processes."""

        if children is None:
            children = []

        children_html = ""
        if children:
            child_cards_markup = []
            for child in children:
                child_color = child.get("color", color)
                child_html = HTMLGenerator.generate_card_visualization(
                    title=child.get("title", "Child Node"),
                    card_type=child.get("card_type", "Process"),
                    resources=child.get("resources", ""),
                    children=child.get("children", []),
                    color=child_color,
                    is_root=False,
                )
                child_cards_markup.append(child_html)

            children_html = f"""
            <div class="slurm-card_children">
                {"".join(child_cards_markup)}
            </div>
            """

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
        master_node = props.get("master_node", slurm_node_list[0])
        coordinator_cores = props.get("drv_cpu_val")
        coordinator_mem = props.get("drv_mem_val")

        worker_node = props.get("worker_node", slurm_node_list)
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
                "children": [],
            }
            if node_i == master_node:
                node_i_info["children"].append({
                    "title": "Coordinator",
                    "card_type": "Process",
                    "is_root": False,
                    "resources": f"Cores: {coordinator_cores} | Memory: {coordinator_mem} MB",
                    "color": col_master_info,
                    "children": [],
                })

            for worker_node_i in worker_node:
                if worker_node_i == node_i:
                    node_i_info["children"].append({
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
                    })
            slurm_children.append(node_i_info)

        slurm_viz = cls.generate_card_visualization(
            title="Slurm Job",
            card_type="Job Allocation",
            is_root=True,
            resources=f"Total Cores: {slurm_cpu_total} | Total Memory: {slurm_mem_total} MB",
            children=slurm_children,
            color=col_slurm_info,
        )
        return slurm_viz

    @staticmethod
    def generate_ssh_instructions(ssh_cmd: str) -> str:
        """Generate HTML for SSH port-forwarding instructions."""
        return f"""
        <div style="padding:5px; background:#fffbea; border:1px solid #f0c36d; border-radius:4px; color:#5f4b32; font-size:12px;width:80%;justify-content:center;margin: auto auto">
          <b>Remote environment detected: </b> If above links do not open in your local browser, set up SSH port forwarding.
          <pre style="background:#f7f7f7; padding:5px; margin:0px 0; font-family:monospace;font-size:12px;">{ssh_cmd}</pre>
        </div>
        """

    @staticmethod
    def generate_framework_cluster_info(
        framework: str,
        status_color: str,
        status_text: str,
        master: str,
        master_port: str,
        workers_str: str,
        is_config_set: bool,
    ) -> str:
        """Generate HTML cluster-status and framework-configuration snippet."""
        html_content = (
            f"<div style='font-size:12px; color:#555; margin-top:4px; display:flex; flex-direction:column; width:100%; align-items:center;'>"
            f"  <div style='display:flex; justify-content:center; align-items:center; margin-bottom:2px;'><b>Cluster Status:&nbsp;</b><span style='width:8px; height:8px; border-radius:50%; background:{status_color}; margin-right:4px;'></span> {status_text}</div>"
            f"  <div style='display:flex; justify-content:center; align-items:center; margin-bottom:2px;'><b>Master:&nbsp;</b> {master}&nbsp;|&nbsp;<b>Port:&nbsp;</b>{master_port}</div>"
            f"  <div style='display:flex; justify-content:center; align-items:center; margin-bottom:4px;'><b>Workers:&nbsp;</b> {workers_str}</div>"
        )

        if is_config_set:
            if framework == "spark":
                html_content += (
                    f"  <div style='font-size:11px; color:#777; margin-top:4px; text-align:center;'>"
                    f"    Use the master node name while initializing Spark context.<br>"
                    f"    <b>eg. spark://{master}:{master_port}</b>"
                    f"  </div>"
                )
            elif framework == "flink":
                py_code = (
                    "from pyflink.common.configuration import Configuration\n\n"
                    "config = Configuration()\n"
                    'config.set_string("execution.target", "remote")\n'
                    f'config.set_string("jobmanager.rpc.address", "{master}")\n'
                    f'config.set_string("jobmanager.rpc.port", "{master_port}")\n'
                    f'config.set_string("rest.address", "{master}")\n'
                    'config.set_string("rest.port", "8081")'
                )
                html_content += (
                    f"  <div style='font-size:11px; color:#777; margin-top:6px; display:flex; flex-direction:column; width:90%; align-items:flex-start;'>"
                    f"    <span style='margin-bottom:4px; align-self:center; text-align:center;'>Set this configuration in your notebook before running the job:</span>"
                    f"    <pre style='font-size:11px;background:#f4f4f4; padding:8px; border-radius:4px; border:1px solid #ddd; font-family:monospace; width:100%; box-sizing:border-box; margin:0; text-align:left;'>{py_code}</pre>"
                    f"  </div>"
                )

        html_content += "</div>"
        return html_content
