"""
HTML content generators for visualization components.

This module provides helpers that produce HTML strings used by the GUI to
render headers, cards, and the resource-allocation tree.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..slurm_utils import SlurmManager


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
        is_root: bool = True,
    ) -> str:
        """
        Generates a hierarchical, recursive HTML visualization for Slurm Jobs,
        Nodes, and Processes.

        :param title: The title of the card (e.g. 'Slurm Job', 'Node 01')
        :param card_type: The badge text (e.g. 'Physical Node', 'Process')
        :param resources: Resource description string (e.g. 'Cores: 8 | Memory: 16384 MB')
        :param children: A list of dicts, where each dict represents a child card structure.
        :param color: Base color (CSS-compatible value, e.g. hex '#016652' or name 'teal').
        :param is_root: Private flag to manage recursive rendering and base CSS styling.
        """
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
