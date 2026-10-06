"""Unit tests for :mod:`bijuty.gui.html`."""

from __future__ import annotations

from bijuty.gui.html import HTMLGenerator


class TestGenerateHeader:
    def test_header_contains_title_and_class(self):
        html = HTMLGenerator.generate_header("Cluster Configurator")

        assert "Cluster Configurator" in html
        assert 'class="gui-header"' in html


class TestGenerateCardVisualization:
    def test_leaf_card_contains_title_type_and_resources(self):
        html = HTMLGenerator.generate_card_visualization(
            title="Node 1",
            card_type="Physical Node",
            resources="Cores: 4",
            color="#123456",
        )

        assert "Node 1" in html
        assert "Physical Node" in html
        assert "Cores: 4" in html
        assert "--slurm-primary: #123456" in html

    def test_nested_children_are_rendered_recursively(self):
        children = [
            {
                "title": "Child A",
                "card_type": "Process",
                "resources": "Cores: 1",
                "children": [
                    {"title": "Grandchild", "card_type": "Process",
                     "resources": "Cores: 0"},
                ],
            }
        ]

        html = HTMLGenerator.generate_card_visualization(
            title="Root", card_type="Job", resources="Total: 4",
            children=children,
        )

        assert "Root" in html
        assert "Child A" in html
        assert "Grandchild" in html
        assert 'class="slurm-card_children"' in html

    def test_children_default_to_empty(self):
        html = HTMLGenerator.generate_card_visualization(
            title="Root", card_type="Job", resources="Total: 4")

        assert "slurm-card_children" not in html


class TestGenerateVizTemplate:
    def test_template_includes_nodes_and_resource_pools(self, fake_slurm):
        props = {
            "master_node": "node1",
            "worker_node": ["node1", "node2"],
            "drv_cpu_val": "1",
            "drv_mem_val": "1000",
            "wrk_cpu_val": "4",
            "wrk_mem_val": "4000",
            "exe_cpu_val": "2",
            "exe_mem_val": "2000",
        }

        html = HTMLGenerator.generate_viz_template(props, fake_slurm)

        assert "Slurm Job" in html
        assert "Node ID: node1" in html
        assert "Node ID: node2" in html
        # The configured master node hosts exactly one coordinator card.
        assert html.count(">Coordinator</h2>") == 1
        # Every selected worker node contributes a resource pool with one
        # compute unit.
        assert html.count(">Resource Pool</h2>") == 2
        assert html.count(">Compute Unit</h2>") == 2
        assert "Cores: 1 | Memory: 1000 MB" in html

    def test_master_defaults_to_first_node(self, fake_slurm):
        props = {
            "worker_node": ["node1"],
            "drv_cpu_val": "1",
            "drv_mem_val": "1",
            "wrk_cpu_val": "1",
            "wrk_mem_val": "1",
            "exe_cpu_val": "1",
            "exe_mem_val": "1",
        }

        html = HTMLGenerator.generate_viz_template(props, fake_slurm)

        assert ">Coordinator</h2>" in html


class TestGenerateSshInstructions:
    def test_instructions_embed_command(self):
        html = HTMLGenerator.generate_ssh_instructions("ssh -L 8080:host:8080 jump")

        assert "ssh -L 8080:host:8080 jump" in html
        assert "SSH port forwarding" in html


class TestGenerateFrameworkClusterInfo:
    def test_spark_hint_when_config_is_set(self):
        html = HTMLGenerator.generate_framework_cluster_info(
            framework="spark",
            status_color="#28a745",
            status_text="Running",
            master="node1",
            master_port="7077",
            workers_str="node1, node2",
            is_config_set=True,
        )

        assert "spark://node1:7077" in html
        assert "Running" in html

    def test_flink_hint_contains_pyflink_config(self):
        html = HTMLGenerator.generate_framework_cluster_info(
            framework="flink",
            status_color="#dc3545",
            status_text="Stopped",
            master="node1",
            master_port="6123",
            workers_str="-",
            is_config_set=True,
        )

        assert "execution.target" in html
        assert '"remote"' in html
        assert "jobmanager.rpc.address" in html

    def test_no_hint_when_config_not_set(self):
        html = HTMLGenerator.generate_framework_cluster_info(
            framework="spark",
            status_color="#dc3545",
            status_text="Stopped",
            master="-",
            master_port="-",
            workers_str="-",
            is_config_set=False,
        )

        assert "spark://" not in html
        assert "Cluster Status" in html
