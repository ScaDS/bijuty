# Big Data Environment Setup for JupyterHub

## Description

This project provides a Python module with interactive tools to configure and manage big data frameworks (Spark, Flink) inside [JupyterHub](https://compendium.hpc.tu-dresden.de/access/jupyterhub/?h=jupyter) on the [ZIH HPC System](https://tu-dresden.de/zih/hochleistungsrechnen/hpc).

It includes:
- An interactive GUI for framework selection, resource allocation, cluster configuration, and start/stop controls
- Automatic SLURM environment detection and node discovery
- Support for running multiple independent framework clusters via a tabbed interface
- Programmatic APIs for advanced scripting

---

## Prerequisites

- Python >= 3.11.5
- A JupyterHub / Jupyter Notebook environment
- `ipywidgets` enabled in Jupyter (the package depends on it)
- An active SLURM job allocation (the tool auto-detects SLURM resources)

---

## Installation

Install the package directly from the GitLab repository inside a Jupyter notebook cell:

```python
!pip install git+https://gitlab.hrz.tu-chemnitz.de/apku868a--tu-dresden.de/big-data-environment-setup-for-jupyterhub@main
```

Or install from a local clone:

```bash
git clone https://gitlab.hrz.tu-chemnitz.de/apku868a--tu-dresden.de/big-data-environment-setup-for-jupyterhub.git
cd big-data-environment-setup-for-jupyterhub
pip install -e .
```

### Enabling Jupyter Widgets

If `ipywidgets` is not already enabled, run once in a terminal:

```bash
jupyter nbextension enable --py widgetsnbextension
# For JupyterLab:
jupyter labextension install @jupyter-widgets/jupyterlab-manager
```

---

## Quick Start

To get started, simply import the package in a notebook cell. It automatically displays the interactive **Framework Manager** GUI:

```python
import big_data_utils
```

This launches a tabbed interface where you can:
- Select a framework (Spark / Flink)
- Configure CPU, memory, and port settings
- Choose master and worker hosts from your SLURM allocation
- Load the environment
- Start and stop clusters

Add new cluster tabs with **+** and remove them with **x**.

<!-- ### 2. Launch the GUI Manually

If you prefer to control when the GUI appears:

```python
from big_data_utils import MultiFrameworkManager

manager = MultiFrameworkManager()
manager.display()
```

Or use the single-cluster GUI directly:

```python
from big_data_utils.gui_utils import GUIUtils

gui = GUIUtils()
gui.launch_gui_config()
```
 -->
---

## Working with the GUI

### Configuration Panel (left side)

| Control | Purpose |
|---------|---------|
| **Framework** | Select Spark or Flink |
| **Custom FRAMEWORK_HOME** | Optionally override the framework installation path |
| **Template** | Use the default config template or specify a custom one |
| **Destination** | Directory where the generated configuration will be written |
| **Master Host** | The node to use as the cluster master |
| **Worker Hosts** | Nodes to use as workers (checkboxes auto-populated from SLURM) |
| **Driver / Worker / Executor CPU** | CPU allocation sliders |
| **Driver / Worker / Executor Memory** | Memory allocation sliders (MB) |
| **Randomize Master Port** | Avoid port conflicts when many users share nodes |
| **Load to Environment** | Generate configuration and update environment variables |

### Resource Allocation Overview (right side)

A live visualization shows how CPU and memory resources are distributed across master, worker, and executor roles based on the SLURM allocation.

### Cluster Controls

After loading the environment:
- **Start Cluster** - starts the selected framework cluster
- **Stop Cluster** - gracefully stops the cluster
- **Web UI Links** - buttons to open framework web UIs (Spark Master, Worker, Application UI; Flink JobManager UI)
- **Metric Dashboard** - live process metrics for running clusters

### SSH Port Forwarding

If running on the HPC cluster, the GUI displays an SSH command to forward web UI ports to your local machine so you can access cluster and application GUI

---

<!-- ## Programmatic API

For scripts and automated workflows, use the programmatic APIs instead of the GUI.

### BigDataManager (Recommended)

```python
from big_data_utils.big_data_manager import BigDataManager
from big_data_utils.gui_components import FRAMEWORK_REGISTRY

bdm = BigDataManager()

# Initialize with cluster properties
bdm.initialize_user_input({
    "fw_name": "SPARK",
    "fw_home": "/path/to/spark",
    "master": "n0001",
    "workers": ["n0001", "n0002"],
    "master_port": "7077",
    "conf_dir": "./cluster-conf-12345/spark",
    "log_dir": "./cluster-conf-12345/spark/log",
    "fw_mapping": FRAMEWORK_REGISTRY,
})

# Set up configuration from template
bdm.setup_config(conf_dest="./cluster-conf-12345", randomize_ports=False)

# Start the cluster
bdm.start_cluster()

# Check cluster health
print(bdm.is_cluster_up())

# Stop the cluster
bdm.stop_cluster()
```

### ClusterService (Legacy API)

```python
from big_data_utils.cluster_utils import ClusterService

cluster = ClusterService("spark")
cluster.start_cluster()
cluster.check_status()
cluster.webui()
cluster.stop_cluster()
```

### SLURM Utilities

```python
from big_data_utils.slurm_utils import SlurmManager

slurm = SlurmManager()
print(slurm.job_id)
print(slurm.get_nodes_list())
print(slurm.get_cpus_per_node())
print(slurm.get_memory_per_node())
```

---

## Environment Variables

After loading the environment, the following variables are set automatically:

| Variable | Description |
|----------|-------------|
| `SPARK_CONF_DIR` / `FLINK_CONF_DIR` | Path to the generated configuration directory |
| `SPARK_LOG_DIR` / `FLINK_LOG_DIR` | Path to framework logs |
| `SPARK_MASTER_HOST` | Master node hostname |
| `SPARK_MASTER_PORT` | Master port |
| `SPARK_WORKER_CORES` | Cores allocated per worker |
| `SPARK_WORKER_MEMORY` | Memory allocated per worker |
| `SPARK_EXECUTOR_CORES` | Cores allocated per executor |
| `SPARK_EXECUTOR_MEMORY` | Memory allocated per executor |
| `SPARK_DRIVER_MEMORY` | Memory allocated for driver |
| `PYSPARK_PYTHON` | Python interpreter path |

--- -->

## Supported Frameworks

| Framework | Version |
|-----------|------------|
| **Spark** |  |
| **Flink** |  |

---

<!-- ## Testing

The project includes a comprehensive test suite. See [`tests/README.md`](./tests/README.md) for detailed instructions.

Quick start:

```bash
# Local testing (mocked SLURM)
python tests/run_tests.py

# With coverage
python tests/run_tests.py --coverage

# HPC testing (requires active SLURM job)
salloc --nodes=2 --time=01:00:00
python tests/run_tests.py --hpc
```

---

## Troubleshooting

### GUI does not appear after import
Make sure `ipywidgets` is installed and enabled:
```bash
pip install ipywidgets
jupyter nbextension enable --py widgetsnbextension
```

### "Not in a SLURM job" warning
The tool is designed to run inside a JupyterHub session backed by a SLURM allocation. The GUI will show limited functionality without SLURM.

### Port conflicts
Enable **Randomize Master Port** in the GUI or set `randomize_ports=True` when calling `setup_config()`.

### Docker / SLURM-related errors during local testing
```bash
docker volume rm $(docker volume ls -q)
docker network prune -f
docker buildx prune -f
docker buildx rm default
docker buildx create --use
```

--- -->

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## Contact

For more information contact us at [ScaDS.AI](https://scads.ai/scads-ai-team/contact/).
