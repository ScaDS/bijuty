# Big Data Environment Setup for Jupyterhub

## Description
This project provides a simple Python module containing tools to setup environment for big data frameworks to be used in [Jupyterhub services](https://compendium.hpc.tu-dresden.de/access/jupyterhub/?h=jupyter) of [ZIH HPC System](https://tu-dresden.de/zih/hochleistungsrechnen/hpc).

## Installation
You can install the module using pip inside jupyter hub cell:

```sh
!pip install git+https://gitlab.hrz.tu-chemnitz.de/apku868a--tu-dresden.de/big-data-environment-setup-for-jupyterhub@main
```
Output can be as follows:
![install](./images/install.png)

## Getting started

Here's an example of how to use the module inside jupyter notebook:
```python
from big_data_utils.environment_utils import ClusterConfig
from big_data_utils.cluster_utils import ClusterService
from big_data_utils.utils import kill_java_processes_by_name

spark_conf = ClusterConfig(fw_name="spark")

# Using default:
#   - configuration template
#   - configuration initialization destination
spark_conf.configure_env()

# Using default:
#   - configuration template
spark_conf.configure_env(fw_name="spark", conf_dest="./conf")

# Using specified configuration template
spark_conf.configure_env(fw_name="spark", conf_dest="./conf", conf_template="/path/to/template")

# If there are many users starting their own clusters, then there can be port 
# conflicts. In such cases 'randomize_ports' can be used to generate random port 
# numbers, at which master services can start. By default it is set to false.
spark_conf.configure_env(fw_name="spark", conf_dest="./conf", conf_template="/path/to/template", randomize_ports=True)
```
Output is as follows for one of the use case:
```
[INFO ] [23/04/2025 12:30:02] - Preparing environment setup as follows:
[INFO ] [23/04/2025 12:30:02] -   Framework                - SPARK
[INFO ] [23/04/2025 12:30:02] -   Config. template         - /data/horse/ws/apku868a-myframeworks/rapids/software/Spark/3.5.3-GCC-13.2.0-hadoop3/conf
[INFO ] [23/04/2025 12:30:02] -   Config. destination dir. - /home/apku868a/cluster-conf-16673979
[INFO ] [23/04/2025 12:30:02] -   Logging directory        - /home/apku868a/cluster-conf-16673979/log
[INFO ] [23/04/2025 12:30:02] - Initializing configuration from template.
[INFO ] [23/04/2025 12:30:03] -   Master (host:port)  - n1319:7077
[INFO ] [23/04/2025 12:30:03] -   Worker (host)       - ['n1319']
[INFO ] [23/04/2025 12:30:03] - Setup information:
Spark master URL: spark://n1319:7077

Once the cluster is started, you can access the SPARK GUI in a browser using port forwarding. To access the SPARK GUI, run the following command in your terminal on your local machine:
  ssh apku868a@login1.barnard.hpc.tu-dresden.de -L 4040:n1319:4040 -L 8080:n1319:8080 -L 8081:n1319:8081

Once the port is forwarded, you can access the GUI at:
  - localhost:4040
  - localhost:8080
  - localhost:8081
[INFO ] [23/04/2025 12:30:03] - Currently, following java processes are running:
[INFO ] [23/04/2025 12:30:04] - 	ID, Name 
[INFO ] [23/04/2025 12:30:04] - 	1307216, Jps
[INFO ] [23/04/2025 12:30:04] - Starting SPARK cluster.
[INFO ] [23/04/2025 12:30:09] - Logging cluster startup info at: /home/apku868a/cluster-conf-16673979/spark/log/cluster.log
[INFO ] [23/04/2025 12:30:09] - Currently, following java processes are running:
[INFO ] [23/04/2025 12:30:09] - 	ID, Name 
[INFO ] [23/04/2025 12:30:09] - 	1307477, Jps
[INFO ] [23/04/2025 12:30:09] - 	1307269, Master
[INFO ] [23/04/2025 12:30:09] - 	1307354, Worker
[INFO ] [23/04/2025 12:30:09] - Showing web UI for:
[INFO ] [23/04/2025 12:30:09] - Spark

```

Once the environment is setup and configuration is initialized, big data cluster
can be started. For spark it can be done in jupyter notebook as shown below:
```python
from big_data_utils.cluster_utils import ClusterService

# Initialize Spark cluster service with configuration name "spark"
spark_cluster = ClusterService("spark")

# Check initial cluster status before starting
spark_cluster.check_status()

# Start the Spark standalone cluster
spark_cluster.start_cluster()

# Verify cluster startup completion
spark_cluster.check_status()

# Once the cluster started, get web interface URL for cluster monitoring
spark_cluster.webui()
```

After starting the cluster, one can proceed with the work in subsequent cells.

## License
This project is licensed under the GNU GENERAL PUBLIC LICENSE - see the [LICENSE](./LICENSE) file for details.

## Contact
For more information contact us at [ScaDS.AI](https://scads.ai/scads-ai-team/contact/).
