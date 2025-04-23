import os
import time
from .utils import run_bash_cmd, SimpleLogger
from IPython.display import IFrame, display
#import ipywidgets as widgets
from ipywidgets import Tab, HBox, VBox, Button, HTML, Layout
from IPython.display import display
    
logger = SimpleLogger()

class ClusterService:
    def __init__(self, fw_name):
        self.fw_name = fw_name.upper()
        self.fw_conf_dir = os.environ[f"{self.fw_name}_CONF_DIR"]
                       
        if self.fw_name in [ 'SPARK', 'FLINK']:
            self.cluster_log = f"{self.fw_conf_dir}/log/cluster.log"
            self.log_dir = f"{self.fw_conf_dir}/log"
    
    def start_cluster(self):    
        logger.info(f"Starting {self.fw_name} cluster.")
        
        if self.fw_name=='SPARK':
            run_bash_cmd(f"nohup start-all.sh > {self.cluster_log} 2>&1")
        elif self.fw_name=='FLINK':
            run_bash_cmd(f"nohup start-cluster.sh > {self.cluster_log} 2>&1")
    
        time.sleep(5)
        logger.info(f"Logging cluster startup info at: {self.cluster_log}")
    
    def stop_cluster(self):
        logger.info(f"Stopping {self.fw_name} cluster.")
        
        if self.fw_name=='SPARK':
            run_bash_cmd(f"nohup stop-all.sh >> {self.cluster_log} 2>&1")
        elif self.fw_name=='FLINK':
            run_bash_cmd(f"nohup stop-cluster.sh >> {self.cluster_log} 2>&1")
       
        time.sleep(3)
        logger.info(f"Logging cluster stopping info at: {self.cluster_log}")

    def kill_cluster_processes(self):
        logger.info(f"Killing {self.fw_name} cluster processes.")
        if self.fw_name == 'FLINK':
            process_names = ['TaskManagerRunner', 'StandaloneSessionClusterEntrypoint']
        elif self.fw_name == 'SPARK':
            process_names = ['SparkSubmit', 'CoarseGrainedExecutorBackend', 'Master', 'Worker']
        else:
            logger.error(f"Unsupported framework: {self.fw_name}")
            return

        for process_name in process_names:
            logger.info(f"Killing process {process_name}")
            run_bash_cmd(f"pkill -f {process_name}")
        logger.info(f"Cluster processes killed.")

    def check_status(self):
        logger.info("Currently, following java processes are running:")
        mylines=run_bash_cmd("jps").split("\n")
        logger.info(f"\tID, Name ")
        
        for lines in mylines:
            words = lines.split(" ",1)
            logger.info(f"\t{words[0]}, {words[1]}")

    def webui(self, url=None, width="100%", height="600px"):
        """
        Embed a web UI in an iframe with JupyterHub launch buttons.
        Args:
        - url (str): The URL to embed. Defaults to framework-specific URLs if None.
        - width (str): Width of the iframe. Default is "100%".
        - height (str): Height of the iframe. Default is "600px".
        """
        # Create JupyterHub buttons
        #logger.info(f"{os.environ}")
        #hub_url = f"{service_prefix}hub/home"
        #lab_url = f"{service_prefix}lab"
    
        # hub_btn = Button(description='JupyterHub', layout=Layout(width='150px'))
        # hub_btn.on_click(lambda b: display(HTML(f'<script>window.open("{hub_url}", "_blank")</script>')))
    
        # lab_btn = Button(description='JupyterLab', layout=Layout(width='150px'))
        # lab_btn.on_click(lambda b: display(HTML(f'<script>window.open("{lab_url}", "_blank")</script>')))
    
        # Display framework-specific UI
        logger.info("Showing web UI for:")
        if self.fw_name == 'FLINK':
            logger.info("Flink")
            if url is None:
                url = f"https://jupyterhub.hpc.tu-dresden.de/user/{os.environ['USER']}/proxy/8081/"
                
            iframe = HTML(value=f'<iframe src="{url}" width="{width}" height="{height}"></iframe>')
            #display(VBox([HBox([hub_btn, lab_btn]), iframe]))
            display(iframe)
    
        elif self.fw_name == 'SPARK':
            logger.info("Spark")
            if url is None:
                url = f"https://jupyterhub.hpc.tu-dresden.de/user/{os.environ['USER']}/proxy/8080/"
                worker_url = f"https://jupyterhub.hpc.tu-dresden.de/user/{os.environ['USER']}/proxy/8081/"
                app_url = f"https://jupyterhub.hpc.tu-dresden.de/user/{os.environ['USER']}/proxy/4040/"
                
                cluster_ui = HTML(value=f'<iframe src="{url}" width="{width}" height="{height}"></iframe>')
                worker_ui = HTML(value=f'<iframe src="{worker_url}" width="{width}" height="{height}"></iframe>')
                app_ui = HTML(value=f'<iframe src="{app_url}" width="{width}" height="{height}"></iframe>')
                
                tabs = Tab(children=[cluster_ui, worker_ui, app_ui])
                tabs.set_title(0, "Cluster UI")
                tabs.set_title(1, "Worker UI")
                tabs.set_title(2, "App UI")
                display(tabs)
            else:
                iframe = HTML(value=f'<iframe src="{url}" width="{width}" height="{height}"></iframe>')
                display(iframe)
