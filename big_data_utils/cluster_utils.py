import os
from .my_utils import run_bash_cmd, SimpleLogger

logger = SimpleLogger()

class ClusterService:
    def __init__(self, fw_name):
        self.fw_name = fw_name.upper()
        self.fw_conf_dir = os.environ[f"{self.fw_name}_CONF_DIR"]
                       
        if self.fw_name=='SPARK':
            self.cluster_log = f"{self.fw_conf_dir}/log/cluster.log"
            self.log_dir = f"{self.fw_conf_dir}/log"
    
    def start_cluster(self):    
        logger.info(f"Starting {self.fw_name} cluster.")
        
        if self.fw_name=='SPARK':
            run_bash_cmd(f"nohup start-all.sh > {self.cluster_log} 2>&1")
    
        logger.info(f"Logging cluster startup info at: {self.cluster_log}")

    def stop_cluster(self):
        logger.info(f"Stopping {self.fw_name} cluster.")
        
        if self.fw_name=='SPARK':
            run_bash_cmd(f"nohup stop-all.sh >> {self.cluster_log} 2>&1")
            logger.info(f"Logging cluster stopping info at: {self.cluster_log}")

    def check_status(self):
        logger.info("Currently, following java processes are running:")
        mylines=run_bash_cmd("jps").split("\n")
        logger.info(f"\tID, Name ")
        
        for lines in mylines:
            words = lines.split(" ",1)
            logger.info(f"\t{words[0]}, {words[1:]}")
