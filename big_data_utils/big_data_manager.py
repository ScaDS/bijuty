import os
import sys
import shutil
from .utils import SimpleLogger, run_bash_cmd
import subprocess
import socket
from .slurm_utils import SlurmManager
import time

logger = SimpleLogger()


# TODO
# - logger message formatting

class BigDataManager:
    
    def __init__(self, fw_name, debug_set_slurm_true=False):
        self.fw_name = fw_name.upper()
        if self.fw_name not in [ 'FLINK', 'SPARK']:
            raise Exception("Sorry, frameworks other than 'flink' and 'spark' is not supported.")
        
        self.slurm = SlurmManager()
        if debug_set_slurm_true:
            self.slurm.in_slurm_job = debug_set_slurm_true
        
        self.cluster_name = socket.getfqdn().strip()
                
        self.message_spacer = ' '*2

    def __repr__(self):
        s = f"framework={self.fw_name}\n"
        # s = s + f"hpc_domain_match_str={self.cluster_name}\n"
        s = s + f"is_hpc={self.slurm.in_slurm_job}\n"
        s = s + f"primary_hostname={self.cluster_name}"
        return s

    # Setup configuration from template or from default values
    def setup_config(self, conf_dest=None,conf_template=None,randomize_ports=False):
        self.conf_dest = conf_dest
        self.conf_template = conf_template
        self.randomize_ports = randomize_ports
        
        if self.slurm.in_slurm_job:
            if not self.conf_dest:
                # self.conf_dest = os.path.abspath(f"{os.environ['HOME']}/cluster-conf-{os.environ['SLURM_JOBID']}")
                self.conf_dest = os.path.abspath(f"./cluster-conf-{os.environ['SLURM_JOBID']}")
            
            if os.path.isdir(self.conf_dest):
                logger.info(f"Removing existing configuration directory: '{self.conf_dest}'")
                try:
                    shutil.move(self.conf_dest,f"{self.conf_dest}_old_{time.time_ns()}")
                except Exception as e:
                    logger.error(f"Error removing existing configuration directory: {e}")
                    logger.info(f"Checking for running {self.fw_name} processes...")
                    # Run the jps command to check for running Spark processes
                    jps_output = subprocess.check_output(["jps"]).decode("utf-8")
                    logger.error(jps_output)
                    logger.error("Create a new cell. And kill the processes using command \"!kill <process_id>\"")
                    raise Exception("Master and worker processes are already running")
            else:
                logger.debug(f"Creating new configuration directory: '{self.conf_dest}'")
                os.mkdir(self.conf_dest)
            
            if self.conf_template == None:
                self.conf_template = os.environ[f"{self.fw_name}_CONF_TEMPLATE"]
            self.conf_template = os.path.abspath(self.conf_template)
        
            logger.info("Environment configuration initialized:")
            logger.info(f"{self.message_spacer}• Framework:        {self.fw_name}")
            logger.info(f"{self.message_spacer}• Config template:  {self.conf_template}")
            logger.info(f"{self.message_spacer}• Config target:    {self.conf_dest}")
            
            if self.fw_name == "SPARK":
                logger.info(f"{self.message_spacer}• Log directory:    {self.conf_dest}/log")

            os.environ[f"MY_{self.fw_name}_CONF_DEST"]=self.conf_dest
            os.environ[f"MY_{self.fw_name}_CONF_TEMPLATE"]=self.conf_template
            
            if self.fw_name == "SPARK":
                os.environ['PYSPARK_PYTHON'] = sys.executable
                    
            # Initializing configuration
            logger.info("Initializing configuration from template.")
            fw_conf_opt=f"--framework {self.fw_name.lower()} --template {self.conf_template} --destination {self.conf_dest}"
            fw_conf_cmd=f"source framework-configure.sh {fw_conf_opt}"
            output = run_bash_cmd(f"{fw_conf_cmd}; env | grep {self.fw_name}")
            
            # Set the environment variable in Python script's environment
            for line in output.strip().split("\n"):
                if '=' in line:
                    key, value = line.strip().split('=',1)    
                    os.environ[key] = value
    
            conf_dest_full=f"{self.conf_dest}/{self.fw_name.lower()}"
            os.environ[f"{self.fw_name}_CONF_DIR"] = conf_dest_full # Configuration is initialized inside spark directory
            
            if self.fw_name == "SPARK":
                self.master_port = 7077
            elif self.fw_name == "FLINK":
                self.master_port = 8081
            
            if self.randomize_ports:    
                # Get SLURM_JOBID to create random port number
                job_id = os.environ['SLURM_JOBID']
                job_digit = job_id[-3:] # Extract last three characters
                self.master_port=int(job_digit) + 7077
                os.environ[f"{self.fw_name}_MASTER_PORT"]=f"{self.master_port}"
           
            slurm_node_list = get_slurm_nodelist()
 
            self.master_host = slurm_node_list[0]
            self.worker_hosts = slurm_node_list

            logger.info("Cluster topology:")
            logger.info(f"{self.message_spacer}• Master node:  {self.master_host}:{self.master_port}")
            logger.info(f"{self.message_spacer}• Worker nodes: {', '.join(self.worker_hosts)}")
            
            # Add information to spark-env.sh
            if self.fw_name == "SPARK":
                with open(f"{conf_dest_full}/spark-env.sh", "a") as f:
                    f.write(f"export LD_LIBRARY_PATH={run_bash_cmd('echo $LD_LIBRARY_PATH')}\n")
                    f.write(f"export {self.fw_name}_MASTER_PORT={self.master_port}\n")
                    f.write(f"export SPARK_MASTER_HOST={self.master_host}\n")
                    f.close()

                # Replace port in custom spark-submit command
                run_bash_cmd(f"sed -i 's!\(spark://\)[a-zA-Z0-9]*:[0-9]*!\1{self.master_host}:{self.master_port}!' {conf_dest_full}/spark-submit")
 
            if self.fw_name == "SPARK":
                master_url_info = f"{self.message_spacer}• Spark master URL: spark://{self.master_host}:{self.master_port}"
            elif self.fw_name == "FLINK":
                master_url_info = f""""""
            logger.info(f"{master_url_info}")
        
            if self.fw_name == "SPARK":
                logger.info(f"Once the cluster is started, one can access the spark GUI in browser using port forwarding.")
                logger.info(f"To access, spark GUI, type following in your terminal on local machine:")
                logger.info(f"{self.message_spacer}ssh {os.environ['USER']}@login1.{self.cluster_name} -L 4040:{self.master_host}:4040 -L 8080:{self.master_host}:8080 -L 8081:{self.master_host}:8081")
                logger.info(f"Once the port is forwarded, one can access the GUI, by accessing")
                logger.info(f"{self.message_spacer}• http://localhost:4040")
                logger.info(f"{self.message_spacer}• http://localhost:8080")
                logger.info(f"{self.message_spacer}• http://localhost:8081")
  
    # Modify worker file
    def set_workers(self,worker_list):
        if f"{self.fw_name}_CONF_DIR" in os.environ:
            if self.fw_name == "SPARK":
                worker_file = f"{os.environ['SPARK_CONF_DIR']}/workers"

            with open(worker_file, "w") as file:
                for worker_i in worker_list:
                    file.write(worker_i + "\n")
        else:
            logger.error("The configuration is not initialized yet.")
            logger.error("Please inititalize configuration using:  ") 
            logger.error("<your-config-class-variable>.configure_env(...)")

    def get_worker_hosts(self):
        return self.worker_hosts

    def get_master_host(self):
        return self.master_host

    def get_master_port(self):
        return self.master_port

def get_slurm_nodelist():
    return run_bash_cmd("scontrol show hostnames $SLURM_JOB_NODELIST").split("\n")


# End of the file
