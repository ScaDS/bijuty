import os
import sys
import shutil

from big_data_utils.gui_utils import GUIUtils
from .utils import SimpleLogger, run_bash_cmd
import subprocess
import socket
from .slurm_utils import SlurmManager
import time
import psutil


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

    def find_processes_using_dir(self, target_dir):
        target_dir = self.parse_path(target_dir)
        found_processes = []

        for proc in psutil.process_iter(['pid', 'name']):
            try:
                # 1. Check if the process's Current Working Directory (CWD) is the target
                if os.path.abspath(proc.cwd()) == target_dir:
                    found_processes.append((proc.info['pid'], proc.info['name'], "Working Directory"))
                
                # 2. Check if the process has any files open inside that directory
                for file in proc.open_files():
                    if file.path.startswith(target_dir):
                        found_processes.append((proc.info['pid'], proc.info['name'], f"Open File: {file.path}"))
                        break # Found usage, no need to check other files for this PID
            
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
                
        return found_processes
                    
    def parse_path(self, path :str, resolve_symlinks=True):
        path = os.path.abspath(path)
        if "~" in path:
            path = os.path.expanduser(path)
        if resolve_symlinks:
            path = os.path.realpath(path)
        return path
        

    
    # Setup configuration from template or from default values
    def setup_config(self, gui=False, conf_dest:str=None,conf_template:str=None,randomize_ports:bool=False):
        if gui == True:
            config_gui = GUIUtils(info=dict({
                    "fw_name":self.fw_name,
                    "slurm":self.slurm
                }))
            config_gui.launch_gui_config()
            print(os.environ.get(f"{self.fw_name}_CONF_TEMPLATE"))
            print(os.environ.get(f"{self.fw_name}_CONF_DIR"))
            
            return 0
        

        self.conf_dest = self.parse_path(conf_dest) if conf_dest else None
        self.conf_template = self.parse_path(conf_template) if conf_template else None
        self.randomize_ports = randomize_ports
        
        if self.slurm.in_slurm_job:
            if not self.conf_dest:
                # self.conf_dest = os.path.abspath(f"{os.environ['HOME']}/cluster-conf-{os.environ['SLURM_JOBID']}")
                self.conf_dest = self.parse_path(f"./cluster-conf-{os.environ['SLURM_JOBID']}")
            
            # If the configuration directory already exists, archive it and create a new one
            if os.path.isdir(self.conf_dest):
                logger.info(f"Archiving existing configuration directory: '{self.conf_dest}'")
                
                results = self.find_processes_using_dir(self.conf_dest)
                if results:
                    print(f"{'PID':<10} {'Process Name':<25} {'Reason'}")
                    print("-" * 60)
                    for pid, name, reason in results:
                        print(f"{pid:<10} {name:<25} {reason}")
                try:
                    # Define the archive name (shutil adds the .zip extension automatically)
                    archive_name = f"{self.conf_dest}_backup_{time.time_ns()}"
                    
                    # Create the zip file
                    shutil.make_archive(archive_name, 'zip', self.conf_dest)
                    
                    # Once zipped, remove the original directory
                    shutil.rmtree(self.conf_dest)
                    logger.info(f"Successfully archived to {archive_name}.zip and removed original directory.")
                    
                except Exception as e:
                    logger.error(f"Error archiving configuration directory: {e}")
                    if results:
                        print(f"{'PID':<10} {'Process Name':<25} {'Reason'}")
                        print("-" * 60)
                        for pid, name, reason in results:
                            print(f"{pid:<10} {name:<25} {reason}")
                    else:
                        print("No processes found using this directory.")
                    logger.error("Create a new cell and kill the processes using command \"!kill <process_id>\"")
            else:
                logger.info(f"Creating new configuration directory: '{self.conf_dest}'")
                os.makedirs(self.conf_dest, exist_ok=True)
            
            if self.conf_template == None:
                # If template is not provided, try to get from environment variable
                self.conf_template = os.environ[f"{self.fw_name}_CONF_TEMPLATE"]
            self.conf_template = self.parse_path(self.conf_template)
        
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

    # def get_worker_hosts(self):
    #     return self.worker_hosts

    # def get_master_host(self):
    #     return self.master_host

    # def get_master_port(self):
    #     return self.master_port

    # def get_slurm_nodelist():
    #     return run_bash_cmd("scontrol show hostnames $SLURM_JOB_NODELIST").split("\n")

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


# End of the file
