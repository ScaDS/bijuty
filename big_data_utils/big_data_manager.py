import os
import sys
import shutil

# from big_data_utils.gui_utils import GUIUtils
from .utils import SimpleLogger, run_bash_command
import subprocess
import socket
from .slurm_utils import SlurmManager
import time
import psutil
import getpass
from .process_monitor import ProcessMonitor
from types import SimpleNamespace

logger = SimpleLogger()


# TODO
# - logger message formatting

class BigDataManager:
    
    def __init__(self, debug_set_slurm_true=False):
        # self.user_inputs.fw_name = fw_name.upper()
        
        self.user_inputs = SimpleNamespace()

        self.initialized = False
        self.message_spacer = ' '*2
        self.user_inputs.fw_name= ""
        self.user_inputs.master = ""
        self.user_inputs.workers = []
        self.user_inputs.master_port = ""
        self.user_inputs.conf_dir = ""
        self.user_inputs.log_dir = ""
        self.fw_mapping = {}

# if self.user_inputs.fw_name not in [ 'FLINK', 'SPARK']:
        #     raise Exception("Sorry, frameworks other than 'flink' and 'spark' is not supported.")
        # self.slurm = SlurmManager()
        # if debug_set_slurm_true:
        #     self.slurm.in_slurm_job = debug_set_slurm_true        
        # self.cluster_name = socket.getfqdn().strip()
        # self.fw_mapping = {
        #     "SPARK": {
        #         "start_proc_cmd": "start-all.sh",
        #         "stop_proc_cmd": "stop-all.sh",
        #         "proc_name_master": "org.apache.spark.deploy.master.Master --host",
        #         "proc_name_worker": "org.apache.spark.deploy.worker.Worker --webui-port",
        #         "proc_name_other": [
        #             "org.apache.spark.deploy.SparkSubmit",
        #             "org.apache.spark.executor.CoarseGrainedExecutorBackend",
        #             "org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend"
        #         ],
        #         "logo":"https://spark.apache.org/images/spark-logo-back.png",
        #         "worker_file": "workers",
        #         "default_master_port": 7077,
        #         "default":{
        #             "mem_driver": 1000,
        #             "mem_worker": 1000,
        #             "mem_executor": 1000,
        #             "cpu_driver": 1,
        #             "cpu_worker": 1,
        #             "cpu_executor": 1,
        #         }

        #     },
        #     "FLINK": {
        #         "start_proc_cmd": "start-cluster.sh",
        #         "stop_proc_cmd": "stop-cluster.sh",
        #         "proc_name_master": "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint",
        #         "proc_name_worker": "org.apache.flink.runtime.taskexecutor.TaskManagerRunner",
        #         "logo":"https://flink.apache.org/img/logo/png/200/flink_squirrel_200_color.png",
        #         "worker_file": "workers",
        #         "default_master_port": 8081,
        #     }
        # }

    # def __repr__(self):
    #     s = f"framework={self.user_inputs.fw_name}\n"
    #     # s = s + f"hpc_domain_match_str={self.cluster_name}\n"
    #     s = s + f"is_hpc={self.slurm.in_slurm_job}\n"
    #     s = s + f"primary_hostname={self.cluster_name}"
    #     return s

    # To check if any particular process is using any directory
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
        
    def initialize_user_input(self,props):
        self.user_inputs.fw_name = str(props.get('fw_name'))
        self.user_inputs.master = str(props.get('master'))
        self.user_inputs.workers = props.get('workers')
        self.user_inputs.master_port = str(props.get('master_port'))
        self.user_inputs.conf_dir = str(props.get('conf_dir'))
        self.user_inputs.log_dir = str(props.get('log_dir'))
        self.fw_mapping = props.get('fw_mapping')
        self.initialized = True
        # print(type(self.fw_mapping))
        # print(self.fw_mapping)
        
    
    # Setup configuration from template or from default values
    def setup_config(self, gui=False, conf_dest:str=None,conf_template:str=None,randomize_ports:bool=False):
        self.gui = gui
        if self.gui == True:
            # self.user_inputs = GUIUtils(info=dict({
            #         "slurm":self.slurm,
            #         "fw_mapping":self.fw_mapping,
            #     }))
            # self.user_inputs.launch_gui_config()
            print("Moved")

        else:
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
                    self.conf_template = os.environ[f"{self.user_inputs.fw_name}_CONF_TEMPLATE"]
                self.conf_template = self.parse_path(self.conf_template)
            
                logger.info("Environment configuration initialized:")
                logger.info(f"{self.message_spacer}• Framework:        {self.user_inputs.fw_name}")
                logger.info(f"{self.message_spacer}• Config template:  {self.conf_template}")
                logger.info(f"{self.message_spacer}• Config target:    {self.conf_dest}")
                
                if self.user_inputs.fw_name == "SPARK":
                    logger.info(f"{self.message_spacer}• Log directory:    {self.get_log_dir()}")

                os.environ[f"MY_{self.user_inputs.fw_name}_CONF_DEST"]=self.conf_dest
                os.environ[f"MY_{self.user_inputs.fw_name}_CONF_TEMPLATE"]=self.conf_template
                
                if self.user_inputs.fw_name == "SPARK":
                    os.environ['PYSPARK_PYTHON'] = sys.executable
                        
                # Initializing configuration
                logger.info("Initializing configuration from template.")
                fw_conf_opt=f"--framework {self.user_inputs.fw_name.lower()} --template {self.conf_template} --destination {self.conf_dest}"
                fw_conf_cmd=f"source framework-configure.sh {fw_conf_opt}"
                output, error, returncode = run_bash_command([f"{fw_conf_cmd}; env | grep {self.user_inputs.fw_name}"])[0]
                
                if returncode != 0:
                    logger.error(f"Failed to initialize configuration: {error}")
                    return

                # Set the environment variable in Python script's environment
                for line in output.strip().split("\n"):
                    if '=' in line:
                        key, value = line.strip().split('=',1)    
                        os.environ[key] = value
        
                conf_dest_full=f"{self.conf_dest}/{self.user_inputs.fw_name.lower()}"
                os.environ[f"{self.user_inputs.fw_name}_CONF_DIR"] = conf_dest_full # Configuration is initialized inside spark directory
                
                self.master_port = self.fw_mapping.get(self.user_inputs.fw_name.upper()).get("default_master_port", self.master_port)
                if self.randomize_ports:    
                    # Get SLURM_JOBID to create random port number
                    job_id = os.environ['SLURM_JOBID']
                    job_digit = job_id[-3:] # Extract last three characters
                    self.master_port=int(job_digit) + 7077
                    os.environ[f"{self.user_inputs.fw_name}_MASTER_PORT"]=f"{self.master_port}"
            
                slurm_node_list = self.slurm.get_nodes_list()
    
                self.master_host = slurm_node_list[0]
                self.worker_hosts = slurm_node_list

                logger.info("Cluster topology:")
                logger.info(f"{self.message_spacer}• Master node:  {self.master_host}:{self.master_port}")
                logger.info(f"{self.message_spacer}• Worker nodes: {', '.join(self.worker_hosts)}")
                
                # Add information to spark-env.sh
                if self.user_inputs.fw_name == "SPARK":
                    with open(f"{conf_dest_full}/spark-env.sh", "a") as f:
                        f.write(f"export LD_LIBRARY_PATH={run_bash_command('echo $LD_LIBRARY_PATH')}\n")
                        f.write(f"export {self.user_inputs.fw_name}_MASTER_PORT={self.master_port}\n")
                        f.write(f"export SPARK_MASTER_HOST={self.master_host}\n")
                        f.close()

                    # Replace port in custom spark-submit command
                    run_bash_command(f"sed -i 's!\(spark://\)[a-zA-Z0-9]*:[0-9]*!\1{self.master_host}:{self.master_port}!' {conf_dest_full}/spark-submit")
    
                if self.user_inputs.fw_name == "SPARK":
                    master_url_info = f"{self.message_spacer}• Spark master URL: spark://{self.master_host}:{self.master_port}"
                elif self.user_inputs.fw_name == "FLINK":
                    master_url_info = f""""""
                logger.info(f"{master_url_info}")
            
                if self.user_inputs.fw_name == "SPARK":
                    logger.info(f"Once the cluster is started, one can access the spark GUI in browser using port forwarding.")
                    logger.info(f"To access, spark GUI, type following in your terminal on local machine:")
                    logger.info(f"{self.message_spacer}ssh {os.environ['USER']}@login1.{self.cluster_name} -L 4040:{self.master_host}:4040 -L 8080:{self.master_host}:8080 -L 8081:{self.master_host}:8081")
                    logger.info(f"Once the port is forwarded, one can access the GUI, by accessing")
                    logger.info(f"{self.message_spacer}• http://localhost:4040")
                    logger.info(f"{self.message_spacer}• http://localhost:8080")
                    logger.info(f"{self.message_spacer}• http://localhost:8081")
  
                # Modify worker file
                def set_workers(self,worker_list):
                    if f"{self.user_inputs.fw_name}_CONF_DIR" in os.environ:
                        if self.user_inputs.fw_name == "SPARK":
                            worker_file = f"{os.environ['SPARK_CONF_DIR']}/workers"

                        with open(worker_file, "w") as file:
                            for worker_i in worker_list:
                                file.write(worker_i + "\n")
                    else:
                        logger.error("The configuration is not initialized yet.")
                        logger.error("Please inititalize configuration using:  ") 
                        logger.error("<your-config-class-variable>.configure_env(...)")

    def get_worker_hosts(self):
        return self.user_inputs.workers # if self.gui else self.worker_hosts

    def get_master_host(self):
        return self.user_inputs.master # if self.gui else self.master_host

    def get_master_port(self):
        return self.user_inputs.master_port # if self.gui else self.master_port

    def get_conf_dir(self):
        return self.user_inputs.conf_dir # if self.gui else self.conf_dest
    
    def get_log_dir(self):
        return self.user_inputs.log_dir # if self.gui else f"{self.conf_dest}/log"

    def get_cluster_log_file(self):
        return self.get_log_dir() + "/cluster_log"

    def get_fw_cluster_processes(self,all=False):
        if self.fw_mapping != {}:
            master_proc_name  = self.fw_mapping.get(self.user_inputs.fw_name.upper()).get("proc_name_master", None)
            worker_proc_name =  self.fw_mapping.get(self.user_inputs.fw_name.upper()).get("proc_name_worker", None)

            if all == True:
                other_processes = self.fw_mapping.get(self.user_inputs.fw_name.upper()).get("other_processes", [])
                return master_proc_name, worker_proc_name, *other_processes
        else:
            master_proc_name, worker_proc_name = None, None
        return master_proc_name, worker_proc_name

    def get_current_user(self):
        return getpass.getuser()

    def is_cluster_up(self):
        # If the variables are not initialized return without executing
        if not self.initialized:
            return False
        
        master_proc_name, worker_proc_name  = self.get_fw_cluster_processes()
         
        if not master_proc_name or not worker_proc_name: return False

        current_user = getpass.getuser()

        found_master = False
        worker_counter = 0
        worker_count = len(self.get_worker_hosts())
        found_workers = False
        
        for proc in psutil.process_iter(['username', 'cmdline']):
            try:
                if proc.info['username'] != current_user: continue
                cmdline = proc.info['cmdline'] or []
                if len(cmdline) <= 0: continue

                

                cmdline = " ".join(cmdline)
                if master_proc_name in cmdline or worker_proc_name in cmdline:
                    logger.debug(f"Checking process PID {proc.pid} with command line: {cmdline}")
                if master_proc_name in cmdline:
                    found_master = True
                if worker_proc_name in cmdline:
                    for worker_i in self.get_worker_hosts():
                        if worker_i in cmdline:
                            worker_counter += 1
                    if worker_counter >= worker_count:
                        found_workers = True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        logger.debug(f"Status: Master {'UP' if found_master else 'DOWN'}, Workers: {worker_counter}/{worker_count} {'UP' if found_workers else 'DOWN'}")

        return found_master and found_workers

    # Wrapper for above function to wait until the cluster is up, with a timeout
    def wait_for_cluster_init(self, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.is_cluster_up():
                return True
            time.sleep(1)
        return False

    def wait_for_cluster_stop(self, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not self.is_cluster_up():
                return True
            time.sleep(1)
        return False

    def start_cluster(self):
        # If the variables are not initialized return without executing
        if not self.initialized:
            return False
        
        # Check if cluster is already up
        if self.is_cluster_up():
            logger.info(f"{self.user_inputs.fw_name} cluster is already running.")
            self.stop_cluster()
        
        logger.info(f"Starting {self.user_inputs.fw_name} cluster.")

        cmd_script = self.fw_mapping \
                            .get(self.user_inputs.fw_name.upper()) \
                            .get("start_proc_cmd")
        
        if not cmd_script:
            logger.error(f"Unsupported framework: {self.user_inputs.fw_name}")
            return

        log_path = self.get_cluster_log_file()
        # full_cmd = f"nohup {cmd_script} > {log_path} 2>&1"
        full_cmd = f"{cmd_script} > {log_path} 2>&1"
        logger.debug("Runnin: " + full_cmd)
        
        # try:
        output, error, return_code = run_bash_command(full_cmd)
        logger.debug(output)
        if return_code != 0:
            logger.error("Error ocurred while starting cluster..\n" + error)
            return False
        
        if self.wait_for_cluster_init(timeout=1000):
            logger.info(f"{self.user_inputs.fw_name} cluster started successfully.")
            return True                
        # except Exception as e:
        logger.error(f"Failed to start {self.user_inputs.fw_name} cluster.")
        
        return False
    
    def cleanup_cluster(self):
        required_procs = list(self.get_fw_cluster_processes(all=True))
        if len(required_procs) == 0:
            logger.error(f"No process names defined for framework {self.user_inputs.fw_name}. Cannot perform cleanup.")
            return
        
        found_procs = []
        for proc in psutil.process_iter(['username','pid', 'name', 'cmdline']):
            if proc.info['username'] != self.get_current_user(): continue
            cmdline = proc.info['cmdline'] or []
            if len(cmdline) <= 0: continue

            try:
                cmdline = " ".join(cmdline)
                if any(req_proc in cmdline for req_proc in required_procs):
                    found_procs.append((proc.pid, proc.info['name'], cmdline))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if len(found_procs) == 0:
            logger.info(f"No running cluster processes found for framework {self.user_inputs.fw_name}.")
            return

        logger.info(f"Found {len(found_procs)} processes related to {self.user_inputs.fw_name}. Terminating...")
        for pid, name, cmdline in found_procs:
            logger.info(f"Terminating PID {pid} ({name}): {cmdline}")
            try:
                proc = psutil.Process(pid)
                proc.terminate()
                proc.wait(timeout=5)
                logger.info(f"Successfully terminated PID {pid}.")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                logger.error(f"Failed to terminate PID {pid}: {e}")
        
        _, alive = psutil.wait_procs(found_procs, timeout=5)
        for p in alive:
            logger.warning(f"Process {p.pid} did not exit. Hard killing...")
            try:
                p.kill()
            except psutil.NoSuchProcess:
                continue
        logger.info(f"Cleanup complete for {self.user_inputs.fw_name}.")

    def stop_cluster(self):
        # If the variables are not initialized return without executing
        if not self.initialized:
            return False
        
        logger.info(f"Stopping {self.user_inputs.fw_name} cluster.")        
        cmd_script = self.fw_mapping.get(self.user_inputs.fw_name.upper()).get("stop_proc_cmd")
        try:
            run_bash_command(f"nohup {cmd_script} >> {self.get_cluster_log_file()} 2>&1")
            if self.wait_for_cluster_stop(timeout=30):
                logger.info(f"{self.user_inputs.fw_name} cluster stopped successfully.")
                return True
            else:
                logger.error(f"Failed to stop {self.user_inputs.fw_name} cluster gracefully within timeout.\nAttempting to kill cluster processes.")
                self.cleanup_cluster()
                return False
        except Exception as e:
            logger.error(f"Failed to stop {self.user_inputs.fw_name} cluster: {e}")
            return False
    
    def show_metrics(self):
        proc_monitor = ProcessMonitor(self.get_fw_cluster_processes(all=True))
        proc_monitor.show()


# End of the file
