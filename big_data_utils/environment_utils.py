import os
import sys
import shutil
from .utils import SimpleLogger, run_bash_cmd

logger = SimpleLogger()

# TODO
# - logger message formatting

class ClusterConfig:
    
    def __init__(self, fw_name):
        self.fw_name = fw_name.upper()
        
        self.is_hpc = running_on_hpc()
        if self.is_hpc:
            self.cluster_name = run_bash_cmd("hostname -f | cut -d'.' -f2-")
        else:
            self.cluster_name = "localhost"

    def running_on_hpc():
        test=run_bash_cmd("hostname -f | cut -d'.' -f2-").strip()
        return test == "hpc.tu-dresden.de"

    def configure_env(conf_dest="default",conf_template="default"):
        # Defining user defined variables
        fw_name_upper = self.fw_name.upper()
        fw_name_lower = self.fw_name.lower()
        
        if is_hpc:
            # Option handling
            if conf_dest == "default":
                self.conf_dest = os.path.abspath(f"{os.environ['HOME']}/cluster-conf-{os.environ['SLURM_JOBID']}")
            
            if os.path.isdir(self.conf_dest):
                logger.info(f"Deleting and recreating directory \"{self.conf_dest}\".")
                shutil.rmtree(self.conf_dest)
            else:
                os.mkdir(self.conf_dest) 
            
            if conf_template == "default":
                self.conf_template = os.environ[f"{fw_name_upper}_CONF_TEMPLATE"]
            self.conf_template = os.path.abspath(self.conf_template)
        
            logger.info("Preparing environment setup as follows:")
            logger.info(f"  Framework                - {fw_name_upper}")
            logger.info(f"  Config. template         - {self.conf_template}")
            logger.info(f"  Config. destination dir. - {self.conf_dest}")
            if fw_name_upper == "SPARK":
                logger.info(f"  Logging directory        - {self.conf_dest}/log")
    
            os.environ[f"MY_{fw_name_upper}_CONF_DEST"]=self.conf_dest
            os.environ[f"MY_{fw_name_upper}_CONF_TEMPLATE"]=self.conf_template
            
            if fw_name_upper == "SPARK":
                os.environ['PYSPARK_PYTHON'] = sys.executable
       
             
            # Initializing configuration
            logger.info("Initializing configuration from template.")
            fw_conf_opt=f"--framework {fw_name_lower} --template {self.conf_template} --destination {self.conf_dest}"
            fw_conf_cmd=f"source framework-configure.sh {fw_conf_opt}"
            output = run_bash_cmd(f"{fw_conf_cmd}; env | grep {fw_name_upper}")
            
            # Set the environment variable in Python script's environment
            for line in output.strip().split("\n"):
                if '=' in line:
                    key, value = line.strip().split('=',1)    
                    os.environ[key] = value
    
            conf_dest_full=f"{self.conf_dest}/{fw_name_lower}"
            os.environ[f"{fw_name_upper}_CONF_DIR"] = conf_dest_full # Configuration is initialized inside spark directory
            
            # Get SLURM_JOBID to create random port number
            job_id = run_bash_cmd("echo $SLURM_JOBID")
            job_digit = job_id[-3:] # Extract last three characters
            self.master_port=int(job_digit) + 7077
            os.environ[f"{fw_name_upper}_MASTER_PORT"]=f"{self.master_port}"
            
            self.master_host = run_bash_cmd("scontrol show hostnames $SLURM_JOB_NODELIST | head -1")
            self.worker_hosts = run_bash_cmd("scontrol show hostnames $SLURM_JOB_NODELIST")
            
            logger.info(f"  Master (host:port)  - {self.master_host}:{self.master_port}")
            logger.info(f"  Worker (host)       - {self.worker_hosts}")
            
            # Add information to spark-env.sh
            if fw_name_upper == "SPARK":
                with open(f"{conf_dest_full}/spark-env.sh", "a") as f:
                    f.write(f"export LD_LIBRARY_PATH={run_bash_cmd('echo $LD_LIBRARY_PATH')}\n")
                    f.write(f"export {fw_name_upper}_MASTER_PORT={self.master_port}\n")
                    f.write(f"export SPARK_MASTER_HOST={self.master_host}\n")
                    f.close()

                # Replace port in custom spark-submit command
                run_bash_cmd(f"sed -i 's!\(spark://\)[a-zA-Z0-9]*:[0-9]*!\1{self.master_host}:{self.master_port}!' {conf_dest_full}/spark-submit")
    
    
            if fw_name_upper == "SPARK":
                logger.info(f"  Spark master url     - spark://{self.master_host}:{self.master_port}")
                print("")
                print("")
                logger.info(f"Once the cluster is started, one can access the spark GUI in browser using port forwarding.")
                logger.info(f"To access, spark GUI, type following in your terminal -")
                user=os.environ['USER']
                logger.info(f"  ssh {os.environ['USER']}@login1.{self.cluster_name} -L 4040:{self.master_host}:4040 -L 8080:{self.master_host}:8080 -L 8081:{self.master_host}:8081")
                print("")
                logger.info(f"Once the port is forwarded, one can access the GUI, by accessing")
                logger.info(f"\t- localhost:4040")
                logger.info(f"\t- localhost:8080")
                logger.info(f"\t- localhost:8081")
                print("")
    
   
    def get_worker_hosts():
        return self.worker_hosts

    def get_master_host():
        return self.master_host

    def get_master_port():
        return self.master_port


# Enn of the file
