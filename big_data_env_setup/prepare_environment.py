import os
import sys
import shutil
from .my_utils import SimpleLogger, run_bash_cmd

def prepare_environment(fw_name,conf_dest="default",conf_template="default"):
    logger = SimpleLogger()
    
    # Defining user defined variables
    fw_name_upper = fw_name.upper()
    fw_name_lower = fw_name.lower()
    
    if conf_dest == "default":
        conf_dest = f"{os.environ['HOME']}/cluster-conf-{os.environ['SLURM_JOBID']}"
    conf_dest = os.path.abspath(conf_dest)
    
    if os.path.isdir(conf_dest):
        logger.info("Deleting and recreating directory \"conf_dest\".")
        shutil.rmtree(conf_dest)
    else:
        os.mkdir(conf_dest) 
    
    if conf_template == "default":
        conf_template = os.environ[f"{fw_name_upper}_CONF_TEMPLATE"]
    conf_template = os.path.abspath(conf_template)
    
    logger.info("Preparing environment setup as follows:")
    logger.info(f"  Framework                - {fw_name_upper}")
    logger.info(f"  Config. template         - {conf_template}")
    logger.info(f"  Config. destination loc. - {conf_dest}")
    if fw_name == "spark":
        spark_home=os.environ["SPARK_HOME"]
    
    os.environ[f"MY_{fw_name_upper}_CONF_DEST"]=conf_dest
    os.environ[f"MY_{fw_name_upper}_CONF_TEMPLATE"]=conf_template
    
    if fw_name == "spark":
        os.environ['PYSPARK_PYTHON'] = sys.executable
   
    # Get SLURM_JOBID to create random port number
    job_id = run_bash_cmd("echo $SLURM_JOBID")
    job_digit = job_id[-3:] # Extract last three characters
    master_port=int(job_digit) + 7077
    os.environ[f"{fw_name_upper}_MASTER_PORT"]=f"{master_port}"
      
    # Initializing configuration
    logger.info("Initializing configuration from template.")
    fw_conf_opt=f"--framework {fw_name_lower} --template {conf_template} --destination {conf_dest}"
    fw_conf_cmd=f"source framework-configure.sh {fw_conf_opt}"
    output = run_bash_cmd(f"{fw_conf_cmd}; env | grep {fw_name_upper}")
    
    # Set the environment variable in Python script's environment
    for line in output.strip().split("\n"):
        if '=' in line:
            key, value = line.strip().split('=',1)    
            os.environ[key] = value

    conf_dest_full=f"{conf_dest}/{fw_name_lower}"
    os.environ[f"{fw_name_upper}_CONF_DIR"] = conf_dest_full # Configuration is initialized inside spark directory

    master_host = run_bash_cmd("scontrol show hostnames $SLURM_JOB_NODELIST | head -1")
    # Add information to spark-env.sh
    if fw_name == "spark":
        with open(f"{conf_dest_full}/spark-env.sh", "a") as f:
            f.write(f"export LD_LIBRARY_PATH={run_bash_cmd('echo $LD_LIBRARY_PATH')}\n")
            f.write(f"export {fw_name_upper}_MASTER_PORT={master_port}\n")
            f.write(f"export SPARK_MASTER_HOST={master_host}\n")
            f.close()
    
    # TODO
    logger.info(f"  Cluster master           - {master_host}")
    workers_host = run_bash_cmd("scontrol show hostnames $SLURM_JOB_NODELIST")
    logger.info(f"  Cluster worker           - {workers_host}")
    logger.info(f"  Cluster master port      - {master_port}")
    
    cluster_name=run_bash_cmd("hostname -f | cut -d'.' -f2-")

    if fw_name == "spark":
        logger.info(f"  Spark master url     - spark://{master_host}:{master_port}")
        print("")
        print("")
        logger.info(f"Once the cluster is started, one can access the spark GUI in browser using port forwarding.")
        logger.info(f"To access, spark GUI, type following in your terminal -")
        user=os.environ['USER']
        logger.info(f"  ssh {os.environ['USER']}@login1.{cluster_name} -L 4040:{master_host}:4040 -L 8080:{master_host}:8080 -L 8081:{master_host}:8081")
        print("")
        logger.info(f"Once the port is forwarded, one can access the GUI, by accessing")
        logger.info(f"  - localhost:4040")
        logger.info(f"  - localhost:8080")
        logger.info(f"  - localhost:8081")

# End of the file
