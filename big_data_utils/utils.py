import datetime
import subprocess
import psutil

class SimpleLogger:
    def log(self, message, log_type):
        timestamp = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        print(f"[{log_type}] [{timestamp}] - {message}")

    def info(self, message):
        self.log(message, "INFO ")

    def error(self, message):
        self.log(message, "ERROR")

def run_bash_cmd(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, executable='/bin/bash')
    return result.stdout.decode().strip()

def load_env_file(filepath):
    with open(filepath) as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

def kill_java_processes_by_name(process_name):
    
    java_processes = run_bash_cmd("jps").split('\n')

    for process_i in java_processes:
        process_i = process_i.split(" ",1)
        pid = process_i[0]
        pname = process_i[1]
        # Check if the process name matches the specified name
        if process_name in pname:
            print(f"Killing process {pname} with PID {pid}")
        try:
            proc = psutil.Process(int(pid))
            proc.terminate() 
            proc.wait(timeout=3)
            break
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

# End of the file
