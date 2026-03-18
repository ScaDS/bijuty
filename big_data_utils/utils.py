import datetime
import subprocess
import psutil
import os

class SimpleLogger:
    def log(self, message, log_type):
        timestamp = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        print(f"[{log_type}] [{timestamp}] - {message}")

    def info(self, message):
        self.log(message, "INFO ")

    def debug(self, message):
        self.log(message, "DEBUG")

    def error(self, message):
        self.log(message, "ERROR")


mylogger = SimpleLogger()

def load_env_file(filepath):
    with open(filepath) as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

# def kill_java_processes_by_name(process_name):
    
#     java_processes = run_bash_cmd("jps").split('\n')

#     for process_i in java_processes:
#         process_i = process_i.split(" ",1)
#         pid = process_i[0]
#         pname = process_i[1]
#         # Check if the process name matches the specified name
#         if process_name in pname:
#             print(f"Killing process {pname} with PID {pid}")
#         try:
#             proc = psutil.Process(int(pid))
#             proc.terminate() 
#             proc.wait(timeout=3)
#             break
#         except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
#             pass


def run_bash_command(cmd: str, timeout=60, shell=False):
    """
    Runs a command safely and returns (stdout, stderr, returncode).
    """
    current_env = os.environ.copy()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
            env=current_env,
            shell=shell,
            executable='/bin/bash'
        )
        return result.stdout.strip(), "", 0

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.strip() or e.stdout.strip()
        mylogger.error(f"Command failed: {cmd}\nError: {error_output}")
        return "", error_output, e.returncode

    except subprocess.TimeoutExpired as e:
        mylogger.error(f"Command timed out after {timeout}s: {cmd}")
        return "", "Timeout expired", 124

    except FileNotFoundError:
        mylogger.error(f"Executable not found: {cmd[0]}")
        return "", "Executable not found", 127

# End of the file
