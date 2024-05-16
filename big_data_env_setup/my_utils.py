import datetime
import subprocess

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

# End of the file