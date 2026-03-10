import os
import json
import subprocess

class SlurmManager:
    def __init__(self):
        self.in_slurm_job = self.is_in_slurm_job()
        
        self.in_slurm_job = True
        if self.in_slurm_job:
            self.job_context = self.get_current_job_context()
            self.job_id = self.job_context["JOB_ID"]
            self.job_info = self.get_job_info()
        else:
            s = "No active Slurm job found."
            s += "\nThe SlurmManager must be initialized inside slurm job"
            raise Exception(s)
        
    def _run_command(self, cmd):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            return f"Error: {e.stderr.strip()}"

    def is_in_slurm_job(self):
        return "SLURM_JOB_ID" in os.environ


    def get_current_job_context(self):
        """
        Get variable information starting with SLURM_*
        """
        if not self.is_in_slurm_job():
            return None
        slurm_info = {k.replace("SLURM_",""): v for k, v in os.environ.items() if k.startswith("SLURM_")}
        # slurm_info = {**slurm_info, **self.get_job_info(slurm_info["JOB_ID"])}
        return slurm_info
    
    def get_job_info(self):
        cmd = ["squeue", "--json"] if self.job_id is None else ["scontrol", "show", "job", str(self.job_id), "--json"]
        output = self._run_command(cmd)
        return json.loads(output) if "{" in output else output

    def cancel_job(self, job_id):
        """Cancel a specific job ID."""
        return self._run_command(["scancel", str(job_id)])
    

    def __repr__(self):
        # Format the job_info dictionary into a pretty-printed string
        # If job_info is already a string (error message), it just uses that.
        if isinstance(self.job_info, dict):
            pretty_job_info = json.dumps(self.job_info, indent=4)
        else:
            pretty_job_info = str(self.job_info)

        return (
            f"=== SlurmManager (Job ID: {self.job_id}) ===\n"
            f"Status: {'Active' if self.in_slurm_job else 'Inactive'}\n"
            f"--- Environment Context ---\n"
            f"Nodes: {self.job_context.get('NNODES', 'N/A')}\n"
            f"Partition: {self.job_context.get('JOB_PARTITION', 'N/A')}\n"
            f"--- Full Job Info ---\n"
            f"{pretty_job_info}\n"
            f"=========================================="
        )
    
    def get_total_nodes(self):
        return int(self.job_info['jobs'][0]['node_count']['number'])

    def get_cpus_per_task(self):
        return int(self.job_info['jobs'][0]['cpus_per_task']['number'])
    
    def get_tasks_per_node(self):
        return int(self.job_info['jobs'][0]['tasks']['number'])
    
    def get_cpus_per_node(self):
        return int(self.get_cpus_per_task() * self.get_tasks_per_node())
    
    def get_memory_per_cpu(self):
        # In Mb
        return int(self.job_info['jobs'][0]['memory_per_cpu']['number'])
    
    def get_memory_per_node(self):
        # In Mb
        mem_per_node = self.job_info['jobs'][0]['memory_per_node']
        if mem_per_node['set']:
            return int(mem_per_node['number'])
        return int(self.get_memory_per_cpu() * self.get_cpus_per_task() * self.get_tasks_per_node())
    
    def get_total_cpus(self):
        return int(self.get_cpus_per_node() * self.get_tasks_per_node())
    
    def get_nodes_list(self):
        node_list = self.job_info['jobs'][0]['job_resources']['nodes']
        if type(node_list) == list:
            return node_list
        else:
            return [node_list]