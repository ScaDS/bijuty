import sys

import ipywidgets as widgets
from IPython.display import display, clear_output
import requests
import os
import shutil
import re
import socket
import subprocess
import shlex

class GUIUtils:
    def __init__(self, info):
        self.fw_name = info['fw_name']
        self.slurm_info = info['slurm']
        self.user = os.environ.get('USER')
        self.label_style = {
            'description_width': '1000px',
            'font_weight': 'bold',
            'color': '#333333',
            'font_size': '14px',
        }

        self.framework_logos = {
            'SPARK': {
                'logo':'https://spark.apache.org/images/spark-logo-back.png'
                },
            'FLINK': {
                'logo':'https://flink.apache.org/img/logo/png/200/flink_squirrel_200_color.png'
                },
        }


    def get_image(self):
        def _get_img_bytes(url):
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                return response.content
            except Exception as e:
                # Fallback to an empty byte string if the URL fails
                print(f"Error loading logo: {e}")
                return b''

        # Get the URL from your mapping
        logo_url = self.framework_logos.get(self.fw_name, {}).get('logo', '')
        
        # Fetch the bytes directly
        img_content = _get_img_bytes(logo_url) if logo_url else b''

        # Create the Widget
        image_display = widgets.Image(
            value=img_content,
            format='svg+xml', # Ensure this matches your actual file type (svg vs png)
            width=100,
            height=100,
        )       
        return image_display


    def launch_gui_config(self):
        self.box_style = widgets.Layout(
            display='flex',
            flex_flow='column',
            align_items='flex-start',
            border='2px solid #444444',
            width='50vw',
            padding='10px',
            border_radius='20px',
            background_color="#ffffff"
        )
        
        # Title
        self.header = widgets.HTML("""
                    <div style='display: block; justify-content: center; align-items: center; gap: 10px; width: 47vw;'>
                      <h2 style='color: #2196F3; text-align: center;'>Framework Cluster Configurator</h2><hr>
                    </div>
                      """)
        
        
        self.image_display = self.get_image()


        # Template selection
        self.config_template = self.get_template_widget()

        self.config_destination = self.get_config_destination_widget()

        # Master host
        self.master_host = self.get_master_selection_widget()

        # Worker hosts
        self.worker_hosts = self.get_worker_selection_widget()

        # CPU per worker slider
        self.worker_cpu = self.get_worker_cpu_widget()
                
        # CPU per executor slider
        self.executor_cpu = self.get_executor_cpu_widget()
        
        # To set max executor CPU to worker CPU and update dynamically
        self.executor_cpu.max = self.worker_cpu.value
        def update_executor_cpu_range(change):
            new_worker_cpu_val = change['new']
            self.executor_cpu.max = new_worker_cpu_val
        self.worker_cpu.observe(update_executor_cpu_range, names='value')

        # Driver memory slider
        self.driver_memory = self.get_driver_memory_widget()
        
        # Worker memory slider
        self.worker_memory = self.get_worker_memory_widget()
        self.worker_memory.max = self.slurm_info.get_memory_per_node() - self.driver_memory.value
        def update_worker_memory_range(change):
            new_driver_memory_val = change['new']
            self.worker_memory.max = self.slurm_info.get_memory_per_node() - new_driver_memory_val
        self.driver_memory.observe(update_worker_memory_range, names='value')
        
        # Exector memory slider
        self.executor_memory = self.get_executor_memory_widget()
        # To set max executor memory to worker memory and update dynamically
        self.executor_memory.max = self.worker_memory.value
        def update_executor_memory_range(change):
            new_worker_memory_val = change['new']
            self.executor_memory.max = new_worker_memory_val
        self.worker_memory.observe(update_executor_memory_range, names='value')


        self.randomize_port = self.get_randomize_port_widget()
                
        self.load_button = widgets.Button(
            description='Load to Environment',
            button_style='info',
            layout=widgets.Layout(width='80%', margin='20px 0 0 0')
        )
        self.output_area = widgets.Output()
        self.load_button.on_click(self.set_environment)

        
        # Assemble and Display
        main_container = widgets.VBox([
            self.header, 
            self.image_display, 
            widgets.HTML("<br>"),
            self.config_template, self.config_destination,
            self.master_host,self.worker_hosts,
            self.worker_cpu, self.executor_cpu,
            self.driver_memory, self.worker_memory, self.executor_memory,
            widgets.HTML("<br>"),
            self.load_button, 
            self.output_area
        ], layout=self.box_style)

        display(main_container)

    def get_template_widget(self):
        use_default_template_chk = widgets.Checkbox(
            value=True,
            description='Use default template',
            indent=False
        )
        config_template = widgets.Text(
            value="default",
            description='Path to config template:',
            description_style=self.label_style
        )

        def _toggle_default(change):
            config_template.disabled = change['new']
        use_default_template_chk.observe(_toggle_default, names='value')

        return widgets.VBox([use_default_template_chk, config_template])
    
    def get_config_destination_widget(self):
        config_template = widgets.Text(
            value=os.getcwd(),
            description='Config destination path:',
            description_style=self.label_style
        )
        return config_template

    def get_master_selection_widget(self):
        return widgets.Dropdown(
            options=self.slurm_info.get_nodes_list(),
            value=self.slurm_info.get_nodes_list()[0],
            description='Master Host:',
            description_style=self.label_style
        )

    def get_worker_selection_widget(self):
        node_options = self.slurm_info.get_nodes_list()
        checkboxes = [
            widgets.Checkbox(value=False, description=node, indent=False) 
            for node in node_options
        ]

        checkbox_container = widgets.VBox(
            checkboxes, 
            layout=widgets.Layout(max_height='200px', overflow_y='auto', border='1px solid #ddd')
        )

        label = widgets.HTML(value="<b>Worker Hosts:</b>")
        selected_display = widgets.HTML(value="<i>None selected</i>")

        def update_selection(change):
            selected = [cb.description for cb in checkboxes if cb.value]
            selected_display.value = f"<b>Selected:</b> {', '.join(selected) if selected else '<i>None</i>'}"

        for cb in checkboxes:
            cb.observe(update_selection, names='value')

        return widgets.VBox([label, checkbox_container, selected_display])

    def get_worker_cpu_widget(self):
        return widgets.IntSlider(
            value=1, min=1, max=self.slurm_info.get_cpus_per_node(), 
            description='CPUs/worker:',
            description_style=self.label_style
        )

    def get_executor_cpu_widget(self):
        return widgets.IntSlider(
            value=1, min=1, max=int(self.worker_cpu.value),
            description='CPUs/executor:',
            description_style=self.label_style
        )
    
    def get_driver_memory_widget(self):
        return widgets.IntSlider(
            value=1, min=1, max=self.slurm_info.get_memory_per_node(),step=128,
            description='Driver Memory (MB):',
            description_style=self.label_style,
        )

    def get_worker_memory_widget(self):
        return widgets.IntSlider(
            value=1, min=1, max=self.slurm_info.get_memory_per_node(),step=128,
            description='Memory/worker (MB):',
            description_style=self.label_style,
        )

    def get_executor_memory_widget(self):
        return widgets.IntSlider(
            value=1, min=1, max=self.worker_memory.value,step=128,
            description='Memory/executor (MB):',
            description_style=self.label_style,
        )
    
    def get_randomize_port_widget(self):
        return widgets.Checkbox(
            value=False,
            description='Randomize Master Port',
            indent=False
        )
    
    def update_env_file(self):
        file_path = self.config_destination.value + "/spark/spark-env.sh"

        with open(file_path, 'r') as f:
            content = f.read()

        for var_name, new_value in self.env_updates.items():
            # Regex explanation:
            # ^[\s#\-]* -> Matches starting spaces, #, or - (handles comments)
            # (?:export\s+)? -> Matches "export " if it's there
            # \bVAR_NAME\b   -> Exact match for your variable name
            # .*$            -> Matches the rest of the line
            pattern = r"^[\s#\-]*(?:export\s+)?\b" + re.escape(var_name) + r"\b.*$"
            
            # The new line we want to inject
            replacement = f'export {var_name}="{new_value}"'
            
            # If the variable exists in some form, replace that line
            if re.search(pattern, content, flags=re.MULTILINE):
                content = re.sub(pattern, replacement, content, count=1, flags=re.MULTILINE)
            else:
                # If it doesn't exist at all, append it to the bottom
                if not content.endswith('\n'):
                    content += '\n'
                content += f'{replacement}\n'
            
            os.environ[str(var_name).strip()] = str(new_value).strip()


        # Write the changes back
        with open(file_path, 'w') as f:
            f.write(content)
    
    def find_first_available_port(self,start_port=8000, end_port=9000, host=socket.gethostname()):
        """
        Returns the first available port on the machine within the given range.
        """
        for port in range(start_port, end_port + 1):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind((host, port))
                    return port
                except OSError:
                    continue
        raise RuntimeError("No available ports found in the specified range.")
    
    def set_environment(self,b):
        self.load_button.disabled = True
        self.load_button.description = "Processing..."
        self.load_button.button_style = 'warning'
        print("DEBUG: Click registered!")
        with self.output_area:
            print(f"Setting environment for {self.fw_name}...")
            clear_output()

            # shutil.copytree(
            #     self.config_template.value,
            #     self.config_destination.value,
            #     dirs_exist_ok=True
            # )
            fw_name_safe = shlex.quote(self.fw_name.lower())
            template_safe = shlex.quote(str(self.config_template.children[1].value))
            dest_safe = shlex.quote(str(self.config_destination.value))
            
            bash_command =f"source framework-configure.sh --framework {fw_name_safe}"

            if not self.config_template.children[0].value:
                bash_command += f" --template {template_safe}"
            bash_command += f" --destination {dest_safe} && env | grep {fw_name_safe} || true"

            try:
                # Use subprocess.run to execute the command
                output = subprocess.run(
                    bash_command,
                    shell=True,
                    check=True,
                    text=True,
                    capture_output=True,
                    executable='/bin/bash'
                )
                
                # Set the environment variable in Python script's environment
                for line in output.stdout.splitlines():
                    if '=' in line:
                        key, value = line.strip().split('=',1)    
                        os.environ[str(key).strip()] = str(value).strip()
            except subprocess.CalledProcessError as e:
                print(f"❌ FATAL ERROR: {str(e)}")
                self.load_button.button_style = 'danger'
                self.load_button.description = "Failed"
                error_msg = f"Bash script failed with exit code {e.returncode}.\nError output: {e.stderr}"
                raise RuntimeError(error_msg)
            finally:
                self.load_button.disabled = False

            try:
                if not self.config_template.children[0].value:
                    self.config_template.children[1].value = os.environ.get(f"{self.fw_name.upper()}_CONF_TEMPLATE")

                if self.fw_name == "SPARK":
                    self.env_updates = {
                        'SPARK_MASTER_HOST': self.master_host.value,
                        'SPARK_WORKER_CORES': self.worker_cpu.value,
                        'SPARK_WORKER_MEMORY': f"{self.worker_memory.value}m",
                        'SPARK_EXECUTOR_CORES': self.executor_cpu.value,
                        'SPARK_EXECUTOR_MEMORY': f"{self.executor_memory.value}m",
                        'SPARK_LOCAL_DIRS': f"/tmp/{self.user}/cluster-conf-{self.slurm_info.job_id}/spark/local",
                        'SPARK_WORKER_DIR': f"/tmp/{self.user}/cluster-conf-{self.slurm_info.job_id}/spark/work",
                        'SPARK_CONF_DIR': self.config_destination.value,
                        'SPARK_LOG_DIR': f"{self.config_destination.value}/logs",
                        'SPARK_PID_DIR': f"{self.config_destination.value}/pid",
                        'SPARK_MASTER_PORT': '7077' if not self.randomize_port.value else str(self.find_first_available_port(start_port=7077)),
                        'PYSPARK_PYTHON': sys.executable,
                    }
                    self.update_env_file()
                print(f"✅ Environment Updated for {self.fw_name}!")
                self.load_button.button_style = 'success'
                self.load_button.description = "Success!"
                self.load_button.disabled = False
            except Exception as e:
                print(f"❌ FATAL ERROR: {str(e)}")
                self.load_button.button_style = 'danger'
                self.load_button.description = "Failed"
            finally:               
                self.load_button.disabled = False
    
            
            