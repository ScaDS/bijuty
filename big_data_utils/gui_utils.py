# big_data_utils/gui_utils.py
import time
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
import traceback
from .utils import run_bash_command
from .slurm_utils import SlurmManager
from .big_data_manager import BigDataManager

class GUIUtils:
    def __init__(self):

        # self.fw_name = fw_name.upper()
        # if self.fw_name not in [ 'FLINK', 'SPARK']:
        #     raise Exception("Sorry, frameworks other than 'flink' and 'spark' is not supported.")
        self.is_config_set = False
        
        self.slurm_info = SlurmManager()
        self.bdm = BigDataManager()
        
        debug_set_slurm_true = False
        if debug_set_slurm_true:
            self.slurm.in_slurm_job = debug_set_slurm_true        
        self.cluster_name = socket.getfqdn().strip()

        self.fw_mapping = {
            "SPARK": {
                "start_proc_cmd": "start-all.sh",
                "stop_proc_cmd": "stop-all.sh",
                "proc_name_master": "org.apache.spark.deploy.master.Master --host",
                "proc_name_worker": "org.apache.spark.deploy.worker.Worker --webui-port",
                "proc_name_other": [
                    "org.apache.spark.deploy.SparkSubmit",
                    "org.apache.spark.executor.CoarseGrainedExecutorBackend",
                    "org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend"
                ],
                "logo":"https://spark.apache.org/images/spark-logo-back.png",
                "worker_file": "workers",
                "default_master_port": 7077,
                "default":{
                    "mem_driver": 1000,
                    "mem_worker": 1000,
                    "mem_executor": 1000,
                    "cpu_driver": 1,
                    "cpu_worker": 1,
                    "cpu_executor": 1,
                }

            },
            "FLINK": {
                "start_proc_cmd": "start-cluster.sh",
                "stop_proc_cmd": "stop-cluster.sh",
                "proc_name_master": "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint",
                "proc_name_worker": "org.apache.flink.runtime.taskexecutor.TaskManagerRunner",
                "logo":"https://flink.apache.org/img/logo/png/200/flink_squirrel_200_color.png",
                "worker_file": "workers",
                "default_master_port": 8081,
            }
        }

        
        self.user = os.environ.get('USER')
        self.label_style = {
            'font_weight': 'bold',
            'color': '#333333',
            'font_size': '14px',
            'description_width': '150px',
        }
        self.widget_layout = widgets.Layout(
            width='100%',
            margin='5px 0px',
            display='flex',
            flex_flow='row',
        )
        self.wdg_viz_display = widgets.HTML()

    def get_viz_proportions(self):
        # Get values from your widgets
        # Defaulting to 1 if 0 to avoid division by zero
        drv_mem = float(self.get_from_selection_driver_memory_val())
        wrk_mem = float(self.get_from_selection_worker_memory_val())
        exe_mem = float(self.get_from_selection_executor_memory_val())
        
        # Define a 'Total Capacity' for the visualization (e.g., a 64GB Node)
        node_mem_capacity = self.slurm_info.get_memory_per_node()
        
        # Calculate height percentages
        # We cap them at 100% just in case
        master_mem_height = (drv_mem / node_mem_capacity) * 100 #max((drv_mem / node_capacity) * 100, 30)
        worker_mem_height = (wrk_mem / node_mem_capacity) * 100 #max((wrk_mem / node_capacity) * 100, 30)
        executor_mem_height = (exe_mem / wrk_mem) * 100 #max((wrk_mem / node_capacity) * 100, 30)

        drv_cpu = float(self.get_from_selection_driver_cpu())
        wrk_cpu = float(self.get_from_selection_worker_cpu())
        exe_cpu = float(self.get_from_selection_executor_cpu())
        
        node_cpu_capacity = self.slurm_info.get_cpus_per_node()
        master_cpu_height = max((drv_cpu / node_cpu_capacity) * 100,10)
        worker_cpu_height = max((wrk_cpu / node_cpu_capacity) * 100,10)
        executor_cpu_height = max((exe_cpu / wrk_cpu) * 100,10)
        
        return {
            'total_mem': "100%",
            'drv_mem_height': f"{master_mem_height}%",
            'wrk_mem_height': f"{worker_mem_height}%",
            'exe_mem_height': f"{executor_mem_height}%",
            'total_mem_val': f"{int(node_mem_capacity)}MB",
            'drv_mem_val': f"{int(drv_mem)}MB",
            'wrk_mem_val': f"{int(wrk_mem)}MB",
            'exe_mem_val': f"{int(exe_mem)}MB",
            'total_cpu_height': "100%",
            'drv_cpu_height': f"{master_cpu_height}%",
            'wrk_cpu_height': f"{worker_cpu_height}%",
            'exe_cpu_height': f"{executor_cpu_height}%",
            'total_cpu_val': f"{int(node_cpu_capacity)}",
            'drv_cpu_val': f"{int(drv_cpu)}",
            'wrk_cpu_val': f"{int(wrk_cpu)}",
            'exe_cpu_val': f"{int(exe_cpu)}"
        }
    
    def update_process_viz(self, change=None):
        props = self.get_viz_proportions()
        
        def render_cpu_process(props: dict) -> str:
            def process_style(height, col_background, col_text):
                return f"""
                    height: calc({height}); 
                    background: {col_background}; 
                    color: {col_text};
                    transition: height 0.4s ease;
                    position: relative;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    width:calc(100% - 10px);
                """
            
            def sub_process_style(height, col_background, col_text):
                return f"""
                    background: {col_background};
                    color: {col_text};
                    padding: 0px 0px 0px 0px;
                    border-radius: 0px;
                    font-size: 11px;
                    width:100%;
                    max-width:100%;
                    height:{height};
                """
            """
            Returns the HTML snippet for the CPU process block.

            keys:
            outer_title
            outer_height
            outer_style_bkg_clr
            outer_style_text_clr
            inner_title
            inner_height
            inner_style_bkg_clr
            inner_style_text_clr

            """
            outer_title = props.get("outer_title", "Outer Title:Val").split(":")[0]
            outer_bg_clr   = props.get("outer_style_bkg_clr", "#9ac3f4")
            outer_txt_clr  = props.get("outer_style_text_clr", "#1565c0")
            outer_height = props.get("outer_height", "100%")
            inner_title = props.get("inner_title", "Inner Title")
            inner_bg_clr   = props.get("inner_style_bkg_clr", "#1565c0")
            inner_txt_clr  = props.get("inner_style_text_clr", "#ffffff")
            inner_txt = props.get("inner_text", "")
            inner_height = props.get("inner_height", "100%")

            outer_style   = process_style(
                height=outer_height,
                col_background=outer_bg_clr,
                col_text=outer_txt_clr
            )
            inner_style  = sub_process_style(
                height=inner_height,
                col_background=inner_bg_clr,
                col_text=inner_txt_clr
            )

            return f"""
                <div style="{outer_style}">
                    <div style="position:absolute; left:0; top:0; bottom:0; width:20px;
                                display:flex; align-items:center; justify-content:center;
                                writing-mode:sideways-lr; text-orientation:mixed;
                                background:{outer_bg_clr}; 
                                color:black;
                                font-weight:bold;">
                        {outer_title}
                    </div>
                    <div>{props.get("outer_title", "Outer Title").split(":")[1]}</div>
                    <div style="{inner_style}; display: flex; align-items: center; justify-content:center;">
                        <div style="position:absolute; left:20px; width:20px;
                                    display:flex; align-items:center; justify-content:center;
                                    writing-mode:sideways-lr; text-orientation:mixed;
                                    background:{inner_bg_clr}; color:{inner_txt_clr}; font-weight:bold;">
                            {inner_title}
                        </div>
                        <div style="padding:0 45px;">{inner_txt}</div>
                    </div>
                </div>
            """.strip()

        def render_slurm_info(slurm_info):
            return f"""
                <div style="display:flex; text-align: center; flex-direction: column;">
                    <div style="text-align: center; font-weight: bold; margin-bottom: 10px; width:100%">
                        Slurm Job
                    </div>
                    
                    <div style="display: grid; grid-template-columns: 33% 33% 33%; align-items: center; justify-content:center">
                        <div style="color: #666; font-size: 12px; padding:0px">
                            NODES
                            <div style="font-family: monospace; font-weight: bold; color: #2980b9;align-items:center;justify-content:center;">
                                {",".join(slurm_info.get_nodes_list())}
                            </div>
                        </div>
                        <div style="color: #666; font-size: 12px; align-items:center;justify-content:center;">
                            CPU CORES
                            <div style="font-weight: bold;">{slurm_info.get_total_cpus()} Cores</div>
                        </div>
                        <div style="color: #666; font-size: 12px; text-transform: uppercase;align-items:center;justify-content:center;">
                            MEMORY
                            <div style="font-weight: bold;">{slurm_info.get_total_memory()} MB</div>
                        </div>
                    </div>
                </div>
            """

        html_template = f"""
        <div style="
            border: 3px solid #444; 
            border-radius: 10px; 
            width: 95%; 
            height: 700px;
            overflow-y: scroll;
            background: #f0f0f0; 
            display: flex; 
            flex-direction: column; 
            padding: 10px;
            font-family: sans-serif;
        ">
            {render_slurm_info(self.slurm_info)}    
            
            <div style="
                display: flex;
                flex-direction: row; 
                justify-content: space-between; 
                width: 100%; 
                gap: 0px;
                margin-top: 0px;
                height:100%;
                font-size: 10px;
                border: 2px solid #444; 
                border-radius: 10px; 
            ">
                <div style="width: 50%; display: flex; flex-direction: column; gap: 0px; height:100%;align-items:center;">
                    <b>CPU</b>
                    {render_cpu_process({
                        "outer_height": props['drv_cpu_height'],
                        "outer_style_bkg_clr": "#9ac3f4",
                        "outer_style_text_clr": "#1565c0",
                        "outer_title": f"Master: {props['drv_cpu_val']}",
                        "inner_height": "100%",
                        "inner_style_bkg_clr": "#1565c0",
                        "inner_style_text_clr": "#ffffff",
                        "inner_title": "Driver",
                        "inner_text": "CPU: " + props['drv_cpu_val'],
                    })}


                    {render_cpu_process({
                        "outer_height": props['wrk_cpu_height'],
                        "outer_style_bkg_clr": "#8ff898",
                        "outer_style_text_clr": "#4caf50",
                        "outer_title": f"Worker:{props['wrk_cpu_val']}",
                        "inner_height": props['exe_cpu_height'],
                        "inner_style_bkg_clr": "#4caf50",
                        "inner_style_text_clr": "#ffffff",
                        "inner_title": "Driver",
                        "inner_text": "CPU: " + props['exe_cpu_val'],
                    })}
                </div>
                
                <div style="width: 50%; display: flex; flex-direction: column; gap: 0px; height:100%; align-items:center;">
                    <b>Memory</b>
                    {render_cpu_process({
                        "outer_height": props['drv_mem_height'],
                        "outer_style_bkg_clr": "#9ac3f4",
                        "outer_style_text_clr": "#1565c0",
                        "outer_title": f"Master: {props['drv_mem_val']}",
                        "inner_height": "100%",
                        "inner_style_bkg_clr": "#1565c0",
                        "inner_style_text_clr": "#ffffff",
                        "inner_title": "Driver",
                        "inner_text": "Memory: " + props['drv_mem_val'],
                    })}


                    {render_cpu_process({
                        "outer_height": props['wrk_mem_height'],
                        "outer_style_bkg_clr": "#8ff898",
                        "outer_style_text_clr": "#4caf50",
                        "outer_title": f"Worker: {props['wrk_mem_val']}",
                        "inner_height": props['exe_mem_height'],
                        "inner_style_bkg_clr": "#4caf50",
                        "inner_style_text_clr": "#ffffff",
                        "inner_title": "Driver",
                        "inner_text": "Memory: " + props['exe_mem_val'],
                    })}
                </div>

                
            </div>
        </div>
        """
        self.wdg_viz_display.value = html_template


    def get_framework_logo(self):
        def _get_img_bytes(url):
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                return response.content
            except Exception as e:
                print(f"Error loading logo: {e}")
                return b''
        if self.get_from_selection_framework_name() is not None:
            logo_url = self.fw_mapping.get(self.get_from_selection_framework_name(), {}).get('logo', '')
            img_content = _get_img_bytes(logo_url) if logo_url else b''
            image_display = widgets.Image(
                value=img_content,
                format='svg+xml',
                width=100,
                height=100,
            )
            return image_display
        else:
            return widgets.HTML(value="<div style='width:100px;height:100px;background-color:#eee;display:flex;align-items:center;justify-content:center;color:#999;'>No Logo</div>")

    def launch_gui_config(self):
        
        # Create GUI components
        self.wdg_header_config = self._create_header(title="Cluster Configurator")

        self.wdg_framework_name = self.get_wdg_framework_name()
        self.wdg_image_display = self.get_framework_logo()
        self.wdg_config_template = self.get_wdg_template()
        self.wdg_config_destination = self.get_wdg_config_destination()
        self.wdg_master_host = self.get_wdg_master()

        self.wdg_driver_cpu = self.get_wdg_driver_cpu()

        self.wdg_worker_hosts = self.get_wdg_worker()
        self.wdg_worker_cpu = self.get_wdg_worker_cpu()
        
        self.wdg_executor_cpu = self.get_wdg_executor_cpu()
        self.wdg_driver_memory = self.get_wdg_driver_memory()       
        self.wdg_worker_memory = self.get_wdg_worker_memory()
        self.wdg_executor_memory = self.get_wdg_executor_memory()
        self.wdg_randomize_port = self.get_wdg_randomize_port()
        self.wdg_load_button = self.get_wdg_load_button()
        self.wdg_output_area = widgets.Output()

        self.wdg_btn_start_cluster = self.get_wdg_btn_start_cluster()
        self.wdg_btn_stop_cluster = self.get_wdg_btn_stop_cluster()

        

        self._attach_wdg_observers()


        self.wdg_header_viz = self._create_header(title="Resource Allocation Overview")
        trigger_widgets = [
            self.wdg_driver_cpu,
            self.wdg_worker_cpu,
            self.wdg_executor_cpu,
            self.wdg_driver_memory,
            self.wdg_worker_memory,
            self.wdg_executor_memory,
        ]

        for w in trigger_widgets:
            w.observe(self.update_process_viz, names='value')

        # Run once initially to show the starting state
        self.update_process_viz()

        # Assemble and display GUI
        self.outer_layout = widgets.Layout(
            display='flex',
            flex_flow='row',
            width='100%',
            max_width='100%',
            border='2px solid #444444',
            border_radius='100px',
            background_color="#ffffff",
            overflow='hidden',
            height="1000px"
        )

        self.config_container = widgets.VBox(
            [
                self.wdg_header_config,
                self.wdg_image_display,
                self.wdg_framework_name,
                self.wdg_config_template,
                self.wdg_config_destination,
                self.wdg_master_host,
                self.wdg_worker_hosts,
                self.wdg_driver_cpu,
                self.wdg_worker_cpu,
                self.wdg_executor_cpu,
                self.wdg_driver_memory,
                self.wdg_worker_memory,
                self.wdg_executor_memory,
                self.wdg_randomize_port,
                self.wdg_load_button,
                self.wdg_output_area
            ], 
            layout = widgets.Layout(
                width='50%', 
                padding='20px',
                display='flex',
                flex_flow='column',
                margin='10px auto',
                align_items='stretch',
                align_content='stretch',
            )
        )

        self.viz_container = widgets.VBox([  
                self.wdg_header_viz,
                self.wdg_viz_display,
            ], 
            layout=widgets.Layout(
                width='50%', 
                padding='20px',
                display='flex',
                flex_flow='column',
                margin='10px 0px',
                align_items='stretch',
            )
        )
                
        row1 = widgets.HBox(
            [
                self.config_container,
                self.viz_container,
            ], 
            layout= widgets.Layout(
                display='flex',
                flex_flow='row',
                width='100%',
                max_width='100%',
                overflow='hidden',
                height="850px"
            )
        )

        row2 = widgets.VBox(
            [
                self.wdg_btn_start_cluster,
                self.wdg_btn_stop_cluster,
            ],
            layout = widgets.Layout(
                display='flex',
                flex_flow='row',
                width='100%',
                max_width='100%',
                justify_content='space-around'
            )
        )
        
        main_container = widgets.VBox([
                row1,
                row2
            ],
            layout = widgets.Layout(
                    display='flex',
                    flex_flow='column',
                    width='100%',
                    max_width='100%',
                    border='2px solid #444444',
                    border_radius='100px',
                    background_color="#ffffff"
            )
        )
        
        display(main_container)
        

    def _create_header(self, title: str):
        """Create the GUI header"""
        return widgets.HTML(f"""
            <div style='display: block; justify-content: center; align-items: center; gap: 10px; width: 100%;'>
              <h2 style='color: #2196F3; text-align: center;'>{title}</h2><hr>
            </div>
        """)
    
    def make_styled_button(
            self,
            description,
            style_overrides = None,
            layout_overrides = None,
            **button_kwargs,
        ):
        # Base style
        base_style = {
            "button_color": "#4caf50",      # green background
            "font_weight": "bold",
            "font_size": "14px",
        }
        # Base layout
        base_layout = {
            "width": "120px",
            "height": "40px",
            "margin": "5px",
            "align_self": "center",
        }

        # Merge overrides (if any)
        final_style = {**base_style, **(style_overrides or {})}
        final_layout = {**base_layout, **(layout_overrides or {})}

        return widgets.Button(
            description=description,
            style=widgets.ButtonStyle(**final_style),
            layout=widgets.Layout(**final_layout),
            **button_kwargs,
        )

      
    # GUI component creation methods
    def get_wdg_framework_name(self):
        """Get the framework selection widget"""
        return widgets.Dropdown(
            options=list(self.fw_mapping.keys()),
            # value="Select a framework",
            description='Framework:',
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_template(self):
        """Get the template selection widget"""
        use_default_template_chk = widgets.Checkbox(
            value=True,
            description='Use default template',
            indent=False
        )
        config_template = widgets.Text(
            value="default",
            description='Path to config template:',
            style=self.label_style,
            layout = self.widget_layout,
        )
        config_template.disabled = use_default_template_chk.value

        def _toggle_default(change):
            config_template.disabled = change['new']
        use_default_template_chk.observe(_toggle_default, names='value')

        return widgets.VBox([use_default_template_chk, config_template])

    def get_wdg_config_destination(self):
        """Get the config destination path widget"""
        return widgets.Text(
            value=os.getcwd(),
            description='Config destination path:',
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_master(self):
        """Get the master host selection widget"""
        return widgets.Dropdown(
            options=self.slurm_info.get_nodes_list(),
            value=self.slurm_info.get_nodes_list()[0],
            description='Master Host:',
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_worker(self):
        """Get the worker host selection widget"""
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

    def get_wdg_driver_cpu(self):
        return widgets.IntSlider(
            value=1, min=1, max=self.slurm_info.get_cpus_per_node() - self.fw_mapping[self.get_from_selection_framework_name()]['default']['cpu_worker'],
            description='CPUs for Driver:',
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_worker_cpu(self):
        """Get the worker CPU slider widget"""
        return widgets.IntSlider(
            value=1, min=1, max=self.slurm_info.get_cpus_per_node() - self.get_from_selection_driver_cpu(),
            description='CPUs/worker:',
                   style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_executor_cpu(self):
        """Get the executor CPU slider widget"""
        return widgets.IntSlider(
            value=1, min=1, max=int(self.get_from_selection_worker_cpu()), step=1,
            description='CPUs/executor:',
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_driver_memory(self):
        """Get the driver memory slider widget"""
        value = self.fw_mapping[self.get_from_selection_framework_name()]['default']['mem_driver']
        return widgets.IntSlider(
            value=value, min=value, max=self.slurm_info.get_memory_per_node() - self.fw_mapping[self.get_from_selection_framework_name()]['default']['mem_worker'],
            step=128,
            description='Driver Memory (MB):',
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_worker_memory(self):
        """Get the worker memory slider widget"""
        value = self.fw_mapping[self.get_from_selection_framework_name()]['default']['mem_driver']
        return widgets.IntSlider(
            value=value, min=value, max=self.slurm_info.get_memory_per_node() - self.get_from_selection_driver_memory_val(), step=128,
            description='Memory/worker (MB):',
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_executor_memory(self):
        """Get the executor memory slider widget"""
        value = self.fw_mapping[self.get_from_selection_framework_name()]['default']['mem_driver']
        return widgets.IntSlider(
            value=value, min=value, max=self.get_from_selection_worker_memory_val(), step=128,
            description='Memory/executor (MB):',
                        style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_randomize_port(self):
        """Get the randomize master port checkbox widget"""
        return widgets.Checkbox(
            value=False,
            description='Randomize Master Port',
            indent=False,
            style=self.label_style,
            layout = self.widget_layout
        )

    def get_wdg_load_button(self):
        """Get the load button widget"""
        load_button = widgets.Button(
            description='Load to Environment',
            button_style='info',
            layout=widgets.Layout(width='80%', margin='20px auto 0 auto',alignment='center')
        )
        load_button.on_click(self.set_environment)
        return load_button
    
    def _toggle_btn_cluster(self, start_disabled: bool|None = None, stop_disabled: bool|None = None, all_disabled:bool|None = None):
        if all_disabled is not None:
            self.wdg_btn_start_cluster.disabled = all_disabled
            self.wdg_btn_stop_cluster.disabled = all_disabled
            return

        if start_disabled is None and stop_disabled is None:
            new_start = not self.wdg_btn_start_cluster.disabled
            new_stop = not self.wdg_btn_stop_cluster.disabled
            self.wdg_btn_start_cluster.disabled = new_start
            self.wdg_btn_stop_cluster.disabled = new_stop
            return
        # Override, if desired
        if start_disabled is not None:
            self.wdg_btn_start_cluster.disabled = start_disabled
        if stop_disabled is not None:
            self.wdg_btn_stop_cluster.disabled = stop_disabled

    def _on_start_cluster_clicked(self,_):
        self._toggle_btn_cluster(all_disabled=True)
        try:
            result_startup = self.bdm.start_cluster()
            self._last_cluster_result = result_startup
            self._toggle_btn_cluster(start_disabled=True,stop_disabled=False)
        except Exception as e:
            print("Failed to start cluster:", e)
            self._toggle_btn_cluster(start_disabled=False,stop_disabled=True)

    def _on_stop_cluster_clicked(self,_):
        self._toggle_btn_cluster(all_disabled=True)
        try:
            result_stop = self.bdm.stop_cluster()           
            self._last_cluster_result = result_stop
            self._toggle_btn_cluster(start_disabled=False,stop_disabled=True)
        except Exception as e:
            print("Failed to stop cluster:", e)
            self._toggle_btn_cluster(start_disabled=True,stop_disabled=False)
        
    def get_wdg_btn_start_cluster(self):
        button = self.make_styled_button(
            description="Start Cluster",
            layout_overrides={'width':"40%",'color':'white'}
            )
        button.disabled = not self.is_config_set # Enable only when all the parameters are set
        button.on_click(self._on_start_cluster_clicked)
        return button
    
    def get_wdg_btn_stop_cluster(self):
        button = self.make_styled_button(
            description="Stop Cluster",
            style_overrides={'button_color':'red','color':'white'},
            layout_overrides={'width':"40%"}
            )
        button.disabled = not self.is_config_set # Enable only when all the parameters are set
        button.on_click(self._on_stop_cluster_clicked)
        return button

    # Widget observers
    def _on_change_parameters_load_button(self,change):
        self.wdg_load_button.button_style = "warning"
        self.wdg_load_button.description = "⟳ Apply Changes"
        self.wdg_output_area.clear_output()
        self.is_config_set = False # Need to click on load if any changes happened
        
    def _attach_wdg_observers(self):
        observable_widgets = [
            self.wdg_config_template, self.wdg_config_destination,
            self.wdg_master_host, self.wdg_worker_hosts,
            self.wdg_driver_cpu, self.wdg_worker_cpu, self.wdg_executor_cpu,
            self.wdg_driver_memory, self.wdg_worker_memory, self.wdg_executor_memory,
            self.wdg_randomize_port,
        ]
        for widget in observable_widgets:
            widget.observe(self._on_change_parameters_load_button, names="value")

        self.wdg_worker_cpu.max = self.slurm_info.get_cpus_per_node() - self.get_from_selection_driver_cpu()
        self.wdg_driver_cpu.observe(self.update_wdg_worker_cpu_range, names='value')

        self.wdg_executor_cpu.max = self.wdg_worker_cpu.value
        self.wdg_worker_cpu.observe(self.update_wdg_executor_cpu_range, names='value')

        self.wdg_worker_memory.max = self.slurm_info.get_memory_per_node() - self.get_from_selection_driver_memory_val()
        self.wdg_driver_memory.observe(self.update_wdg_worker_memory_max, names='value')
        self.wdg_worker_memory.observe(self.update_wdg_executor_memory_max, names='value')

    def update_wdg_worker_cpu_range(self, change):
        new_driver_cpu_val = change['new']
        self.wdg_worker_cpu.max = self.slurm_info.get_cpus_per_node() - new_driver_cpu_val

    def update_wdg_executor_cpu_range(self, change):
        new_worker_cpu_val = change['new']
        self.wdg_executor_cpu.max = new_worker_cpu_val

    def update_wdg_worker_memory_max(self, change):
        new_driver_memory_val = change['new']
        self.wdg_worker_memory.max = self.slurm_info.get_memory_per_node() - new_driver_memory_val
    
    def update_wdg_executor_memory_max(self, change):
        new_worker_memory_val = change['new']
        self.wdg_executor_memory.max = new_worker_memory_val
    
    # Value extraction methods
    def get_from_selection_framework_name(self):
        """Get the selected framework name"""
        return self.wdg_framework_name.value
    
    def get_from_selection_workers(self):
        """Get the selected worker hosts"""
        return [cb.description for cb in self.wdg_worker_hosts.children[1].children if cb.value]

    def get_from_selection_master_port(self):
        """Get the selected master port"""
        if self.wdg_randomize_port.value:
            port = str(self.find_first_available_port(start_port=7077))
        else:
            port = "7077"
        return port

    def get_from_selection_master_host(self):
        """Get the selected master host"""
        return self.wdg_master_host.value

    def get_from_selection_driver_cpu(self):
        """Get the selected driver CPU"""
        return self.wdg_driver_cpu.value

    def get_from_selection_worker_cpu(self):
        """Get the selected worker CPU"""
        return self.wdg_worker_cpu.value

    def get_from_selection_worker_memory_val(self):
        """Get the selected worker memory in integer format for calculations"""
        return int(self.wdg_worker_memory.value)

    def get_from_selection_worker_memory(self):
        """Get the selected worker memory"""
        return f"{self.get_from_selection_worker_memory_val()}m"

    def get_from_selection_executor_cpu(self):
        """Get the selected executor CPU"""
        return self.wdg_executor_cpu.value

    def get_from_selection_executor_memory_val(self):
        """Get the selected executor memory in integer format for calculations"""
        return int(self.wdg_executor_memory.value)

    def get_from_selection_executor_memory(self):
        """Get the selected executor memory"""
        return f"{self.get_from_selection_executor_memory_val()}m"

    def get_from_selection_driver_memory_val(self):
        """Get the selected driver memory in integer format for calculations"""
        return int(self.wdg_driver_memory.value)

    def get_from_selection_driver_memory(self):
        """Get the selected driver memory"""
        return f"{self.get_from_selection_driver_memory_val()}m"

    def get_from_selection_config_destination(self):
        """Get the selected config destination path"""
        config_destination = os.path.join(self.wdg_config_destination.value, self.get_from_selection_framework_name().lower())
        #print(f"DEBUG: Config destination path set to {config_destination}")
        return config_destination

    def get_from_selection_config_template(self):
        """Get the selected config template path"""
        default_true = self.wdg_config_template.children[0].value
        if default_true:
            return os.environ.get(f"{self.get_from_selection_framework_name().upper()}_CONF_TEMPLATE")
        else:
            return self.wdg_config_template.children[1].value
        

    def get_from_selection_local_dirs(self):
        """Get the selected local directories"""
        return f"/tmp/{self.user}/cluster-conf-{self.slurm_info.job_id}/spark/local"

    def get_from_selection_worker_dir(self):
        """Get the selected worker directory"""
        return f"/tmp/{self.user}/cluster-conf-{self.slurm_info.job_id}/spark/work"

    def get_from_selection_log_dir(self):
        """Get the selected log directory"""
        return f"{self.get_from_selection_config_destination()}/log"

    def get_from_selection_pid_dir(self):
        """Get the selected PID directory"""
        return f"{self.get_from_selection_config_destination()}/pid"

    # Environment update methods
    def update_env_file(self):
        """Update the environment file"""
        if self.get_from_selection_framework_name() == "SPARK":
            file_path = os.path.join(self.get_from_selection_config_destination(), "spark-env.sh")

            with open(file_path, 'r') as f:
                content = f.read()

            for var_name, new_value in self.env_updates.items():
                escaped_var = re.escape(var_name)
                replacement = f'export {var_name}="{new_value}"'

                active_pattern = rf"^\s*export\s+\b{escaped_var}\b.*$"
                comment_pattern = rf"^[\s#\-]+(?:export\s+)?\b{escaped_var}\b.*$"

                if re.search(active_pattern, content, flags=re.MULTILINE):
                    content = re.sub(active_pattern, replacement, content, flags=re.MULTILINE)
                elif re.search(comment_pattern, content, flags=re.MULTILINE):
                    content = re.sub(comment_pattern, replacement, content, count=1, flags=re.MULTILINE)
                else:
                    if content and not content.endswith('\n'):
                        content += '\n'
                    content += f'{replacement}\n'

                os.environ[str(var_name).strip()] = str(new_value).strip()

            with open(file_path, 'w') as f:
                f.write(content)

    def update_worker_file(self):
        """Update the worker file"""
        worker_file_path = os.path.join(self.get_from_selection_config_destination(), self.fw_mapping[self.get_from_selection_framework_name()]["worker_file"])
        with open(worker_file_path, 'w') as f:
            for node in self.get_from_selection_workers():
                f.write(f"{node}\n")

    def find_first_available_port(self, start_port=8000, end_port=9000, host=socket.gethostname()):
        """Find the first available port"""
        for port in range(start_port, end_port + 1):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind((host, port))
                    return port
                except OSError:
                    continue

        raise RuntimeError("No available ports found in the specified range.")

    def set_environment(self, b):
        """Set the environment"""

        print("DEBUG: Load button clicked!")
        
        self.wdg_load_button.disabled = True
        self.wdg_load_button.description = "Processing..."
        self.wdg_load_button.button_style = 'warning'
        
        with self.wdg_output_area:
            clear_output()
            print(f"Setting environment for {self.get_from_selection_framework_name()}...")

            fw_name_safe = shlex.quote(self.get_from_selection_framework_name().lower())
            template_safe = shlex.quote(self.get_from_selection_config_template())
            dest_safe = shlex.quote(str(self.get_from_selection_config_destination().replace(self.get_from_selection_framework_name().lower(), "")))
            
            bash_command = [
                "source framework-configure.sh",
                "--framework", f"{fw_name_safe}",
                "--template", f"{template_safe}",
                "--destination", f"{dest_safe}",
                f"&& env | grep {fw_name_safe} || true"
            ]
            bash_command = " ".join(bash_command)

            try:
                # output = subprocess.run(
                #     bash_command,
                #     shell=True,
                #     check=True,
                #     text=True,
                #     capture_output=True,
                #     executable='/bin/bash'
                # )
                
                start_time = time.time()    
                output, error, return_code = run_bash_command(bash_command,shell=True, timeout=6000)  # Log the command being run    
                end_time = time.time()
                print(f"DEBUG:Time elapsed for config init: {end_time - start_time} seconds")

                if return_code != 0:
                    print(f" FATAL ERROR: {error}")
                    self.wdg_load_button.button_style = 'danger'
                    self.wdg_load_button.description = "Failed"
                    self.wdg_load_button.disabled = False
                    raise RuntimeError(f"Bash script failed with exit code {return_code}.\nError output: {error}")
                
                for line in output.splitlines():
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        os.environ[str(key).strip()] = str(value).strip()

            # except subprocess.CalledProcessError as e:
            #     print(f" FATAL ERROR: {str(e)}")
            #     self.wdg_load_button.button_style = 'danger'
            #     self.wdg_load_button.description = "Failed"
            #     error_msg = f"Bash script failed with exit code {e.returncode}.\nError output: {e.stderr}"
            #     raise RuntimeError(error_msg)
            finally:
                self.wdg_load_button.disabled = False

            try:
                # if not self.wdg_config_template.children[0].value:
                #     self.wdg_config_template.children[1].value = os.environ.get(f"{self.fw_name.upper()}_CONF_TEMPLATE")

                if self.get_from_selection_framework_name() == "SPARK":
                    self.env_updates = {
                        'SPARK_MASTER_HOST': self.get_from_selection_master_host(),
                        'SPARK_WORKER_CORES': self.get_from_selection_worker_cpu(),
                        'SPARK_WORKER_MEMORY': self.get_from_selection_worker_memory(),
                        'SPARK_EXECUTOR_CORES': self.get_from_selection_executor_cpu(),
                        'SPARK_EXECUTOR_MEMORY': self.get_from_selection_executor_memory(),
                        'SPARK_DRIVER_MEMORY': self.get_from_selection_driver_memory(),
                        'SPARK_LOCAL_DIRS': self.get_from_selection_local_dirs(),
                        'SPARK_WORKER_DIR': self.get_from_selection_worker_dir(),
                        'SPARK_CONF_DIR': self.get_from_selection_config_destination(),
                        'SPARK_LOG_DIR': self.get_from_selection_log_dir(),
                        'SPARK_PID_DIR': self.get_from_selection_pid_dir(),
                        'SPARK_MASTER_PORT': self.get_from_selection_master_port(),
                        'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', sys.executable)
                    }
                    self.update_env_file()
                    self.update_worker_file()
                    print(f" Environment Updated for {self.get_from_selection_framework_name()}!")
                    
                    self.wdg_load_button.button_style = 'success'
                    self.wdg_load_button.description = "Success!"
                    self.wdg_load_button.disabled = False

                    self.is_config_set = True
                    self._toggle_btn_cluster(start_disabled=False)
            except Exception as e:
                print(f" FATAL ERROR: {str(e)}")
                tb = traceback.format_exc()
                print(tb)
                self.wdg_load_button.button_style = 'danger'
                self.wdg_load_button.description = "Failed"

                self.is_config_set = False
                self._toggle_btn_cluster(all_disabled=False)
            finally:
                self.wdg_load_button.disabled = False
                self.bdm.initialize_user_input({
                    'fw_name': self.get_from_selection_framework_name(),
                    'master': self.get_from_selection_master_host(),
                    'workers': self.get_from_selection_workers(),
                    'master_port': self.get_from_selection_master_port(),
                    'conf_dir': self.get_from_selection_config_destination(),
                    'log_dir': self.get_from_selection_log_dir(),
                    'fw_mapping':self.fw_mapping
                })
                