import psutil
import threading
import time
import json
from datetime import datetime
from collections import deque
import getpass

import ipywidgets as widgets
from IPython.display import clear_output, display, HTML
import plotly.graph_objects as go
from plotly.subplots import make_subplots


class ProcessMetricCollector:
    HISTORY = 40  # maximum data points to store
    
    def __init__(self, process_names: list[str]):
        self.process_names = process_names
        self.user = getpass.getuser()

        # history[name] = {'cpu': deque, 'mem': deque}
        self.history = {}
        for process_i in process_names:
            self.history[process_i] = {
                "cpu": deque([0.0] * self.HISTORY, maxlen=self.HISTORY),
                "mem_pct":  deque([0.0] * self.HISTORY, maxlen=self.HISTORY),
                "mem_rss":  deque([0.0] * self.HISTORY, maxlen=self.HISTORY),
                "mem_vms":  deque([0.0] * self.HISTORY, maxlen=self.HISTORY),
                "threads":  deque([0]   * self.HISTORY, maxlen=self.HISTORY),
                "io_read":  deque([0.0] * self.HISTORY, maxlen=self.HISTORY),
                "io_write": deque([0.0] * self.HISTORY, maxlen=self.HISTORY),
                "timestamp": deque([0.0] * self.HISTORY, maxlen=self.HISTORY),
            }

    def _match(self, proc):
        if proc.username() != self.user:
            return None
        try:
            cmdline = " ".join(proc.cmdline())
            for name in self.process_names:
                if name in cmdline:
                    return name
        except Exception as e:
            print(f"Could not access process {proc.pid} info to match name.", e)
            return None
        
        return None
    
    def collect(self):
        results = {}
        for proc in psutil.process_iter():
            name = self._match(proc)
            if name is None:
                continue
            try:
                with proc.oneshot(): # Extracts the metrics in one go for better performance
                    cpu     = proc.cpu_percent(interval=None)
                    mem     = proc.memory_info()
                    mem_pct = proc.memory_percent()
                    threads = proc.num_threads()
                    io_counters = proc.io_counters()
                    io_read  = io_counters.read_bytes
                    io_write = io_counters.write_bytes

                h = self.history[name]
                h["cpu"].append(cpu)
                h["mem_pct"].append(mem_pct)
                h["mem_rss"].append(mem.rss / (1024 * 1024))  # Convert to MB
                h["mem_vms"].append(mem.vms / (1024 * 1024))
                h["threads"].append(threads)
                h["io_read"].append(io_read / (1024 * 1024))
                h["io_write"].append(io_write / (1024 * 1024))
                h["timestamp"].append(time.time())

                results[name] = {"found": True, "history": h}
            except Exception as e:
                print(e)
                continue
        return results

class ProcessMonitor:
    def __init__(self, process_names, refresh_interval=2):
        self.process_names = process_names
        self.user = getpass.getuser()
        self.refresh_interval = refresh_interval
        self.collector = ProcessMetricCollector(self.process_names) # Defined elsewhere
        self.running = False

        # Controls setup
        self._btn_start = widgets.Button(description="▶  Start", button_style="success", layout=widgets.Layout(width="120px"))
        self._btn_start.on_click(self._on_start)
        
        self._btn_stop = widgets.Button(description="■  Stop", button_style="danger", layout=widgets.Layout(width="120px"), disabled=True)
        self._btn_stop.on_click(self._on_stop)

        self._interval_slider = widgets.FloatSlider(
            value=self.refresh_interval, min=0.5, max=10.0, step=0.5,
            description="Interval (s):", style={"description_width": "90px"}, layout=widgets.Layout(width="340px")
        )
        self._interval_slider.observe(self._on_interval_change, names="value")
        
        self._controls = widgets.HBox([self._btn_start, self._btn_stop, self._interval_slider])
        
        # We replace the Output container entirely with a VBox that will hold our UI and Plot
        self._dashboard = widgets.VBox([self._controls])
        self._plot = None 

    def show(self):
        # Just display the main dashboard layout
        display(self._dashboard)
        
    def _start_collecting(self):
        if self.running: return
        self.running = True
        self._btn_start.disabled = True
        self._btn_stop.disabled = False
        threading.Thread(target=self._collect_loop, daemon=True).start()

    def _stop_collecting(self):
        self.running = False
        self._btn_start.disabled = False
        self._btn_stop.disabled = True

    def _collect_loop(self):
        while self.running:
            print("plotting")
            metrics = self.collector.collect()
            self._render_metrics(metrics)
            time.sleep(self.refresh_interval)
    
    def _on_start(self, b): self._start_collecting()
    def _on_stop(self, b): self._stop_collecting()
    def _on_interval_change(self, change): self.refresh_interval = change["new"]

    def _render_metrics(self, metrics):
        if not metrics: return

        history_keys = [k for k in next(iter(metrics.values()))["history"].keys() if k != "timestamp"]
        
        if self._plot is None:
            num_procs = len(metrics)
            num_metrics = len(history_keys)
            
            # Create a subplot grid
            fig = make_subplots(rows=num_procs, cols=num_metrics, 
                                subplot_titles=[f"{m}" for p in metrics for m in history_keys])
            
            # Convert standard figure to an interactive FigureWidget
            self._plot = go.FigureWidget(fig)
            self._plot.layout.height = 400 * num_procs
            self._plot.layout.width = 2000
            self._plot.layout.margin = dict(l=20, r=20, t=40, b=20)
            self._plot.layout.showlegend = False
            
            # Add empty lines (traces) to the plot
            for i, (proc, proc_data) in enumerate(metrics.items()):
                for j, metric in enumerate(history_keys):
                    self._plot.add_scatter(y=[], row=i+1, col=j+1, mode='lines')

            # Append the plot directly to our dashboard VBox
            self._dashboard.children = [self._controls, self._plot]

        else:
            # batch_update prevents the plot from redrawing until all new data is injected
            with self._plot.batch_update():
                trace_idx = 0
                for proc, proc_data in metrics.items():
                    for metric in history_keys:
                        # Grab the new values
                        values = list(proc_data["history"][metric])
                        
                        # Update the specific line's Y data
                        self._plot.data[trace_idx].y = values
                        trace_idx += 1