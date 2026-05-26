"""
Monitoring package for Bijuty.

This package provides real-time metric collection and visualization for big
data clusters and processes via Plotly dashboards and ipywidgets.
"""

from .dashboard import MetricDashboard
from .spark import SparkMetricCollector, SparkMetricMonitor, SparkMetricsSnapshot, SparkMetricsHistory
from .flink import FlinkMetricCollector, FlinkMetricMonitor, FlinkMetricsSnapshot, FlinkMetricsHistory
from .process import ProcessMonitor, ProcessMetricsSnapshot, ProcessMetricsHistory

__all__ = [
    # dashboard
    "MetricDashboard",
    # spark
    "SparkMetricCollector",
    "SparkMetricMonitor",
    "SparkMetricsSnapshot",
    "SparkMetricsHistory",
    # flink
    "FlinkMetricCollector",
    "FlinkMetricMonitor",
    "FlinkMetricsSnapshot",
    "FlinkMetricsHistory",
    # process
    "ProcessMonitor",
    "ProcessMetricsSnapshot",
    "ProcessMetricsHistory",
]
