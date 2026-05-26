"""
GUI package for Bijuty.

This package provides reusable components for building Jupyter notebook-based
interfaces to configure and manage big data frameworks (Spark, Flink).
"""

from .config import FrameworkConfig, ResourceAllocation, FRAMEWORK_REGISTRY, COLOR_SCHEME
from .html import HTMLGenerator
from .widgets import (
    WidgetFactory,
    CustomCheckbox,
    VBox,
    HBox,
    ContainerMixin,
    create_placeholder_logo,
    fetch_image,
    DEFAULT_LABEL_STYLE,
    DEFAULT_WIDGET_LAYOUT,
)
from .cluster_configurator import ClusterConfigurator

__all__ = [
    # config
    "FrameworkConfig",
    "ResourceAllocation",
    "FRAMEWORK_REGISTRY",
    "COLOR_SCHEME",
    # html
    "HTMLGenerator",
    # widgets
    "WidgetFactory",
    "CustomCheckbox",
    "VBox",
    "HBox",
    "ContainerMixin",
    "create_placeholder_logo",
    "fetch_image",
    "DEFAULT_LABEL_STYLE",
    "DEFAULT_WIDGET_LAYOUT",
    # cluster_configurator
    "ClusterConfigurator",
]
