"""
Big Data Utilities for JupyterHub.

This package provides tools for configuring and managing big data frameworks
(Spark, Flink) in Jupyter Notebook environments.
"""

# Only load magic extension when in IPython environment
try:
    from IPython import get_ipython
    if get_ipython() is not None:
        from .magic import load_ipython_extension
except ImportError:
    pass

# Import GUI components for easy access
from .gui_utils import GUIUtils
from .gui_components import (
    FRAMEWORK_REGISTRY,
    HTMLGenerator,
    WidgetFactory,
    FrameworkConfig,
    ResourceAllocation,
    COLOR_SCHEME,
)

__all__ = [
    "GUIUtils",
    "FRAMEWORK_REGISTRY",
    "HTMLGenerator",
    "WidgetFactory",
    "FrameworkConfig",
    "ResourceAllocation",
    "COLOR_SCHEME",
]

GUIUtils().launch_gui_config()