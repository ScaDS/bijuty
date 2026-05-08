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

# import logging
# class IgnoreNoSuchComm(logging.Filter):
#     def filter(self, record):
#         return 'No such comm' not in record.getMessage()
# logging.getLogger('Comm').addFilter(IgnoreNoSuchComm())

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
from .multi_framework_manager import MultiFrameworkManager

__all__ = [
    "GUIUtils",
    "FRAMEWORK_REGISTRY",
    "HTMLGenerator",
    "WidgetFactory",
    "FrameworkConfig",
    "ResourceAllocation",
    "COLOR_SCHEME",
    "MultiFrameworkManager",
]

#GUIUtils().launch_gui_config()
MultiFrameworkManager().display()