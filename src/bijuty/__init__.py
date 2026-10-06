"""Big data utilities for configuring and managing Spark and Flink clusters."""

from __future__ import annotations
from .gui.multi_framework_manager import MultiFrameworkManager
from .gui.widgets import WidgetFactory
from .gui.html import HTMLGenerator
from .gui.config import (
    FRAMEWORK_REGISTRY,
    FrameworkConfig,
    ResourceAllocation,
    COLOR_SCHEME,
)
from .gui.main import GUIMain
from .templates import (
    available_templates,
    factory_template_path,
    init_template,
)

import logging
import sys

# =============================================================================
# Package-wide logging configuration
# =============================================================================


class _LoggerFormatter(logging.Formatter):
    """Colored formatter matching the legacy SimpleLogger output style."""

    _COLORS = {
        logging.DEBUG: "\033[90m",     # Gray
        logging.INFO: "\033[94m",      # Blue
        logging.WARNING: "\033[93m",   # Yellow
        logging.ERROR: "\033[91m",    # Red
        logging.CRITICAL: "\033[31m",  # Dark Red
    }
    _RESET = "\033[0m"  # Black

    def __init__(self, datefmt: str = "%d/%m/%Y %H:%M:%S"):
        super().__init__(datefmt=datefmt)

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        timestamp = self.formatTime(record, self.datefmt)
        padded = f"{record.levelname:<5}"
        color = self._COLORS.get(record.levelno, "")
        reset = self._RESET
        return f"{reset}[{color}{padded}{reset}] [{timestamp}] - {message}"


def set_log_level(level: int | str) -> None:
    """Set the log level for the entire ``bijuty`` package."""
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger("bijuty").setLevel(level)


# Configure the package root logger once
_bijuty_log = logging.getLogger("bijuty")
_bijuty_log.setLevel(logging.INFO)
# _bijuty_log.setLevel(logging.DEBUG)

# Avoid duplicate handlers on reload (common in Jupyter)
if not _bijuty_log.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(_LoggerFormatter())
    _bijuty_log.addHandler(_handler)


# Import GUI components for easy access

__all__ = [
    "GUIMain",
    "FRAMEWORK_REGISTRY",
    "HTMLGenerator",
    "WidgetFactory",
    "FrameworkConfig",
    "ResourceAllocation",
    "COLOR_SCHEME",
    "MultiFrameworkManager",
    "set_log_level",
    "init_template",
    "factory_template_path",
    "available_templates",
]

# Only auto-display when running inside an IPython kernel
try:
    from IPython import get_ipython
    if get_ipython() is not None:
        MultiFrameworkManager().display()
except ImportError:
    pass
