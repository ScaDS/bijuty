"""GUI package for Bijuty."""

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
    DEFAULT_WIDGET_LAYOUT,
)
from .main import GUIMain

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
    "DEFAULT_WIDGET_LAYOUT",
    # Main gui class
    "GUIMain",
    "MultiFrameworkManager"
]
