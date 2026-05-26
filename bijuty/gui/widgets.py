"""
Reusable widget factories and custom ipywidgets extensions.

This module provides standardized widget creation helpers (buttons, sliders,
dropdowns, text fields, checkboxes) and custom containers (VBox, HBox,
CustomCheckbox) used across the GUI.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import re

import ipywidgets as widgets
from traitlets import Bool

from .config import DEFAULT_LABEL_STYLE


DEFAULT_WIDGET_LAYOUT = widgets.Layout(
    width="100%",
    margin="5px 0px",
    display="flex",
    flex_flow="row",
)


# =============================================================================
# Standalone helpers
# =============================================================================

def fetch_image(url: str) -> bytes:
    """Fetch image content from URL."""
    import requests  # local import to keep dependency handling lazy
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.content
    except Exception:
        return b""


def create_placeholder_logo() -> widgets.HTML:
    """Create a placeholder logo widget."""
    return widgets.HTML(
        value="<div style='width:100px;height:100px;background-color:#eee;"
              "display:flex;align-items:center;justify-content:center;color:#999;'>logo</div>"
    )


# =============================================================================
# Widget Factory
# =============================================================================

class WidgetFactory:
    """Factory for creating standardized widgets."""

    @staticmethod
    def create_styled_button(
        description: str,
        style_overrides: Optional[Dict[str, Any]] = None,
        layout_overrides: Optional[Dict[str, Any]] = None,
        **button_kwargs,
    ) -> widgets.Button:
        """Create a styled button widget."""
        base_style = {
            "button_color": "#4caf50",
            "font_weight": "bold",
            "font_size": "14px",
        }
        base_layout = {
            "width": "120px",
            "height": "40px",
            "margin": "5px",
            "align_self": "center",
        }

        final_style = {**base_style, **(style_overrides or {})}
        final_layout = {**base_layout, **(layout_overrides or {})}

        return widgets.Button(
            description=description,
            style=widgets.ButtonStyle(**final_style),
            layout=widgets.Layout(**final_layout),
            **button_kwargs,
        )

    @staticmethod
    def create_styled_button_redirect(
        url: str,
        description: str,
        style_overrides: Optional[Dict[str, Any]] = None,
        layout_overrides: Optional[Dict[str, Any]] = None,
        **button_kwargs,
    ) -> widgets.HTML:
        link_html = f"""
            <a href="{url}" target="_blank" style="text-decoration:none;">
                <button class="p-Widget jupyter-widgets jupyter-button widget-button mod-primary"
                        style="width:160px; height:32px; cursor:pointer;" title="Open {url}">
                    {description}
                </button>
            </a>
            """
        return widgets.HTML(value=link_html)

    @staticmethod
    def update_widget_state(widget, disable=False):
        """Enable or disable an HTML-based button widget."""
        current_html = widget.value
        clean_html = re.sub(r'\s+disabled(?=[\s>])', '', current_html)
        if disable:
            new_html = re.sub(r'(<button[^>]*)(>)', r'\1 disabled\2', clean_html)
        else:
            new_html = clean_html
        widget.value = new_html
        return widget

    @staticmethod
    def create_slider(
        value: int,
        min_val: int,
        max_val: int,
        description: str,
        tooltip: str = None,
        step: int = 1,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> widgets.IntSlider:
        """Create a standardized IntSlider widget."""
        slider_style = (label_style or {}).copy()
        slider_style.setdefault("font_weight", "bold")
        slider_style.setdefault("color", "#333333")
        slider_style.setdefault("font_size", "14px")
        slider_style.setdefault("description_width", "200px")
        slider_style["handle_color"] = "blue"
        if not tooltip:
            tooltip = description
        return widgets.IntSlider(
            value=value,
            min=min_val,
            max=max_val,
            step=step,
            description=description,
            style=slider_style,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
            tooltip=tooltip,
        )

    @staticmethod
    def create_dropdown(
        options: List[str],
        value: Optional[str],
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> widgets.Dropdown:
        """Create a standardized Dropdown widget."""
        return widgets.Dropdown(
            options=options,
            value=value,
            description=description,
            style=label_style or DEFAULT_LABEL_STYLE,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
        )

    @staticmethod
    def create_text(
        value: str,
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
        disabled: bool = False,
    ) -> widgets.Text:
        """Create a standardized Text widget."""
        return widgets.Text(
            value=value,
            description=description,
            style=label_style or DEFAULT_LABEL_STYLE,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
            disabled=disabled,
        )

    @staticmethod
    def create_checkbox(
        value: bool,
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> "CustomCheckbox":
        """Create a standardized Checkbox widget."""
        return CustomCheckbox(
            value=value,
            description=description,
            indent=False,
            style=label_style or DEFAULT_LABEL_STYLE,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
        )


# =============================================================================
# Container mixins / extensions
# =============================================================================

class ContainerMixin:
    """Mixin that adds enable/disable functionality to Box widgets."""

    def disable(self):
        """Disable the container by adding the CSS class."""
        self.add_class("disable")

    def enable(self):
        """Enable the container by removing the CSS class."""
        self.remove_class("disable")

    def is_disabled(self) -> bool:
        """Check whether the container is currently disabled."""
        return "disable" in (self._dom_classes or [])

    def toggle(self):
        """Toggle between enabled and disabled states."""
        if self.is_disabled():
            self.enable()
        else:
            self.disable()


class VBox(ContainerMixin, widgets.VBox):
    """VBox extended with enable/disable support."""
    pass


class HBox(ContainerMixin, widgets.HBox):
    """HBox extended with enable/disable support."""
    pass


class CustomCheckbox(widgets.HBox):
    """Custom-styled checkbox with observable value trait and external label."""

    value = Bool(False).tag(sync=True)

    def __init__(self, description="Label", value=False, **kwargs):
        self._checkbox: widgets.Checkbox = widgets.Checkbox(value=value, indent=False)
        self._checkbox.add_class("custom-box-design")

        self._label: widgets.Label = widgets.Label(value=f"{description}: ")
        self._label.add_class("custom-box-label")

        self._css = widgets.HTML("""
            <style>
                .custom-box-design input[type='checkbox'] {
                    width: 20px;
                    height: 20px;
                    cursor: pointer;
                    accent-color: #007bff;
                    margin-left: 5px;
                }
                .custom-box-design { width: auto !important; }
                .custom-box-label {
                    width: 200px;
                    justify-content: right;
                }
            </style>
        """)

        super().__init__(children=[self._label, self._checkbox, self._css], **kwargs)
        widgets.link((self._checkbox, "value"), (self, "value"))

    def update_label(self, label: str):
        self._label.value = f"{label}: "

    @property
    def description(self):
        return self._label.value.rstrip(": ")

    @description.setter
    def description(self, value: str):
        self._label.value = f"{value}: "

    def is_checked(self):
        return self._checkbox.value
