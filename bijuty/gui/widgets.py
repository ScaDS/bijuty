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


DEFAULT_WIDGET_LAYOUT = widgets.Layout(
    width="100%",
    margin="5px 0px",
    display="flex",
    flex_flow="row",
)

DEFAULT_SLIDER_HANDLE_COLOR = "#ffffff00"

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
        kwargs: Dict[str, Any] = {**button_kwargs}
        if style_overrides:
            kwargs["style"] = widgets.ButtonStyle(**style_overrides)
        if layout_overrides:
            kwargs["layout"] = widgets.Layout(**layout_overrides)
        button = widgets.Button(description=description, **kwargs)
        button.add_class("gui-button")
        return button

    @staticmethod
    def create_styled_button_redirect(
        url: str,
        description: str,
        style_overrides: Optional[Dict[str, Any]] = None,
        layout_overrides: Optional[Dict[str, Any]] = None,
        **button_kwargs,
    ) -> widgets.HTML:
        link_html = f"""
            <a href="{url}" target="_blank" class="gui-redirect-link">
                <button class="gui-redirect-button" title="Open {url}">
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
            new_html = re.sub(r'(<button[^>]*)(>)',
                              r'\1 disabled\2', clean_html)
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
        if not tooltip:
            tooltip = description
        kwargs: Dict[str, Any] = dict(
            value=value,
            min=min_val,
            max=max_val,
            step=step,
            description=description,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
            tooltip=tooltip,
        )
        style = label_style or {}
        style = {**style, "handle_color": DEFAULT_SLIDER_HANDLE_COLOR}
        kwargs["style"] = style
        slider = widgets.IntSlider(**kwargs)
        if label_style is None:
            slider.add_class("default-label-style")
            slider.add_class("slider-style")
            # for i in [ "slider-track", "ui-slider", "ui-slider-range" ]:
            #     slider.add_class(i)
        return slider

    @staticmethod
    def create_dropdown(
        options: List[str],
        value: Optional[str],
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> widgets.Dropdown:
        """Create a standardized Dropdown widget."""
        kwargs: Dict[str, Any] = dict(
            options=options,
            value=value,
            description=description,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
        )
        if label_style is not None:
            kwargs["style"] = label_style
        dropdown = widgets.Dropdown(**kwargs)
        if label_style is None:
            dropdown.add_class("default-label-style")
        return dropdown

    @staticmethod
    def create_text(
        value: str,
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
        disabled: bool = False,
    ) -> widgets.Text:
        """Create a standardized Text widget."""
        kwargs: Dict[str, Any] = dict(
            value=value,
            description=description,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
            disabled=disabled,
        )
        if label_style is not None:
            kwargs["style"] = label_style
        text = widgets.Text(**kwargs)
        if label_style is None:
            text.add_class("default-label-style")
        return text

    @staticmethod
    def create_checkbox(
        value: bool,
        description: str,
        label_style: Optional[Dict[str, str]] = None,
        layout: Optional[widgets.Layout] = None,
    ) -> "CustomCheckbox":
        """Create a standardized Checkbox widget."""
        kwargs: Dict[str, Any] = dict(
            value=value,
            description=description,
            indent=False,
            layout=layout or DEFAULT_WIDGET_LAYOUT,
        )
        if label_style is not None:
            kwargs["style"] = label_style
        checkbox = CustomCheckbox(**kwargs)
        if label_style is None:
            checkbox.add_class("default-label-style")
        return checkbox


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
        self._checkbox: widgets.Checkbox = widgets.Checkbox(
            value=value, indent=False)
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

        super().__init__(children=[self._label,
                                   self._checkbox, self._css], **kwargs)
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
