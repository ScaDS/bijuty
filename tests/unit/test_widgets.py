"""Unit tests for :mod:`bijuty.gui.widgets`."""

from __future__ import annotations

from unittest.mock import patch

import ipywidgets as widgets

from bijuty.gui.widgets import (
    DEFAULT_SLIDER_HANDLE_COLOR,
    CustomCheckbox,
    HBox,
    VBox,
    WidgetFactory,
    create_placeholder_logo,
    fetch_image,
)


class TestFetchImage:
    """Tests for :func:`fetch_image`."""

    def test_returns_response_content_on_success(self):
        with patch("requests.get") as get:
            get.return_value.content = b"image-bytes"
            get.return_value.raise_for_status.return_value = None

            assert fetch_image("https://example.invalid/logo.png") == b"image-bytes"

        get.assert_called_once_with("https://example.invalid/logo.png", timeout=5)

    def test_returns_empty_bytes_on_request_failure(self):
        with patch("requests.get", side_effect=Exception("network down")):
            assert fetch_image("https://example.invalid/logo.png") == b""

    def test_returns_empty_bytes_on_http_error_status(self):
        with patch("requests.get") as get:
            get.return_value.raise_for_status.side_effect = Exception("404")

            assert fetch_image("https://example.invalid/missing.png") == b""


def test_create_placeholder_logo_returns_html_widget():
    logo = create_placeholder_logo()

    assert isinstance(logo, widgets.HTML)
    assert "logo" in logo.value


class TestWidgetFactoryButtons:
    """Tests covering button creation helpers."""

    def test_create_styled_button_sets_description_and_class(self):
        button = WidgetFactory.create_styled_button("Launch")

        assert isinstance(button, widgets.Button)
        assert button.description == "Launch"
        assert "gui-button" in button._dom_classes

    def test_create_styled_button_applies_style_and_layout_overrides(self):
        button = WidgetFactory.create_styled_button(
            "Launch",
            style_overrides={"button_color": "red"},
            layout_overrides={"width": "50%"},
        )

        assert button.style.button_color == "red"
        assert button.layout.width == "50%"

    def test_create_styled_button_redirect_embeds_url_and_target(self):
        html = WidgetFactory.create_styled_button_redirect(
            url="http://localhost:8080", description="Spark UI")

        assert isinstance(html, widgets.HTML)
        assert "http://localhost:8080" in html.value
        assert "Spark UI" in html.value
        assert 'target="_blank"' in html.value


class TestUpdateWidgetState:
    """Tests for :meth:`WidgetFactory.update_widget_state`."""

    def _button(self):
        return WidgetFactory.create_styled_button_redirect(
            url="http://localhost:8080", description="UI")

    def test_disable_adds_disabled_attribute(self):
        button = self._button()

        result = WidgetFactory.update_widget_state(button, disable=True)

        assert result is button
        assert "disabled" in button.value

    def test_enable_removes_disabled_attribute(self):
        button = self._button()
        WidgetFactory.update_widget_state(button, disable=True)

        WidgetFactory.update_widget_state(button, disable=False)

        assert "<button" in button.value
        assert " disabled" not in button.value

    def test_disable_is_idempotent(self):
        button = self._button()

        WidgetFactory.update_widget_state(button, disable=True)
        WidgetFactory.update_widget_state(button, disable=True)

        assert button.value.count("disabled") == 1


class TestWidgetFactoryInputs:
    """Tests for slider, dropdown, text and checkbox factories."""

    def test_create_slider_defaults(self):
        slider = WidgetFactory.create_slider(
            value=2, min_val=1, max_val=10, description="Cores")

        assert isinstance(slider, widgets.IntSlider)
        assert (slider.value, slider.min, slider.max, slider.step) == (2, 1, 10, 1)
        assert slider.tooltip == "Cores"
        assert "slider-style" in slider._dom_classes
        assert slider.style.handle_color == DEFAULT_SLIDER_HANDLE_COLOR

    def test_create_slider_custom_style_skips_default_classes(self):
        slider = WidgetFactory.create_slider(
            value=1, min_val=1, max_val=4, description="Cores",
            tooltip="tip", step=2, label_style={"description_width": "120px"})

        assert slider.tooltip == "tip"
        assert slider.step == 2
        assert "slider-style" not in slider._dom_classes

    def test_create_dropdown(self):
        dropdown = WidgetFactory.create_dropdown(
            options=["A", "B"], value="B", description="Pick")

        assert isinstance(dropdown, widgets.Dropdown)
        assert dropdown.options == ("A", "B")
        assert dropdown.value == "B"
        assert "default-label-style" in dropdown._dom_classes

    def test_create_text_disabled_flag(self):
        text = WidgetFactory.create_text(
            value="path", description="Path", disabled=True)

        assert isinstance(text, widgets.Text)
        assert text.value == "path"
        assert text.disabled is True

    def test_create_checkbox_returns_custom_checkbox(self):
        checkbox = WidgetFactory.create_checkbox(
            value=True, description="Enable")

        assert isinstance(checkbox, CustomCheckbox)
        assert checkbox.value is True


class TestContainerMixin:
    """Tests for the enable/disable container mixin."""

    def test_disable_enable_toggle_vbox(self):
        box = VBox([widgets.HTML("x")])

        assert box.is_disabled() is False

        box.disable()
        assert box.is_disabled() is True
        assert "disable" in box._dom_classes

        box.enable()
        assert box.is_disabled() is False

    def test_toggle_flips_state(self):
        box = HBox([widgets.HTML("x")])

        box.toggle()
        assert box.is_disabled() is True
        box.toggle()
        assert box.is_disabled() is False


class TestCustomCheckbox:
    """Tests for :class:`CustomCheckbox`."""

    def test_initial_value_and_is_checked(self):
        checkbox = CustomCheckbox(description="Monitor", value=True)

        assert checkbox.value is True
        assert checkbox.is_checked() is True
        assert checkbox.description == "Monitor"

    def test_setting_value_updates_underlying_checkbox(self):
        checkbox = CustomCheckbox(description="Monitor", value=False)

        checkbox.value = True

        assert checkbox.is_checked() is True

    def test_update_label_and_description_setter(self):
        checkbox = CustomCheckbox(description="Old", value=False)

        checkbox.update_label("Use custom SPARK_HOME")
        assert checkbox.description == "Use custom SPARK_HOME"

        checkbox.description = "Renamed"
        assert checkbox.description == "Renamed"
        assert checkbox._label.value == "Renamed: "
