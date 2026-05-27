"""
Multi-framework management widget for Jupyter notebooks.

Provides a tabbed interface with a single initial tab and dynamic "+" / "x"
controls to add or remove framework GUIs on demand.
"""

from __future__ import annotations

from typing import Any

import ipywidgets as widgets
from IPython.display import display

from .gui.cluster_configurator import ClusterConfigurator
from .gui.config import FRAMEWORK_REGISTRY


class MultiFrameworkManager:
    """Dynamic tabbed manager for big data framework GUIs.

    Starts with a single tab. Clicking **+** appends a new ``GUIUtils`` tab,
    and clicking **x** closes the currently-selected tab (at least one tab
    always remains).

    Example::

        manager = MultiFrameworkManager()
        manager.display()
    """

    def __init__(self) -> None:
        """Initialize the multi-framework manager."""
        self._gui_instances: list[GUIUtils] = []
        self._tabs: widgets.Tab | None = None
        self._root_layout: widgets.HBox | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def display(self) -> widgets.HBox:
        """Build the GUI and immediately display it.

        Returns:
            The root layout widget.
        """
        self.build_widget()
        display(self._root_layout)
        return self._root_layout

    def build_widget(self) -> widgets.HBox:
        """Build (or rebuild) the widget without displaying it.

        Returns:
            The root ``HBox`` containing the tabs and controls.
        """
        self._gui_instances.clear()

        # Create initial single tab
        gui = ClusterConfigurator()
        gui.launch_gui_config(display_gui=False)
        self._gui_instances.append(gui)

        self._tabs = widgets.Tab(
            children=[self._wrap_tab(gui.main_container)],
            layout=widgets.Layout(width="98%", height="auto"),
        )
        self._set_tab_title(0, "Cluster 1")

        # Side controls: add / close
        add_btn = widgets.Button(
            description="+",
            tooltip="Add new cluster tab",
            layout=widgets.Layout(width="20px", height="20px"),
            button_style="info",
        )
        add_btn.on_click(self._on_add_tab)

        close_btn = widgets.Button(
            description="x",
            tooltip="Close selected tab",
            layout=widgets.Layout(width="20px", height="20px"),
            button_style="danger",
        )
        close_btn.on_click(self._on_close_tab)

        controls = widgets.VBox(
            [add_btn, close_btn],
            layout=widgets.Layout(
                width="20px",
                justify_content="flex-start",
                align_items="center",
                padding="4px 0 0 0",
            ),
        )

        self._root_layout = widgets.HBox(
            [self._tabs, controls],
            layout=widgets.Layout(width="100%", align_items="flex-start"),
        )

        return self._root_layout

    def get_tab_count(self) -> int:
        """Return the current number of tabs."""
        return len(self._gui_instances)

    def get_gui(self, idx: int = 0) -> GUIUtils:
        """Get the ``GUIUtils`` instance for a given tab index.

        Args:
            idx: Tab index (0-based).

        Raises:
            IndexError: If the tab index is out of range.

        Returns:
            The ``GUIUtils`` instance at the requested index.
        """
        return self._gui_instances[idx]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wrap_tab(self, content: widgets.Widget) -> widgets.Box:
        """Wrap a widget in a scrollable container suitable for a tab."""
        return widgets.Box(
            [content],
            layout=widgets.Layout(width="100%", overflow="auto"),
        )

    def _set_tab_title(self, idx: int, name: str) -> None:
        """Set the title of a tab."""
        if self._tabs is not None:
            self._tabs.set_title(idx, name)

    def _on_add_tab(self, _button: widgets.Button) -> None:
        """Create a new GUI instance and append it as a new tab."""
        new_gui = GUIUtils()
        new_gui.launch_gui_config(display_gui=False)
        self._gui_instances.append(new_gui)

        new_idx = len(self._gui_instances) - 1
        # Extend tab children and select the new tab
        wrapped = self._wrap_tab(new_gui.main_container)
        self._tabs.children = (*self._tabs.children, wrapped)
        self._set_tab_title(new_idx, f"Cluster {new_idx + 1}")
        self._tabs.selected_index = new_idx

    def _on_close_tab(self, _button: widgets.Button) -> None:
        """Close the currently selected tab if more than one tab exists."""
        if len(self._gui_instances) <= 1:
            return  # keep at least one tab

        current = self._tabs.selected_index
        if current is None or current < 0:
            return

        # Remove from our list and rebuild children
        self._gui_instances.pop(current)

        new_children = [
            self._wrap_tab(g.main_container)
            for g in self._gui_instances
        ]
        self._tabs.children = tuple(new_children)

        # Re-label tabs
        for idx in range(len(self._gui_instances)):
            self._set_tab_title(idx, f"Cluster {idx + 1}")

        # Adjust selection if the removed tab was last
        if current >= len(self._gui_instances):
            self._tabs.selected_index = len(self._gui_instances) - 1
