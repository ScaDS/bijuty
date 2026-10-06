"""Unit tests for the top-level :mod:`bijuty` package API."""

from __future__ import annotations

import logging
import time

import bijuty
from bijuty import set_log_level


class TestSetLogLevel:
    def test_accepts_string_level(self):
        try:
            set_log_level("DEBUG")

            assert logging.getLogger("bijuty").level == logging.DEBUG
        finally:
            set_log_level("INFO")

    def test_accepts_numeric_level(self):
        try:
            set_log_level(logging.ERROR)

            assert logging.getLogger("bijuty").level == logging.ERROR
        finally:
            set_log_level("INFO")

    def test_unknown_string_falls_back_to_info(self):
        try:
            set_log_level("not-a-level")

            assert logging.getLogger("bijuty").level == logging.INFO
        finally:
            set_log_level("INFO")


class TestPackageExports:
    def test_all_exports_are_importable(self):
        for name in bijuty.__all__:
            assert hasattr(bijuty, name), name

    def test_expected_public_symbols(self):
        assert {
            "GUIMain",
            "FRAMEWORK_REGISTRY",
            "FrameworkConfig",
            "ResourceAllocation",
            "MultiFrameworkManager",
            "set_log_level",
        } <= set(bijuty.__all__)

    def test_package_logger_has_single_stdout_handler(self):
        logger = logging.getLogger("bijuty")

        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1


class TestLoggerFormatter:
    def test_format_includes_level_and_message(self):
        formatter = bijuty._LoggerFormatter()
        record = logging.LogRecord(
            name="bijuty", level=logging.WARNING, pathname=__file__,
            lineno=1, msg="something happened", args=(), exc_info=None)

        rendered = formatter.format(record)

        assert "something happened" in rendered
        assert "WARN" in rendered

    def test_format_uses_custom_datefmt(self):
        formatter = bijuty._LoggerFormatter(datefmt="%Y")
        record = logging.LogRecord(
            name="bijuty", level=logging.INFO, pathname=__file__,
            lineno=1, msg="hi", args=(), exc_info=None)
        expected_year = time.strftime("%Y", time.localtime(record.created))

        assert expected_year in formatter.format(record)
