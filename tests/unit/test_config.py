"""Unit tests for :mod:`bijuty.gui.config`."""

from __future__ import annotations

import dataclasses
import os

import pytest

from bijuty.gui.config import (
    COLOR_SCHEME,
    FRAMEWORK_REGISTRY,
    FrameworkConfig,
    ResourceAllocation,
)


class TestFrameworkConfig:
    """Tests for the :class:`FrameworkConfig` dataclass."""

    def _config(self, **overrides):
        data = dict(
            name="SPARK",
            proc_master={"title": "Master", "pattern": "master-pattern"},
            proc_worker={"title": "Worker", "pattern": "worker-pattern"},
            logo_url="https://example.invalid/logo.png",
            worker_file="workers",
            default_master_port=7077,
            rest_api_port=8080,
        )
        data.update(overrides)
        return FrameworkConfig(**data)

    def test_name_helpers_return_upper_and_lower(self):
        config = self._config()

        assert config.name_upper == "SPARK"
        assert config.name_lower == "spark"

    def test_name_helpers_handle_mixed_case(self):
        config = self._config(name="Flink")

        assert config.name_upper == "FLINK"
        assert config.name_lower == "flink"

    def test_default_template_points_at_framework_template_dir(self):
        config = self._config()

        normalized = os.path.normpath(config.default_template)
        assert normalized.endswith(os.path.join("framework_template", "spark"))

    def test_default_template_exists_on_disk(self):
        for config in FRAMEWORK_REGISTRY.values():
            assert os.path.isdir(os.path.normpath(config.default_template))

    def test_config_is_frozen(self):
        config = self._config()

        with pytest.raises(dataclasses.FrozenInstanceError):
            config.rest_api_port = 1234

    def test_optional_fields_default_to_none(self):
        config = self._config()

        assert config.default_resources is None
        assert config.proc_other is None
        assert config.web_ui_links is None


class TestResourceAllocation:
    """Tests for the :class:`ResourceAllocation` dataclass."""

    def test_defaults(self):
        allocation = ResourceAllocation()

        assert allocation.driver_memory == 1000
        assert allocation.worker_memory == 1000
        assert allocation.executor_memory == 1000
        assert allocation.driver_cpu == 1
        assert allocation.worker_cpu == 1
        assert allocation.executor_cpu == 1

    def test_to_dict_maps_short_keys(self):
        allocation = ResourceAllocation(
            driver_memory=1,
            worker_memory=2,
            executor_memory=3,
            driver_cpu=4,
            worker_cpu=5,
            executor_cpu=6,
        )

        assert allocation.to_dict() == {
            "drv_mem": 1,
            "wrk_mem": 2,
            "exe_mem": 3,
            "drv_cpu": 4,
            "wrk_cpu": 5,
            "exe_cpu": 6,
        }


class TestFrameworkRegistry:
    """Tests for the module-level framework registry."""

    def test_registry_contains_spark_and_flink(self):
        assert set(FRAMEWORK_REGISTRY) == {"SPARK", "FLINK"}

    @pytest.mark.parametrize(
        "name,rest_port,master_port",
        [("SPARK", 8080, 7077), ("FLINK", 8081, 6123)],
    )
    def test_expected_ports(self, name, rest_port, master_port):
        config = FRAMEWORK_REGISTRY[name]

        assert config.rest_api_port == rest_port
        assert config.default_master_port == master_port

    @pytest.mark.parametrize("name", ["SPARK", "FLINK"])
    def test_required_processes_and_worker_file(self, name):
        config = FRAMEWORK_REGISTRY[name]

        assert config.proc_master["pattern"]
        assert config.proc_worker["pattern"]
        assert config.worker_file == "workers"
        assert config.default_resources is not None
        assert config.web_ui_links

    def test_spark_and_flink_master_patterns_are_distinct(self):
        spark = FRAMEWORK_REGISTRY["SPARK"].proc_master["pattern"]
        flink = FRAMEWORK_REGISTRY["FLINK"].proc_master["pattern"]

        assert spark != flink


def test_color_scheme_contains_expected_keys():
    assert set(COLOR_SCHEME) >= {
        "master_bg",
        "master_text",
        "worker_bg",
        "worker_text",
    }
    assert all(value.startswith("#") for value in COLOR_SCHEME.values())
