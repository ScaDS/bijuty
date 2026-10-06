"""Unit tests for :mod:`bijuty.gui.env_setup`."""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

from bijuty.gui.config import FRAMEWORK_REGISTRY
from bijuty.gui.env_setup import GUIEnvSetup


class FakeHost(GUIEnvSetup):
    """Test double wiring :class:`GUIEnvSetup` to scripted selections."""

    def __init__(self, **selections):
        defaults = {
            "framework": "SPARK",
            "master_host": "node1",
            "workers": ["node1", "node2"],
            "master_port": "7077",
            "config_template": "/templates/custom",
            "use_default_template": True,
            "framework_home": "/opt/framework",
            "config_destination": "/tmp/bijuty/spark",
            "driver_cpu": 1,
            "worker_cpu": 4,
            "executor_cpu": 2,
            "driver_memory": 1000,
            "worker_memory": 4000,
            "executor_memory": 2000,
            "local_dirs": "/tmp/local",
            "worker_dir": "/tmp/work",
            "log_dir": "/tmp/bijuty/spark/log",
            "pid_dir": "/tmp/bijuty/spark/pid",
        }
        defaults.update(selections)
        self.sel = defaults
        self.is_config_set = False
        self.messages = []
        self.bdm = MagicMock()

    def _log(self, message="", msg_type="info", **kwargs):
        self.messages.append((msg_type, message))

    def get_selected_framework_name(self):
        return self.sel["framework"]

    def get_selected_master_host(self):
        return self.sel["master_host"]

    def get_selected_workers(self):
        return self.sel["workers"]

    def get_selected_master_port(self):
        return self.sel["master_port"]

    def get_selected_config_template(self):
        return self.sel["config_template"]

    def is_default_config_template(self):
        return self.sel["use_default_template"]

    def get_selected_framework_home(self):
        return self.sel["framework_home"]

    def get_selected_config_destination(self):
        return self.sel["config_destination"]

    def get_selected_driver_cpu(self):
        return self.sel["driver_cpu"]

    def get_selected_worker_cpu(self):
        return self.sel["worker_cpu"]

    def get_selected_executor_cpu(self):
        return self.sel["executor_cpu"]

    def get_selected_driver_memory(self):
        return f"{self.sel['driver_memory']}m"

    def get_selected_worker_memory(self):
        return f"{self.sel['worker_memory']}m"

    def get_selected_executor_memory(self):
        return f"{self.sel['executor_memory']}m"

    def get_selected_local_dirs(self):
        return self.sel["local_dirs"]

    def get_selected_worker_dir(self):
        return self.sel["worker_dir"]

    def get_selected_log_dir(self):
        return self.sel["log_dir"]

    def get_selected_pid_dir(self):
        return self.sel["pid_dir"]


class TestEnvBuilders:
    def test_build_spark_env_updates(self):
        host = FakeHost()

        env = host._build_spark_env_updates()

        assert env["SPARK_MASTER_HOST"] == "node1"
        assert env["SPARK_WORKER_CORES"] == "4"
        assert env["SPARK_WORKER_MEMORY"] == "4000m"
        assert env["SPARK_EXECUTOR_CORES"] == "2"
        assert env["SPARK_EXECUTOR_MEMORY"] == "2000m"
        assert env["SPARK_DRIVER_MEMORY"] == "1000m"
        assert env["SPARK_MASTER_PORT"] == "7077"
        assert env["SPARK_CONF_DIR"] == "/tmp/bijuty/spark"
        assert "LD_LIBRARY_PATH" in env

    def test_build_flink_env_updates(self):
        host = FakeHost(framework="FLINK", master_port="6123")

        env = host._build_flink_env_updates()

        assert env["FLINK_MASTER_HOSTNAME"] == "node1"
        assert env["FLINK_MASTER_PORT"] == "6123"
        assert env["FLINK_MEM_MASTER"] == "1000m"
        assert env["FLINK_MEM_PER_WORKER"] == "4000m"
        assert env["FLINK_SLOTS_PER_TASKMANAGER"] == "2"
        assert env["FLINK_CONF_DIR"] == "/tmp/bijuty/spark"


class TestWorkerAndTemplateFiles:
    def test_update_worker_file_writes_selected_workers(self, tmp_path):
        conf_dir = tmp_path / "spark"
        conf_dir.mkdir()
        host = FakeHost(config_destination=str(conf_dir))

        host._update_worker_file()

        assert (conf_dir / "workers").read_text() == "node1\nnode2\n"

    def test_set_fw_config_template_uses_registry_default(self, monkeypatch):
        host = FakeHost()

        host._set_fw_config_template()

        value = os.environ["SPARK_CONF_TEMPLATE"]
        assert value.endswith(os.path.join("framework_template", "spark"))

    def test_set_fw_config_template_uses_custom_path(self, monkeypatch):
        host = FakeHost(use_default_template=False)

        host._set_fw_config_template()

        assert os.environ["SPARK_CONF_TEMPLATE"] == "/templates/custom"

    def test_create_conf_dest_dir_creates_parent(self, tmp_path):
        destination = tmp_path / "nested" / "spark"
        host = FakeHost(config_destination=str(destination))

        host._create_conf_dest_dir()

        assert destination.parent.is_dir()


class TestUpdateSparkEnvFile:
    def _prepare(self, tmp_path):
        conf_dir = tmp_path / "spark"
        conf_dir.mkdir()
        (conf_dir / "spark-env.sh").write_text(
            "export SPARK_MASTER_HOST=old\n"
            "# export SPARK_MASTER_PORT=1111\n"
        )
        (conf_dir / "log4j2.properties").write_text(
            "log=FRAMEWORK_LOG_DIR/app.log\n")
        return conf_dir

    def test_replace_comment_and_append_missing_variables(self, tmp_path, monkeypatch):
        conf_dir = self._prepare(tmp_path)
        host = FakeHost(config_destination=str(conf_dir))

        host._update_env_file()

        content = (conf_dir / "spark-env.sh").read_text()
        assert 'export SPARK_MASTER_HOST="node1"' in content
        assert 'export SPARK_MASTER_PORT="7077"' in content
        assert 'export SPARK_DRIVER_MEMORY="1000m"' in content
        # Log4j placeholder is substituted.
        log4j = (conf_dir / "log4j2.properties").read_text()
        assert "FRAMEWORK_LOG_DIR" not in log4j
        assert "/tmp/bijuty/spark/log" in log4j
        # Variables are exported into the process environment.
        assert os.environ["SPARK_MASTER_HOST"] == "node1"

    def test_update_flink_conf_file_replaces_placeholders(self, tmp_path):
        conf_dir = tmp_path / "flink"
        conf_dir.mkdir()
        for name in ("config.yaml", "masters"):
            (conf_dir / name).write_text("host: FLINK_MASTER_HOSTNAME\n")
        host = FakeHost(framework="FLINK", config_destination=str(conf_dir))

        host._update_flink_conf_file()

        for name in ("config.yaml", "masters"):
            text = (conf_dir / name).read_text()
            assert "FLINK_MASTER_HOSTNAME" not in text
            assert "host: node1" in text

    def test_update_flink_conf_file_skips_missing_files(self, tmp_path):
        conf_dir = tmp_path / "flink"
        conf_dir.mkdir()
        (conf_dir / "config.yaml").write_text("host: FLINK_MASTER_HOSTNAME\n")
        host = FakeHost(framework="FLINK", config_destination=str(conf_dir))

        # "masters" is absent and must be skipped without raising.
        host._update_flink_conf_file()

        assert "FLINK_MASTER_HOSTNAME" not in (
            conf_dir / "config.yaml"
        ).read_text()


class TestInitializeBigDataManager:
    def test_forwards_selection_to_manager(self):
        host = FakeHost()

        host._initialize_big_data_manager()

        host.bdm.initialize_user_input.assert_called_once()
        payload = host.bdm.initialize_user_input.call_args[0][0]
        assert payload["fw_name"] == "SPARK"
        assert payload["master"] == "node1"
        assert payload["workers"] == ["node1", "node2"]
        assert payload["fw_mapping"] is FRAMEWORK_REGISTRY


class TestEnsurePyflinkJar:
    def test_returns_false_without_framework_home(self):
        host = FakeHost(framework_home="")

        assert host._ensure_pyflink_jar_in_lib() is False

    def test_returns_false_without_lib_dir(self, tmp_path):
        host = FakeHost(framework_home=str(tmp_path / "flink"))

        assert host._ensure_pyflink_jar_in_lib() is False

    def test_returns_false_without_pyflink(self, tmp_path, monkeypatch):
        flink_home = tmp_path / "flink"
        (flink_home / "lib").mkdir(parents=True)
        host = FakeHost(framework_home=str(flink_home))
        monkeypatch.setitem(sys.modules, "pyflink", None)

        assert host._ensure_pyflink_jar_in_lib() is False

    def test_copies_versioned_jar_into_flink_lib(self, tmp_path, monkeypatch):
        flink_home = tmp_path / "flink"
        flink_lib = flink_home / "lib"
        flink_lib.mkdir(parents=True)

        pyflink_pkg = tmp_path / "pyflinkpkg"
        opt_dir = pyflink_pkg / "opt"
        opt_dir.mkdir(parents=True)
        jar = opt_dir / "flink-python-1.0.jar"
        jar.write_bytes(b"jar-bytes")
        (pyflink_pkg / "__init__.py").write_text("")

        fake_pyflink = SimpleNamespace(__file__=str(pyflink_pkg / "__init__.py"))
        monkeypatch.setitem(sys.modules, "pyflink", fake_pyflink)
        host = FakeHost(framework_home=str(flink_home))

        assert host._ensure_pyflink_jar_in_lib() is True
        assert (flink_lib / jar.name).read_bytes() == b"jar-bytes"
