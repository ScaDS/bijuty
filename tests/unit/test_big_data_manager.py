"""Unit tests for :mod:`bijuty.big_data_manager`."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psutil
import pytest
import requests

from bijuty.big_data_manager import BigDataManager
from bijuty.gui.config import FRAMEWORK_REGISTRY


# =============================================================================
# Helpers
# =============================================================================


def _make_manager(fake_slurm, framework="SPARK", **overrides):
    manager = BigDataManager(slurm_info=fake_slurm)
    props = {
        "fw_name": framework,
        "fw_home": "/opt/spark",
        "master": "node1",
        "workers": ["node1", "node2"],
        "master_port": "7077",
        "conf_dir": "/tmp/spark/conf",
        "log_dir": "/tmp/spark/log",
        "fw_mapping": FRAMEWORK_REGISTRY,
    }
    props.update(overrides)
    manager.initialize_user_input(props)
    return manager


@pytest.fixture
def manager(fake_slurm) -> BigDataManager:
    return _make_manager(fake_slurm)


# =============================================================================
# Initialisation
# =============================================================================


class TestInitializeUserInput:
    def test_populates_user_inputs_and_framework_mapping(self, manager):
        inputs = manager._user_inputs

        assert inputs.fw_name == "SPARK"
        assert inputs.master == "node1"
        assert inputs.workers == ["node1", "node2"]
        assert inputs.workers_count == 2
        assert inputs.master_port == "7077"
        assert inputs.rest_api_port == 8080
        assert manager._fw_mapping is FRAMEWORK_REGISTRY["SPARK"]
        assert manager._initialized is True

    def test_flink_mapping_uses_flink_rest_port(self, fake_slurm):
        manager = _make_manager(fake_slurm, framework="FLINK")

        assert manager._user_inputs.rest_api_port == 8081

    def test_missing_keys_raise_wrapped_exception(self, fake_slurm):
        manager = BigDataManager(slurm_info=fake_slurm)

        with pytest.raises(Exception, match="Error while initializing user input"):
            manager.initialize_user_input({"fw_name": "SPARK"})

    def test_unknown_framework_raises_wrapped_exception(self, fake_slurm):
        manager = BigDataManager(slurm_info=fake_slurm)

        with pytest.raises(Exception, match="Error while initializing user input"):
            manager.initialize_user_input({
                "fw_name": "HADOOP",
                "fw_home": "/opt/hadoop",
                "master": "node1",
                "workers": ["node1"],
                "master_port": "1",
                "conf_dir": "/tmp",
                "log_dir": "/tmp",
                "fw_mapping": FRAMEWORK_REGISTRY,
            })

    def test_get_cluster_log_file_uses_log_dir(self, manager):
        assert manager._get_cluster_log_file() == "/tmp/spark/log/cluster_log"


# =============================================================================
# Process discovery
# =============================================================================


class TestGetFrameworkProcesses:
    def test_returns_empty_tuple_before_initialization(self, fake_slurm):
        manager = BigDataManager(slurm_info=fake_slurm)

        assert manager.get_fw_cluster_processes() == ()

    def test_returns_master_and_worker_for_framework(self, manager):
        master, worker = manager.get_fw_cluster_processes()

        assert master is FRAMEWORK_REGISTRY["SPARK"].proc_master
        assert worker is FRAMEWORK_REGISTRY["SPARK"].proc_worker

    def test_all_procs_returns_other_processes(self, manager):
        procs = manager.get_fw_cluster_processes(all_procs=True)

        assert procs == FRAMEWORK_REGISTRY["SPARK"].proc_other


class TestFindClusterProcesses:
    def test_matches_required_pattern_for_current_user(self, manager):
        proc = SimpleNamespace(pid=101, info={
            "username": "tester", "pid": 101, "name": "java",
            "cmdline": ["java", "-cp", "org.apache.spark.deploy.master.Master", "--host"],
        })

        with patch("bijuty.big_data_manager.getpass.getuser", return_value="tester"), \
                patch("bijuty.big_data_manager.psutil.process_iter", return_value=[proc]):
            found = manager._find_cluster_processes(["org.apache.spark.deploy.master.Master"])

        assert found == [(101, "java",
                          "java -cp org.apache.spark.deploy.master.Master --host")]

    def test_ignores_processes_owned_by_other_users(self, manager):
        proc = SimpleNamespace(pid=1, info={
            "username": "someone-else", "pid": 1, "name": "java",
            "cmdline": ["java", "pattern"]})

        with patch("bijuty.big_data_manager.getpass.getuser", return_value="tester"), \
                patch("bijuty.big_data_manager.psutil.process_iter", return_value=[proc]):
            assert manager._find_cluster_processes(["pattern"]) == []

    def test_ignores_processes_without_cmdline(self, manager):
        proc = SimpleNamespace(pid=1, info={
            "username": "tester", "pid": 1, "name": "java", "cmdline": None})

        with patch("bijuty.big_data_manager.getpass.getuser", return_value="tester"), \
                patch("bijuty.big_data_manager.psutil.process_iter", return_value=[proc]):
            assert manager._find_cluster_processes(["pattern"]) == []


class TestTerminateProcesses:
    def test_terminates_and_hard_kills_survivors(self, manager):
        proc = MagicMock()
        with patch("bijuty.big_data_manager.psutil.Process", return_value=proc), \
                patch("bijuty.big_data_manager.psutil.wait_procs",
                      return_value=([], [proc])) as wait:
            manager._terminate_processes([(5, "java", "cmd")])

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        wait.assert_called_once()

    def test_survives_already_gone_process(self, manager):
        with patch("bijuty.big_data_manager.psutil.Process",
                   side_effect=psutil.NoSuchProcess(5)), \
                patch("bijuty.big_data_manager.psutil.wait_procs") as wait:
            manager._terminate_processes([(5, "java", "cmd")])

        wait.assert_not_called()


# =============================================================================
# Cluster status
# =============================================================================


class TestVerifyClusterWorkers:
    def test_no_workers_configured(self, manager):
        manager._user_inputs.workers = []

        ok, count, err = manager._verify_cluster_workers()

        assert (ok, count, err) == (False, 0, "no workers configured")

    def test_spark_counts_alive_workers(self, manager):
        manager._user_inputs.workers = ["node1"]
        response = MagicMock()
        response.json.return_value = {
            "workers": [
                {"state": "ALIVE", "host": " node1 "},
                {"state": "DEAD", "host": "node2"},
            ]
        }
        response.raise_for_status.return_value = None

        with patch("bijuty.big_data_manager.requests.get", return_value=response):
            ok, count, err = manager._verify_cluster_workers()

        assert (ok, count, err) == (True, 1, "")

    def test_spark_reports_mismatch_when_worker_missing(self, manager):
        manager._user_inputs.workers = ["node1", "node2"]
        response = MagicMock()
        response.json.return_value = {
            "workers": [{"state": "ALIVE", "host": "node1"}]
        }
        response.raise_for_status.return_value = None

        with patch("bijuty.big_data_manager.requests.get", return_value=response):
            ok, count, _ = manager._verify_cluster_workers()

        assert ok is False
        assert count == 1

    def test_flink_counts_taskmanagers(self, fake_slurm):
        manager = _make_manager(fake_slurm, framework="FLINK",
                                workers=["tm1", "tm2"])
        response = MagicMock()
        response.json.return_value = {"taskmanagers": [{"id": "tm1"}, {"id": "tm2"}]}
        response.raise_for_status.return_value = None

        with patch("bijuty.big_data_manager.requests.get", return_value=response):
            ok, count, err = manager._verify_cluster_workers()

        assert (ok, count, err) == (True, 2, "")

    def test_request_exception_is_returned(self, manager):
        manager._user_inputs.workers = ["node1"]

        with patch("bijuty.big_data_manager.requests.get",
                   side_effect=requests.exceptions.ConnectionError("down")):
            ok, count, err = manager._verify_cluster_workers()

        assert ok is False
        assert count == 0
        assert isinstance(err, requests.exceptions.RequestException)

    def test_unknown_framework_reports_no_workers(self, manager):
        manager._user_inputs.fw_name = "HADOOP"

        ok, count, err = manager._verify_cluster_workers()

        assert (ok, count, err) == (False, 0, "no workers configured")


class TestIsClusterUp:
    def test_false_before_initialization(self, fake_slurm):
        manager = BigDataManager(slurm_info=fake_slurm)

        assert manager.is_cluster_up() is False

    def test_false_when_fewer_than_two_processes(self, manager):
        with patch.object(manager, "get_fw_cluster_processes", return_value=()):
            assert manager.is_cluster_up() is False

    def test_true_when_master_process_and_workers_found(self, manager):
        master_pattern = FRAMEWORK_REGISTRY["SPARK"].proc_master["pattern"]
        proc = SimpleNamespace(
            pid=10,
            info={"username": "tester",
                  "cmdline": ["java", master_pattern]},
        )
        with patch("bijuty.big_data_manager.getpass.getuser", return_value="tester"), \
                patch("bijuty.big_data_manager.psutil.process_iter", return_value=[proc]), \
                patch.object(manager, "_verify_cluster_workers",
                             return_value=(True, 2, "")):
            assert manager.is_cluster_up() is True

    def test_false_when_workers_report_down(self, manager):
        master_pattern = FRAMEWORK_REGISTRY["SPARK"].proc_master["pattern"]
        proc = SimpleNamespace(
            pid=10, info={"username": "tester", "cmdline": ["java", master_pattern]})

        with patch("bijuty.big_data_manager.getpass.getuser", return_value="tester"), \
                patch("bijuty.big_data_manager.psutil.process_iter", return_value=[proc]), \
                patch.object(manager, "_verify_cluster_workers",
                             return_value=(False, 0, "down")):
            assert manager.is_cluster_up() is False

    def test_false_when_master_process_missing(self, manager):
        proc = SimpleNamespace(
            pid=10, info={"username": "tester", "cmdline": ["java", "other"]})

        with patch("bijuty.big_data_manager.getpass.getuser", return_value="tester"), \
                patch("bijuty.big_data_manager.psutil.process_iter", return_value=[proc]), \
                patch.object(manager, "_verify_cluster_workers",
                             return_value=(True, 2, "")):
            assert manager.is_cluster_up() is False


# =============================================================================
# Cleanup
# =============================================================================


class TestCleanupCluster:
    def test_no_patterns_logs_and_returns(self, manager):
        with patch.object(manager, "get_fw_cluster_processes",
                          return_value=[{"pattern": ""}]) as get_procs, \
                patch.object(manager, "_find_cluster_processes") as find:
            manager._cleanup_cluster()

        find.assert_not_called()

    def test_no_running_processes_is_noop(self, manager):
        with patch.object(manager, "get_fw_cluster_processes",
                          return_value=[{"pattern": "pattern"}]), \
                patch.object(manager, "_find_cluster_processes", return_value=[]), \
                patch.object(manager, "_terminate_processes") as terminate:
            manager._cleanup_cluster()

        terminate.assert_not_called()

    def test_terminates_found_processes(self, manager):
        found = [(1, "java", "cmd")]
        with patch.object(manager, "get_fw_cluster_processes",
                          return_value=[{"pattern": "pattern"}]), \
                patch.object(manager, "_find_cluster_processes", return_value=found), \
                patch.object(manager, "_terminate_processes") as terminate:
            manager._cleanup_cluster()

        terminate.assert_called_once_with(found)


# =============================================================================
# Wait helpers
# =============================================================================


class TestWaitHelpers:
    def test_wait_for_cluster_init_true_when_up(self):
        manager = MagicMock()
        manager.is_cluster_up.return_value = True

        assert BigDataManager._wait_for_cluster_init(manager, timeout=30) is True

    def test_wait_for_cluster_init_false_on_timeout(self):
        manager = MagicMock()
        manager.is_cluster_up.return_value = False

        assert BigDataManager._wait_for_cluster_init(manager, timeout=0) is False

    def test_wait_for_cluster_stop_true_when_down(self):
        manager = MagicMock()
        manager.is_cluster_up.return_value = False

        assert BigDataManager._wait_for_cluster_stop(manager, timeout=30) is True

    def test_wait_for_cluster_stop_false_on_timeout(self):
        manager = MagicMock()
        manager.is_cluster_up.return_value = True

        assert BigDataManager._wait_for_cluster_stop(manager, timeout=0) is False


# =============================================================================
# Start / stop
# =============================================================================


class TestStartCluster:
    def test_returns_one_when_not_initialized(self, fake_slurm):
        manager = BigDataManager(slurm_info=fake_slurm)

        assert manager.start_cluster() == 1

    def test_starts_successfully(self, manager, monkeypatch):
        monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark/conf")
        ok = subprocess.CompletedProcess(args=[], returncode=0,
                                         stdout="started", stderr="")

        with patch.object(manager, "is_cluster_up", return_value=False), \
                patch.object(manager, "_wait_for_cluster_init", return_value=True), \
                patch("bijuty.big_data_manager.run_bash_command",
                      return_value=ok) as run:
            assert manager.start_cluster() is True

        run.assert_called_once()
        assert run.call_args.kwargs["shell"] is True

    def test_stops_existing_cluster_before_start(self, manager, monkeypatch):
        monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark/conf")
        ok = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

        with patch.object(manager, "is_cluster_up", return_value=True), \
                patch.object(manager, "stop_cluster") as stop, \
                patch.object(manager, "_wait_for_cluster_init", return_value=True), \
                patch("bijuty.big_data_manager.run_bash_command", return_value=ok):
            assert manager.start_cluster() is True

        stop.assert_called_once()

    def test_returns_false_when_start_command_fails(self, manager, monkeypatch):
        monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark/conf")
        failed = subprocess.CompletedProcess(args=[], returncode=1,
                                             stdout="", stderr="boom")

        with patch.object(manager, "is_cluster_up", return_value=False), \
                patch("bijuty.big_data_manager.run_bash_command", return_value=failed):
            assert manager.start_cluster() is False

    def test_returns_false_when_cluster_never_initializes(self, manager, monkeypatch):
        monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark/conf")
        ok = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

        with patch.object(manager, "is_cluster_up", return_value=False), \
                patch.object(manager, "_wait_for_cluster_init", return_value=False), \
                patch("bijuty.big_data_manager.run_bash_command", return_value=ok):
            assert manager.start_cluster() is False


class TestStopCluster:
    def test_returns_one_when_not_initialized(self, fake_slurm):
        manager = BigDataManager(slurm_info=fake_slurm)

        assert manager.stop_cluster() == 1

    def test_stops_gracefully(self, manager, monkeypatch):
        monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark/conf")

        with patch.object(manager, "_wait_for_cluster_stop", return_value=True), \
                patch("bijuty.big_data_manager.run_bash_command") as run:
            assert manager.stop_cluster() is True

        run.assert_called_once()

    def test_cleanup_on_stop_timeout(self, manager, monkeypatch):
        monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark/conf")

        with patch.object(manager, "_wait_for_cluster_stop", return_value=False), \
                patch.object(manager, "_cleanup_cluster") as cleanup, \
                patch("bijuty.big_data_manager.run_bash_command"):
            assert manager.stop_cluster() is False

        cleanup.assert_called_once()

    def test_returns_false_when_command_raises(self, manager, monkeypatch):
        monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark/conf")

        with patch("bijuty.big_data_manager.run_bash_command",
                   side_effect=OSError("nope")):
            assert manager.stop_cluster() is False
