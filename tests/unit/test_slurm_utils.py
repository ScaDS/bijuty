"""Unit tests for :mod:`bijuty.slurm_utils`."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from unittest.mock import mock_open, patch

import pytest

from bijuty.slurm_utils import (
    JobResources,
    SlurmManager,
    get_local_cpu_count,
    get_local_memory_mb,
)


# =============================================================================
# Module-level helpers
# =============================================================================


class TestLocalHostHelpers:
    def test_get_local_cpu_count_returns_int(self):
        assert get_local_cpu_count() >= 1

    def test_get_local_cpu_count_defaults_to_one_when_unknown(self):
        with patch("bijuty.slurm_utils.os.cpu_count", return_value=None):
            assert get_local_cpu_count() == 1

    def test_get_local_memory_mb_parses_memavailable(self):
        meminfo = "MemTotal:       1000000 kB\nMemAvailable:   2097152 kB\n"

        with patch("bijuty.slurm_utils.open", mock_open(read_data=meminfo)):
            assert get_local_memory_mb() == 2048

    def test_get_local_memory_mb_returns_zero_without_memavailable(self):
        meminfo = "MemTotal: 1000000 kB\n"

        with patch("bijuty.slurm_utils.open", mock_open(read_data=meminfo)):
            assert get_local_memory_mb() == 0

    def test_get_local_memory_mb_raises_when_meminfo_unreadable(self):
        with patch("bijuty.slurm_utils.open", side_effect=OSError("no procfs")):
            with pytest.raises(Exception, match="MemAvailable"):
                get_local_memory_mb()


# =============================================================================
# JobResources
# =============================================================================


class TestJobResources:
    def test_derived_properties_with_explicit_memory_per_node(self, fake_resources):
        assert fake_resources.cpus_per_node == 4
        assert fake_resources.total_cpus == 8
        assert fake_resources.memory_per_node_effective == 8000
        assert fake_resources.total_memory == 16000
        assert fake_resources.node_count == 2

    def test_memory_per_node_computed_from_memory_per_cpu(self):
        resources = JobResources(
            node_list=["a"], cpus_per_task=2, tasks_per_node=3,
            memory_per_cpu=512)

        assert resources.memory_per_node_effective == 512 * 6
        assert resources.total_memory == 512 * 6

    def test_empty_node_list_yields_zero_totals(self):
        resources = JobResources(
            node_list=[], cpus_per_task=4, tasks_per_node=1,
            memory_per_cpu=1000)

        assert resources.node_count == 0
        assert resources.total_cpus == 0
        assert resources.total_memory == 0


# =============================================================================
# SlurmManager construction
# =============================================================================


class TestSlurmManagerDetection:
    @pytest.mark.parametrize("value", ["1234", "0", "999999"])
    def test_is_in_slurm_job_true_for_numeric_job_id(self, monkeypatch, value):
        monkeypatch.setenv("SLURM_JOB_ID", value)

        assert SlurmManager._is_in_slurm_job(object()) is True

    @pytest.mark.parametrize("value", ["", "localhost", "abc123"])
    def test_is_in_slurm_job_false_for_non_numeric_job_id(self, monkeypatch, value):
        monkeypatch.setenv("SLURM_JOB_ID", value)

        assert SlurmManager._is_in_slurm_job(object()) is False

    def test_is_in_slurm_job_accepts_legacy_jobid(self, monkeypatch):
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)
        monkeypatch.setenv("SLURM_JOBID", "4242")

        assert SlurmManager._is_in_slurm_job(object()) is True

    def test_raises_outside_slurm_without_opt_in(self):
        with pytest.raises(Exception, match="No active SLURM job"):
            SlurmManager()

    def test_local_mode_when_outside_job_allowed(self):
        manager = SlurmManager(allow_outside_job=True)

        assert manager.in_slurm_job is False
        assert manager.job_id == "localhost"
        assert manager.job_info["partition"] == "local"
        assert manager.resources.node_list  # falls back to hostname


class TestSlurmManagerInsideJob:
    def _scontrol_payload(self, job_id=12345):
        return {
            "jobs": [
                {
                    "job_id": job_id,
                    "name": "test-job",
                    "partition": "batch",
                    "user_name": "tester",
                    "node_count": {"set": True, "infinite": False, "number": 2},
                    "cpus_per_task": {"set": True, "infinite": False, "number": 4},
                    "tasks": {"set": True, "infinite": False, "number": 1},
                    "memory_per_cpu": {"set": True, "infinite": False, "number": 2000},
                    "memory_per_node": {"set": True, "infinite": False, "number": 8192},
                    "start_time": {"set": True, "infinite": False,
                                   "number": "2026-01-01T00:00:00"},
                    "job_resources": {"nodes": ["nodeA", "nodeB"]},
                    "nodes": "nodeA,nodeB",
                }
            ]
        }

    def _manager(self, monkeypatch, payload=None):
        monkeypatch.setenv("SLURM_JOB_ID", "12345")
        monkeypatch.setenv("USER", "tester")
        completed = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=json.dumps(payload or self._scontrol_payload()),
            stderr="")
        with patch("bijuty.slurm_utils.run_bash_command", return_value=completed) as run:
            manager = SlurmManager()
        return manager, run

    def test_reads_job_info_and_resources(self, monkeypatch):
        manager, run = self._manager(monkeypatch)

        assert manager.in_slurm_job is True
        assert manager.job_id == "12345"
        assert manager.job_info["name"] == "test-job"
        assert manager.resources.node_list == ["nodeA", "nodeB"]
        assert manager.resources.memory_per_node == 8192
        # scontrol is preferred when a job id is known.
        argv = run.call_args[0][0]
        assert argv[:2] == ["scontrol", "show"]

    def test_fetch_job_info_raises_on_nonzero_returncode(self, monkeypatch):
        monkeypatch.setenv("SLURM_JOB_ID", "12345")
        completed = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="scontrol: error")

        with patch("bijuty.slurm_utils.run_bash_command", return_value=completed):
            with pytest.raises(RuntimeError, match="scontrol: error"):
                SlurmManager()

    def test_fetch_job_info_raises_on_invalid_json(self, monkeypatch):
        monkeypatch.setenv("SLURM_JOB_ID", "12345")
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="not-json", stderr="")

        with patch("bijuty.slurm_utils.run_bash_command", return_value=completed):
            with pytest.raises(Exception, match="invalid JSON"):
                SlurmManager()


# =============================================================================
# SlurmManager helpers
# =============================================================================


class TestSlurmManagerHelpers:
    def test_get_nodes_list_handles_string_node(self):
        fake_self = SimpleNamespace(
            _job_info={"job_resources": {"nodes": "nodeA"}})

        assert SlurmManager._get_nodes_list(fake_self) == ["nodeA"]

    def test_get_nodes_list_handles_missing_job_info(self):
        fake_self = SimpleNamespace(_job_info={})

        assert SlurmManager._get_nodes_list(fake_self) == []

    def test_get_nodes_list_handles_empty_string(self):
        fake_self = SimpleNamespace(
            _job_info={"job_resources": {"nodes": ""}})

        assert SlurmManager._get_nodes_list(fake_self) == []

    def test_get_nodes_list_handles_modern_dict_with_allocation(self):
        fake_self = SimpleNamespace(_job_info={
            "job_resources": {
                "nodes": {
                    "count": 2,
                    "list": "n[1135-1136]",
                    "allocation": [
                        {"index": 0, "name": "n1135"},
                        {"index": 1, "name": "n1136"},
                    ],
                }
            }
        })

        assert SlurmManager._get_nodes_list(fake_self) == ["n1135", "n1136"]

    def test_get_nodes_list_handles_modern_dict_list_only(self):
        fake_self = SimpleNamespace(_job_info={
            "job_resources": {"nodes": {"count": 1, "list": "n1135"}}
        })

        assert SlurmManager._get_nodes_list(fake_self) == ["n1135"]

    def test_get_nodes_list_expands_compact_nodelist(self):
        fake_self = SimpleNamespace(_job_info={
            "job_resources": {"nodes": {"list": "node[01-03]"}}
        })

        assert SlurmManager._get_nodes_list(fake_self) == \
            ["node01", "node02", "node03"]

    def test_get_nodes_list_expands_mixed_nodelist(self):
        fake_self = SimpleNamespace(_job_info={
            "job_resources": {"nodes": {"list": "n[1135-1136,1140]"}}
        })

        assert SlurmManager._get_nodes_list(fake_self) == \
            ["n1135", "n1136", "n1140"]

    def test_get_nodes_list_handles_none(self):
        fake_self = SimpleNamespace(
            _job_info={"job_resources": {"nodes": None}})

        assert SlurmManager._get_nodes_list(fake_self) == []

    def test_default_login_host_derived_from_fqdn(self):
        with patch("bijuty.slurm_utils.socket.getfqdn",
                   return_value="node01.cluster.tu-dresden.de"):
            assert SlurmManager._get_default_login_host(object()) == \
                "login1.cluster.tu-dresden.de"

    def test_default_login_host_falls_back_to_localhost(self):
        with patch("bijuty.slurm_utils.socket.getfqdn", return_value="myhost"):
            assert SlurmManager._get_default_login_host(object()) == "localhost"

    def test_local_mode_sets_missing_slurm_env_context(self, monkeypatch):
        manager = SlurmManager(allow_outside_job=True)

        assert manager  # constructed
        import os
        assert os.environ["SLURM_CPUS_PER_NODE"] == str(
            manager.resources.cpus_per_node)
        assert os.environ["SLURM_MEM_TOTAL"] == str(manager.resources.total_memory)

    def test_repr_contains_job_id_and_status(self):
        manager = SlurmManager(allow_outside_job=True)

        text = repr(manager)
        assert "SlurmManager" in text
        assert "Inactive" in text
