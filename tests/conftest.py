"""Shared pytest fixtures for the ``bijuty`` test suite."""

from __future__ import annotations

import os
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from bijuty.gui.config import FRAMEWORK_REGISTRY
from bijuty.slurm_utils import JobResources


# =============================================================================
# Environment isolation
# =============================================================================


@pytest.fixture(autouse=True)
def isolate_slurm_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove SLURM/framework variables so tests cannot leak into each other."""

    prefixes = ("SLURM_", "PYFLINK", "SPARK_", "FLINK_")
    for key in list(os.environ):
        if key.startswith(prefixes):
            monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("USER", os.environ.get("USER", "tester"))


# =============================================================================
# SLURM stand-ins
# =============================================================================


@pytest.fixture
def fake_resources() -> JobResources:
    """A deterministic two-node SLURM job resource description."""

    return JobResources(
        node_list=["node1", "node2"],
        cpus_per_task=4,
        tasks_per_node=1,
        memory_per_cpu=2000,
        memory_per_node=8000,
    )


class FakeSlurm:
    """Minimal duck-typed replacement for :class:`SlurmManager`."""

    def __init__(self, resources: JobResources | None = None):
        self.resources = resources or JobResources(
            node_list=["node1", "node2"],
            cpus_per_task=4,
            tasks_per_node=1,
            memory_per_cpu=2000,
            memory_per_node=8000,
        )
        self.in_slurm_job = True
        self.user = "tester"
        self.job_id = "12345"
        self.login_node = "login1.example.org"
        self._start_time = "1000"
        self._partition = "batch"
        self.start_time = "1000"
        self.partition = "batch"

    def __repr__(self) -> str:  # pragma: no cover - debugging helper only
        return f"FakeSlurm(job_id={self.job_id})"


@pytest.fixture
def fake_slurm(fake_resources: JobResources) -> FakeSlurm:
    """Provide a deterministic :class:`FakeSlurm` instance."""

    return FakeSlurm(fake_resources)


@pytest.fixture
def framework_registry() -> Dict[str, Any]:
    """Expose the real framework registry for configuration assertions."""

    return FRAMEWORK_REGISTRY


# =============================================================================
# Metric helpers
# =============================================================================


def make_process_snapshot(**overrides: Any):
    """Build a :class:`ProcessMetricsSnapshot` with sensible defaults."""

    from bijuty.monitoring.process import ProcessMetricsSnapshot

    data = dict(
        cpu_percent=12.5,
        memory_percent=3.0,
        memory_rss_mb=64.0,
        memory_vms_mb=128.0,
        num_threads=8,
        io_read_mb=1.5,
        io_write_mb=2.5,
        timestamp=datetime.now().strftime("%H:%M:%S"),
    )
    data.update(overrides)
    return ProcessMetricsSnapshot(**data)


@pytest.fixture
def process_snapshot_factory():
    """Return a factory fixture for process metric snapshots."""

    return make_process_snapshot


# =============================================================================
# Process doubles
# =============================================================================


class FakePsutilProcess:
    """A scriptable stand-in for :class:`psutil.Process`."""

    def __init__(
        self,
        pid: int = 4242,
        name: str = "java",
        username: str = "tester",
        cmdline: List[str] | None = None,
        status: str = "running",
        rss: int = 64 * 1024 * 1024,
        vms: int = 128 * 1024 * 1024,
        cpu_percent: float = 12.5,
        memory_percent: float = 3.0,
        num_threads: int = 8,
        read_bytes: int = 1024 * 1024,
        write_bytes: int = 2 * 1024 * 1024,
    ):
        self.pid = pid
        self.info = {"pid": pid, "name": name, "username": username,
                     "cmdline": cmdline if cmdline is not None else [name]}
        self._name = name
        self._username = username
        self._cmdline = list(cmdline) if cmdline is not None else [name]
        self._status = status
        self._rss = rss
        self._vms = vms
        self._cpu_percent = cpu_percent
        self._memory_percent = memory_percent
        self._num_threads = num_threads
        self._read_bytes = read_bytes
        self._write_bytes = write_bytes

    # -- context manager -------------------------------------------------
    def oneshot(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    # -- psutil.Process API ---------------------------------------------
    def status(self) -> str:
        return self._status

    def username(self) -> str:
        return self._username

    def cmdline(self) -> List[str]:
        return self._cmdline

    def cpu_percent(self, interval: float | None = None) -> float:
        return self._cpu_percent

    def memory_info(self) -> SimpleNamespace:
        return SimpleNamespace(rss=self._rss, vms=self._vms)

    def memory_percent(self) -> float:
        return self._memory_percent

    def num_threads(self) -> int:
        return self._num_threads

    def io_counters(self) -> SimpleNamespace:
        return SimpleNamespace(read_bytes=self._read_bytes,
                               write_bytes=self._write_bytes)


@pytest.fixture
def fake_psutil_process() -> FakePsutilProcess:
    return FakePsutilProcess()
