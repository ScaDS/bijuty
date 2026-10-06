"""Unit tests for :mod:`bijuty.monitoring.process`."""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import psutil

from bijuty.monitoring.process import (
    BYTES_TO_MB,
    DEFAULT_HISTORY_SIZE,
    ProcessMetricCollector,
    ProcessMetricsHistory,
    ProcessMetricsSnapshot,
    ProcessMonitor,
)

from tests.conftest import FakePsutilProcess


# =============================================================================
# Data classes
# =============================================================================


class TestProcessMetricsSnapshotAndHistory:
    def test_snapshot_stores_all_fields(self):
        snapshot = ProcessMetricsSnapshot(
            cpu_percent=1.0, memory_percent=2.0, memory_rss_mb=3.0,
            memory_vms_mb=4.0, num_threads=5, io_read_mb=6.0,
            io_write_mb=7.0, timestamp="12:00:00")

        assert snapshot.cpu_percent == 1.0
        assert snapshot.timestamp == "12:00:00"

    def test_history_preallocates_fixed_size_deques(self):
        history = ProcessMetricsHistory()

        for metric in ("cpu", "mem_pct", "mem_rss", "mem_vms",
                       "threads", "io_read", "io_write", "timestamp"):
            deque = getattr(history, metric)
            assert len(deque) == DEFAULT_HISTORY_SIZE
            assert deque.maxlen == DEFAULT_HISTORY_SIZE

    def test_history_append_rolls_window(self):
        history = ProcessMetricsHistory()
        snapshot = ProcessMetricsSnapshot(
            cpu_percent=99.0, memory_percent=1.0, memory_rss_mb=2.0,
            memory_vms_mb=3.0, num_threads=4, io_read_mb=5.0,
            io_write_mb=6.0, timestamp="12:00:00")

        history.append(snapshot)

        assert history.cpu[-1] == 99.0
        assert history.threads[-1] == 4
        assert history.timestamp[-1] == "12:00:00"
        assert len(history.cpu) == DEFAULT_HISTORY_SIZE


# =============================================================================
# Collector
# =============================================================================


class TestProcessMetricCollectorInit:
    def test_normalises_string_patterns(self):
        collector = ProcessMetricCollector(["spark-master"])

        assert collector.process_names == [
            {"title": "spark-master", "pattern": "spark-master"}]

    def test_keeps_dict_patterns_and_builds_history(self):
        collector = ProcessMetricCollector([
            {"title": "Master", "pattern": "master-pattern"},
            {"pattern": "no-title"},
        ])

        assert list(collector.history) == ["Master", "no-title"]

    def test_none_process_names_defaults_to_empty(self):
        collector = ProcessMetricCollector()

        assert collector.process_names == []
        assert collector.history == {}

    def test_history_size_is_configurable(self):
        collector = ProcessMetricCollector(["x"], history_size=5)

        assert collector._history_size == 5


class TestMatchProcess:
    def _collector(self):
        return ProcessMetricCollector(
            [{"title": "Master", "pattern": "master-pattern"}])

    def test_matches_process_for_current_user(self):
        collector = self._collector()
        proc = FakePsutilProcess(
            username=collector.user,
            cmdline=["java", "-cp", "master-pattern", "--host"])

        assert collector._match_process(proc) == {
            "title": "Master", "pattern": "master-pattern"}

    def test_ignores_zombie_process(self):
        collector = self._collector()
        proc = FakePsutilProcess(
            username=collector.user, status=psutil.STATUS_ZOMBIE,
            cmdline=["java", "master-pattern"])

        assert collector._match_process(proc) is None

    def test_ignores_other_users(self):
        collector = self._collector()
        proc = FakePsutilProcess(
            username="someone-else", cmdline=["java", "master-pattern"])

        assert collector._match_process(proc) is None

    def test_ignores_non_matching_cmdline(self):
        collector = self._collector()
        proc = FakePsutilProcess(
            username=collector.user, cmdline=["java", "unrelated"])

        assert collector._match_process(proc) is None

    def test_returns_none_when_status_access_denied(self):
        collector = self._collector()
        proc = MagicMock()
        proc.status.side_effect = psutil.AccessDenied(pid=1)

        assert collector._match_process(proc) is None


class TestExtractMetrics:
    def test_converts_bytes_to_megabytes(self):
        collector = ProcessMetricCollector(["x"])
        proc = FakePsutilProcess(
            cpu_percent=42.0,
            rss=64 * BYTES_TO_MB,
            vms=128 * BYTES_TO_MB,
            memory_percent=7.5,
            num_threads=11,
            read_bytes=3 * BYTES_TO_MB,
            write_bytes=4 * BYTES_TO_MB,
        )

        snapshot = collector._extract_metrics(proc)

        assert snapshot is not None
        assert snapshot.cpu_percent == 42.0
        assert snapshot.memory_percent == 7.5
        assert snapshot.memory_rss_mb == 64.0
        assert snapshot.memory_vms_mb == 128.0
        assert snapshot.num_threads == 11
        assert snapshot.io_read_mb == 3.0
        assert snapshot.io_write_mb == 4.0
        assert re.fullmatch(r"\d{2}:\d{2}:\d{2}", snapshot.timestamp)

    def test_returns_none_on_access_denied(self):
        collector = ProcessMetricCollector(["x"])
        proc = MagicMock()
        proc.oneshot.return_value.__enter__.return_value = None
        proc.cpu_percent.side_effect = psutil.AccessDenied(pid=1)

        assert collector._extract_metrics(proc) is None


class TestCollect:
    def test_empty_process_names_returns_empty_dict(self):
        assert ProcessMetricCollector().collect() == {}

    def test_collect_records_matching_process(self):
        collector = ProcessMetricCollector(
            [{"title": "Master", "pattern": "master-pattern"}])
        proc = FakePsutilProcess(
            pid=77, username=collector.user,
            cmdline=["java", "master-pattern"], cpu_percent=5.0)

        with patch("bijuty.monitoring.process.psutil.process_iter",
                   return_value=[proc]):
            results = collector.collect()

        assert results["Master"]["found"] is True
        assert results["Master"]["history"] is collector.history["Master"]
        assert collector.history["Master"].cpu[-1] == 5.0


# =============================================================================
# Monitor (widget layer exercised without a kernel)
# =============================================================================


class TestProcessMonitor:
    def test_metric_display_names(self):
        assert ProcessMonitor._get_metric_display_name(object(), "cpu") == \
            "CPU Usage (%)"
        assert ProcessMonitor._get_metric_display_name(object(), "unknown") == \
            "unknown"

    def test_set_process_names_rebuilds_collector(self, fake_slurm):
        monitor = ProcessMonitor(slurm_info=fake_slurm, process_names=["a"])

        monitor.set_process_names([{"title": "B", "pattern": "b"}])

        assert monitor.process_names == [{"title": "B", "pattern": "b"}]
        assert list(monitor.collector.history) == ["B"]

    def test_render_metrics_noop_for_empty_payload(self, fake_slurm):
        monitor = ProcessMonitor(slurm_info=fake_slurm, process_names=["a"])

        # Should simply return without raising.
        monitor._render_metrics({})

    def test_render_metrics_updates_existing_plot(self, fake_slurm):
        monitor = ProcessMonitor(slurm_info=fake_slurm, process_names=["a"])
        history = ProcessMetricsHistory()
        history.cpu[-1] = 12.0
        monitor._process_plots["Master"] = {"latest_data": None}

        with patch.object(monitor, "_update_plot") as update:
            monitor._render_metrics({
                "Master": {"found": True, "history": history,
                           "proc_info": object()},
            })

        update.assert_called_once()
        assert monitor._process_plots["Master"]["latest_data"] is not None
