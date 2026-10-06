"""Unit tests for :mod:`bijuty.monitoring.spark`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from bijuty.monitoring.spark import (
    DEFAULT_HISTORY_SIZE,
    SparkMetricCollector,
    SparkMetricsHistory,
    SparkMetricsSnapshot,
    SparkMetricMonitor,
)


# =============================================================================
# Data classes
# =============================================================================


class TestSparkMetricsDataClasses:
    def test_history_preallocates_and_appends(self):
        history = SparkMetricsHistory()
        snapshot = SparkMetricsSnapshot(
            active_jobs=1, completed_jobs=2, failed_jobs=0,
            active_stages=1, completed_stages=2, failed_stages=0,
            active_tasks=3, completed_tasks=4, failed_tasks=0,
            executor_count=2, total_cores=8, total_memory_mb=10.0,
            total_disk_usage_mb=1.0, total_input_mb=2.0,
            total_shuffle_read_mb=3.0, total_shuffle_write_mb=4.0,
            total_gc_time_ms=5.0, total_duration_ms=6.0,
            jvm_heap_used_mb=7.0, timestamp="12:00:00")

        assert len(history.active_jobs) == DEFAULT_HISTORY_SIZE

        history.append(snapshot)

        assert history.active_jobs[-1] == 1
        assert history.total_memory_mb[-1] == 10.0
        assert history.jvm_heap_used_mb[-1] == 7.0
        assert history.timestamp[-1] == "12:00:00"
        assert len(history.active_jobs) == DEFAULT_HISTORY_SIZE


# =============================================================================
# Collector
# =============================================================================


class TestSparkGet:
    def test_get_builds_url_and_returns_json(self):
        collector = SparkMetricCollector(base_url="http://host:4040")
        response = MagicMock()
        response.json.return_value = {"ok": True}

        with patch("bijuty.monitoring.spark.requests.get",
                   return_value=response) as get:
            result = collector._get("/applications")

        assert result == {"ok": True}
        get.assert_called_once_with(
            "http://host:4040/api/v1/applications", timeout=10)
        response.raise_for_status.assert_called_once()


class TestFetchCurrentAppId:
    def test_picks_first_incomplete_application(self):
        collector = SparkMetricCollector(base_url="http://host:4040")
        apps = [
            {"id": "app-1", "attempts": [{"completed": True}]},
            {"id": "app-2", "attempts": [{"completed": False}]},
        ]

        with patch.object(collector, "_get", return_value=apps):
            assert collector._fetch_current_app_id() == "app-2"

    def test_falls_back_to_last_application(self):
        collector = SparkMetricCollector(base_url="http://host:4040")
        apps = [
            {"id": "app-1", "attempts": [{"completed": True}]},
            {"id": "app-2", "attempts": [{"completed": True}]},
        ]

        with patch.object(collector, "_get", return_value=apps):
            assert collector._fetch_current_app_id() == "app-2"

    def test_returns_none_for_empty_application_list(self):
        collector = SparkMetricCollector(base_url="http://host:4040")

        with patch.object(collector, "_get", return_value=[]):
            assert collector._fetch_current_app_id() is None

    def test_returns_none_on_error(self):
        collector = SparkMetricCollector(base_url="http://host:4040")

        with patch.object(collector, "_get", side_effect=Exception("boom")):
            assert collector._fetch_current_app_id() is None


class TestComputeSnapshot:
    def _collector(self):
        return SparkMetricCollector(base_url="http://host:4040")

    def _fake_get(self, jobs, stages, executors):
        def _get(endpoint):
            if endpoint.endswith("/jobs"):
                return jobs
            if endpoint.endswith("/stages"):
                return stages
            if endpoint.endswith("/executors"):
                return executors
            raise AssertionError(f"unexpected endpoint {endpoint}")
        return _get

    def test_computes_counts_and_unit_conversions(self):
        collector = self._collector()
        jobs = [
            {"status": "RUNNING", "numActiveTasks": 2,
             "numCompletedTasks": 1, "numFailedTasks": 0},
            {"status": "SUCCEEDED", "numActiveTasks": 0,
             "numCompletedTasks": 3, "numFailedTasks": 0},
            {"status": "FAILED", "numActiveTasks": 0,
             "numCompletedTasks": 0, "numFailedTasks": 1},
        ]
        stages = [
            {"status": "ACTIVE"}, {"status": "COMPLETE"},
            {"status": "COMPLETE"}, {"status": "FAILED"},
        ]
        mib = 1024 ** 2
        executors = [
            {"totalCores": 4, "memoryUsed": 2 * mib, "diskUsed": 1 * mib,
             "totalInputBytes": 3 * mib, "totalShuffleRead": 4 * mib,
             "totalShuffleWrite": 5 * mib, "totalGCTime": 60,
             "totalDuration": 120,
             "peakMemoryMetrics": {"JVMHeapMemory": 6 * mib}},
            {"totalCores": 4, "memoryUsed": 2 * mib, "diskUsed": 0,
             "totalInputBytes": 0, "totalShuffleRead": 0,
             "totalShuffleWrite": 0, "totalGCTime": 40,
             "totalDuration": 80, "memoryMetrics": {}},
        ]

        collector._get = self._fake_get(jobs, stages, executors)
        snapshot = collector._compute_snapshot("app-1")

        assert snapshot is not None
        assert snapshot.active_jobs == 1
        assert snapshot.completed_jobs == 1
        assert snapshot.failed_jobs == 1
        assert snapshot.active_stages == 1
        assert snapshot.completed_stages == 2
        assert snapshot.failed_stages == 1
        assert snapshot.active_tasks == 2
        assert snapshot.completed_tasks == 4
        assert snapshot.failed_tasks == 1
        assert snapshot.executor_count == 2
        assert snapshot.total_cores == 8
        assert snapshot.total_memory_mb == 4.0
        assert snapshot.total_disk_usage_mb == 1.0
        assert snapshot.total_input_mb == 3.0
        assert snapshot.total_shuffle_read_mb == 4.0
        assert snapshot.total_shuffle_write_mb == 5.0
        assert snapshot.total_gc_time_ms == 100
        assert snapshot.total_duration_ms == 200
        assert snapshot.jvm_heap_used_mb == 6.0

    def test_falls_back_to_on_heap_storage_metric(self):
        collector = self._collector()
        mib = 1024 ** 2
        executors = [
            {"totalCores": 1, "memoryUsed": 0, "diskUsed": 0,
             "totalInputBytes": 0, "totalShuffleRead": 0,
             "totalShuffleWrite": 0, "totalGCTime": 0, "totalDuration": 0,
             "memoryMetrics": {"usedOnHeapStorageMemory": 8 * mib}},
        ]

        collector._get = self._fake_get([], [], executors)
        snapshot = collector._compute_snapshot("app-1")

        assert snapshot.jvm_heap_used_mb == 8.0

    def test_returns_none_when_api_call_fails(self):
        collector = self._collector()

        with patch.object(collector, "_get", side_effect=Exception("boom")):
            assert collector._compute_snapshot("app-1") is None


class TestSparkCollect:
    def test_collect_builds_history_and_result(self):
        collector = SparkMetricCollector(base_url="http://host:4040")
        snapshot = MagicMock(active_jobs=3, timestamp="12:00:00")

        with patch.object(collector, "_fetch_current_app_id", return_value="app-1"), \
                patch.object(collector, "_compute_snapshot", return_value=snapshot):
            results = collector.collect()

        assert results["app-1"]["found"] is True
        assert results["app-1"]["app_id"] == "app-1"
        assert collector.history["app-1"].active_jobs[-1] == 3

    def test_collect_returns_empty_without_application(self):
        collector = SparkMetricCollector(base_url="http://host:4040")

        with patch.object(collector, "_fetch_current_app_id", return_value=None):
            assert collector.collect() == {}

    def test_collect_resets_app_id_when_snapshot_missing(self):
        collector = SparkMetricCollector(base_url="http://host:4040")
        collector._current_app_id = "stale"

        with patch.object(collector, "_compute_snapshot", return_value=None):
            assert collector.collect() == {}

        assert collector._current_app_id is None


# =============================================================================
# Monitor
# =============================================================================


class TestSparkMetricMonitor:
    def test_metric_display_names(self):
        assert SparkMetricMonitor._get_metric_display_name(object(), "active_jobs") \
            == "Active Jobs"
        assert SparkMetricMonitor._get_metric_display_name(object(), "unknown") \
            == "unknown"

    def test_set_monitor_with_base_url(self):
        monitor = SparkMetricMonitor(base_url="http://old:4040")

        monitor.set_monitor(base_url="http://new:4040")

        assert monitor._base_url == "http://new:4040"
        assert monitor.collector._base_url == "http://new:4040"

    def test_set_monitor_with_user_input_host(self):
        monitor = SparkMetricMonitor(base_url="http://old:4040")
        user_input = MagicMock(master="node9")

        monitor.set_monitor(user_input=user_input)

        assert monitor._base_url == "http://node9:4040"

    def test_set_monitor_requires_url_or_user_input(self):
        monitor = SparkMetricMonitor(base_url="http://old:4040")

        try:
            monitor.set_monitor()
        except Exception as exc:  # noqa: BLE001 - asserting the public contract
            assert "base url" in str(exc)
        else:  # pragma: no cover - defensive
            raise AssertionError("set_monitor should raise without arguments")
