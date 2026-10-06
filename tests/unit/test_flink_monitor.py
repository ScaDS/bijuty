"""Unit tests for :mod:`bijuty.monitoring.flink`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from bijuty.monitoring.flink import (
    DEFAULT_HISTORY_SIZE,
    FlinkMetricCollector,
    FlinkMetricsHistory,
    FlinkMetricsSnapshot,
    FlinkMetricMonitor,
)


# =============================================================================
# Data classes
# =============================================================================


class TestFlinkMetricsDataClasses:
    def test_history_preallocates_and_appends(self):
        history = FlinkMetricsHistory()
        snapshot = FlinkMetricsSnapshot(
            total_jobs=1, running_jobs=1, failed_jobs=0, finished_jobs=0,
            cancelled_jobs=0, total_tasks=3, running_tasks=3, failed_tasks=0,
            finished_tasks=0, cancelled_tasks=0, total_slots=4, used_slots=3,
            free_slots=1, task_managers=1, total_memory_mb=10.0,
            total_network_memory_mb=2.0, total_jvm_memory_mb=8.0,
            timestamp=123.0)

        assert len(history.total_jobs) == DEFAULT_HISTORY_SIZE

        history.append(snapshot)

        assert history.total_jobs[-1] == 1
        assert history.free_slots[-1] == 1
        assert history.timestamp[-1] == 123.0
        assert len(history.total_jobs) == DEFAULT_HISTORY_SIZE


# =============================================================================
# Collector URL handling
# =============================================================================


class TestResolveUrl:
    def test_provided_url_is_stripped_of_trailing_slash(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081/", slurm_info=fake_slurm)

        assert collector._base_url == "http://host:8081"

    def test_prefers_jupyterhub_proxy_when_reachable(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

        with patch.object(collector, "_check_url",
                          side_effect=lambda url: "jupyterhub" in url):
            resolved = collector._resolve_url(None)

        assert resolved == (
            "https://jupyterhub.hpc.tu-dresden.de/user/tester/proxy/8081")

    def test_falls_back_to_localhost(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

        with patch.object(collector, "_check_url",
                          side_effect=lambda url: url.startswith("http://localhost")):
            resolved = collector._resolve_url(None)

        assert resolved == "http://localhost:8081"

    def test_falls_back_to_first_allocated_node(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

        with patch.object(collector, "_check_url",
                          side_effect=lambda url: "node1" in url):
            resolved = collector._resolve_url(None)

        assert resolved == "http://node1:8081"

    def test_final_fallback_is_localhost(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

        with patch.object(collector, "_check_url", return_value=False):
            resolved = collector._resolve_url(None)

        assert resolved == "http://localhost:8081"


class TestCheckUrl:
    def test_true_on_successful_response(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

        with patch("bijuty.monitoring.flink.requests.get") as get:
            assert collector._check_url("http://host:8081") is True

        get.assert_called_once_with("http://host:8081/overview", timeout=2)

    def test_false_on_request_exception(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

        with patch("bijuty.monitoring.flink.requests.get",
                   side_effect=Exception("nope")):
            assert collector._check_url("http://host:8081") is False


class TestFlinkGetAndJobs:
    def test_get_builds_url(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)
        response = MagicMock()
        response.json.return_value = {"jobs": []}

        with patch("bijuty.monitoring.flink.requests.get",
                   return_value=response) as get:
            result = collector._get("/jobs")

        assert result == {"jobs": []}
        get.assert_called_once_with("http://host:8081/jobs", timeout=10)
        response.raise_for_status.assert_called_once()

    def test_fetch_running_job_id(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)
        payload = {"jobs": [
            {"id": "j1", "status": "FINISHED"},
            {"id": "j2", "status": "RUNNING"},
        ]}

        with patch.object(collector, "_get", return_value=payload):
            assert collector._fetch_running_job_id() == "j2"

    def test_fetch_running_job_id_none_and_error(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

        with patch.object(collector, "_get", return_value={"jobs": []}):
            assert collector._fetch_running_job_id() is None

        with patch.object(collector, "_get", side_effect=Exception("boom")):
            assert collector._fetch_running_job_id() is None


class TestComputeSnapshot:
    def _collector(self, fake_slurm):
        return FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)

    def _fake_get(self, overview, jobs, taskmanagers):
        def _get(endpoint):
            return {
                "/overview": overview,
                "/jobs": jobs,
                "/taskmanagers": taskmanagers,
            }[endpoint]
        return _get

    def test_computes_snapshot(self, fake_slurm):
        collector = self._collector(fake_slurm)
        mib = 1024 ** 2
        overview = {
            "tasks": {"total": 5, "running": 2, "failed": 1,
                      "finished": 1, "canceled": 1},
            "slots-total": 4, "slots-used": 1,
            "total-memory": 10 * mib, "total-network-memory": 2 * mib,
            "total-jvm-memory": 8 * mib,
        }
        jobs = {"jobs": [
            {"status": "RUNNING"}, {"status": "FINISHED"},
            {"status": "FAILED"}, {"status": "CANCELLED"},
        ]}
        taskmanagers = {"taskmanagers": [{"id": 1}, {"id": 2}]}

        collector._get = self._fake_get(overview, jobs, taskmanagers)
        snapshot = collector._compute_snapshot()

        assert snapshot is not None
        assert snapshot.total_jobs == 4
        assert snapshot.running_jobs == 1
        assert snapshot.failed_jobs == 1
        assert snapshot.finished_jobs == 1
        assert snapshot.cancelled_jobs == 1
        assert snapshot.total_tasks == 5
        assert snapshot.running_tasks == 2
        assert snapshot.failed_tasks == 1
        assert snapshot.finished_tasks == 1
        assert snapshot.cancelled_tasks == 1
        assert snapshot.total_slots == 4
        assert snapshot.used_slots == 1
        assert snapshot.free_slots == 3
        assert snapshot.task_managers == 2
        assert snapshot.total_memory_mb == 10.0
        assert snapshot.total_network_memory_mb == 2.0
        assert snapshot.total_jvm_memory_mb == 8.0

    def test_returns_none_on_error(self, fake_slurm):
        collector = self._collector(fake_slurm)

        with patch.object(collector, "_get", side_effect=Exception("boom")):
            assert collector._compute_snapshot() is None


class TestFlinkCollect:
    def test_collect_uses_running_job_id(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)
        snapshot = MagicMock(total_jobs=7, timestamp=1.0)

        with patch.object(collector, "_fetch_running_job_id", return_value="job-1"), \
                patch.object(collector, "_compute_snapshot", return_value=snapshot):
            results = collector.collect()

        assert results["job-1"]["found"] is True
        assert results["job-1"]["job_id"] == "job-1"
        assert collector.history["job-1"].total_jobs[-1] == 7

    def test_collect_uses_cluster_key_without_running_job(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)
        snapshot = MagicMock(total_jobs=0, timestamp=1.0)

        with patch.object(collector, "_fetch_running_job_id", return_value=None), \
                patch.object(collector, "_compute_snapshot", return_value=snapshot):
            results = collector.collect()

        assert "cluster" in results

    def test_collect_resets_job_id_on_missing_snapshot(self, fake_slurm):
        collector = FlinkMetricCollector(
            base_url="http://host:8081", slurm_info=fake_slurm)
        collector._current_job_id = "stale"

        with patch.object(collector, "_compute_snapshot", return_value=None):
            assert collector.collect() == {}

        assert collector._current_job_id is None


# =============================================================================
# Monitor
# =============================================================================


class TestFlinkMetricMonitor:
    def test_metric_display_names(self):
        assert FlinkMetricMonitor._get_metric_display_name(object(), "total_jobs") \
            == "Total Jobs"
        assert FlinkMetricMonitor._get_metric_display_name(object(), "unknown") \
            == "unknown"

    def test_set_monitor_with_base_url(self, fake_slurm):
        monitor = FlinkMetricMonitor(
            base_url="http://old:8081", slurm_info=fake_slurm)

        monitor.set_monitor(base_url="http://new:8081")

        assert monitor.collector._base_url == "http://new:8081"

    def test_set_monitor_with_user_input_host(self, fake_slurm):
        monitor = FlinkMetricMonitor(
            base_url="http://old:8081", slurm_info=fake_slurm)
        user_input = MagicMock(master="node9")

        monitor.set_monitor(user_input=user_input)

        assert monitor.collector._base_url == "http://node9:8081"

    def test_set_monitor_requires_url_or_user_input(self, fake_slurm):
        monitor = FlinkMetricMonitor(
            base_url="http://old:8081", slurm_info=fake_slurm)

        try:
            monitor.set_monitor()
        except Exception as exc:  # noqa: BLE001 - asserting the public contract
            assert "base url" in str(exc)
        else:  # pragma: no cover - defensive
            raise AssertionError("set_monitor should raise without arguments")
