"""Unit tests for :mod:`bijuty.monitoring.pika`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import ipywidgets as widgets
import pytest

from bijuty.monitoring.pika import (
    MAX_POINTS_PER_PLOT,
    PikaClientLite,
    PikaMetricMonitor,
    data_blocks,
    extract_job_detail,
    extract_timeline_meta,
    iter_timeline_points,
    merge_timeline,
    pick_data_block,
)


# =============================================================================
# HTTP client
# =============================================================================


class TestPikaClientLite:
    def test_sets_accept_and_authorization_headers(self):
        client = PikaClientLite("http://pika", token="secret")

        assert client.session.headers["accept"] == "application/json"
        assert client.session.headers["Authorization"] == "Bearer secret"

    def test_omits_authorization_without_token(self):
        client = PikaClientLite("http://pika")

        assert "Authorization" not in client.session.headers

    def test_mounts_retry_adapters(self):
        client = PikaClientLite("http://pika")

        assert "http://" in client.session.adapters
        assert "https://" in client.session.adapters

    def test_post_uses_default_body_and_raises_for_status(self):
        client = PikaClientLite("http://pika", token="t")
        response = MagicMock()
        response.json.return_value = {"ok": True}

        with patch.object(client.session, "post", return_value=response) as post:
            result = client._post("/timeline/cpu/1/2/p")

        assert result == {"ok": True}
        post.assert_called_once_with(
            "http://pika/timeline/cpu/1/2/p", json={}, timeout=10.0,
            verify=True)
        response.raise_for_status.assert_called_once()

    def test_get_credential(self):
        client = PikaClientLite("http://pika")
        response = MagicMock()
        response.json.return_value = {"user": "tester"}

        with patch.object(client.session, "get", return_value=response) as get:
            assert client.get_credential() == {"user": "tester"}

        get.assert_called_once_with(
            "http://pika/credential", timeout=10.0, verify=True)

    def test_get_job_detail_posts_format_result_false(self):
        client = PikaClientLite("http://pika")
        response = MagicMock()
        response.json.return_value = [{"job_name": "j"}]

        with patch.object(client.session, "post", return_value=response) as post:
            assert client.get_job_detail("42", "100", "batch") == [{"job_name": "j"}]

        post.assert_called_once_with(
            "http://pika/job/42/100/batch", json={"format_result": False},
            timeout=10.0, verify=True)

    def test_get_timeline_default_body(self):
        client = PikaClientLite("http://pika")
        response = MagicMock()

        with patch.object(client.session, "post", return_value=response) as post:
            client.get_timeline("cpu", "1", "2", "p")

        post.assert_called_once_with(
            "http://pika/timeline/cpu/1/2/p", json={"mean_line": True},
            timeout=10.0, verify=True)

    def test_get_timeline_extern_custom_body(self):
        client = PikaClientLite("http://pika")
        response = MagicMock()

        with patch.object(client.session, "post", return_value=response) as post:
            client.get_timeline_extern("cpu", "1", "2", "p", body={"x": 1})

        post.assert_called_once_with(
            "http://pika/timeline_extern/cpu/1/2/p", json={"x": 1},
            timeout=10.0, verify=True)


# =============================================================================
# Timeline parsing helpers
# =============================================================================


class TestDataBlocks:
    def test_yields_valid_data_blocks_with_meta(self):
        raw = {
            "unit": "MB",
            "timestamps": [1, 2],
            "a": [[10, 20], {"mean": 15}],
            "b": "not-a-block",
            "c": [],
            "d": [5, 6],
        }

        blocks = list(data_blocks(raw))

        assert blocks == [("a", [1, 2], [10, 20], {"mean": 15})]

    def test_returns_nothing_for_non_dict(self):
        assert list(data_blocks([1, 2, 3])) == []

    def test_returns_nothing_without_timestamps(self):
        assert list(data_blocks({"a": [[1], {}]})) == []


class TestPickDataBlock:
    def test_picks_block_with_most_non_null_values(self):
        raw = {
            "timestamps": [1, 2, 3],
            "sparse": [[1, None, None], {}],
            "dense": [[1, 2, 3], {"mean": 2}],
        }

        key, ts, vals, meta = pick_data_block(raw)

        assert key == "dense"
        assert vals == [1, 2, 3]
        assert ts == [1, 2, 3]

    def test_falls_back_to_timestamp_only_payload(self):
        raw = {"timestamps": [1, 2]}

        key, ts, vals, meta = pick_data_block(raw)

        assert (key, ts, vals, meta) == (None, [1, 2], [], {})

    def test_defaults_for_unusable_payload(self):
        assert pick_data_block({"a": "b"}) == (None, None, None, {})


class TestIterTimelinePoints:
    def test_skips_nulls_and_invalid_values(self):
        raw = {"timestamps": [1, 2, 3, 4], "a": [[10, None, "bad", 40], {}]}

        assert list(iter_timeline_points(raw)) == [(1.0, 10.0), (4.0, 40.0)]

    def test_empty_for_payload_without_points(self):
        assert list(iter_timeline_points({})) == []


class TestExtractTimelineMeta:
    def test_extracts_unit_mean_and_nodes(self):
        raw = {
            "unit": "MB",
            "timestamps": [1],
            "a": [[5], {"mean": "2.5", "best_node": "n1", "lowest_node": "n2"}],
        }

        assert extract_timeline_meta(raw) == ("MB", 2.5, "n1", "n2")

    def test_handles_non_dict_payload(self):
        assert extract_timeline_meta(None) == (None, None, None, None)

    def test_handles_missing_meta(self):
        raw = {"unit": "MB", "timestamps": [1], "a": [[5], {}]}

        assert extract_timeline_meta(raw) == ("MB", None, None, None)

    def test_ignores_non_numeric_mean(self):
        raw = {
            "unit": "MB",
            "timestamps": [1],
            "a": [[5], {"mean": "n/a"}],
        }

        assert extract_timeline_meta(raw) == ("MB", None, None, None)


class TestExtractJobDetail:
    def test_dict_passthrough(self):
        assert extract_job_detail({"job_name": "j"}) == {"job_name": "j"}

    def test_list_takes_first_dict(self):
        assert extract_job_detail([{"job_name": "j"}, {"x": 1}]) == \
            {"job_name": "j"}

    @pytest.mark.parametrize("value", [None, [], "text", 42, [1, 2]])
    def test_invalid_payloads_return_empty_dict(self, value):
        assert extract_job_detail(value) == {}


class TestMergeTimeline:
    def _entry(self):
        return {
            "ts": [], "vals": [], "seen": set(), "unit": "",
            "mean": None, "best_node": None, "lowest_node": None,
            "data_key": "", "error_shown": False,
        }

    def test_merges_and_deduplicates_by_timestamp(self):
        entry = self._entry()

        merge_timeline(entry, {
            "unit": "u", "timestamps": [1, 2],
            "a": [[10, 20], {"mean": 15}]})
        merge_timeline(entry, {
            "unit": "u", "timestamps": [2, 3],
            "a": [[20, 30], {"mean": 25, "best_node": "n1"}]})

        assert entry["ts"] == [1.0, 2.0, 3.0]
        assert entry["vals"] == [10.0, 20.0, 30.0]
        assert entry["data_key"] == "a"
        assert entry["mean"] == 25
        assert entry["best_node"] == "n1"

    def test_trims_to_max_points_and_syncs_seen(self):
        entry = self._entry()
        count = MAX_POINTS_PER_PLOT + 5
        raw = {
            "unit": "u",
            "timestamps": list(range(count)),
            "a": [list(range(count)), {"mean": 1}],
        }

        ts, vals = merge_timeline(entry, raw)

        assert len(ts) == MAX_POINTS_PER_PLOT
        assert len(vals) == MAX_POINTS_PER_PLOT
        assert len(entry["seen"]) == MAX_POINTS_PER_PLOT
        assert entry["ts"][0] == 5.0
        assert entry["ts"][-1] == float(count - 1)
        assert 0.0 not in entry["seen"]


# =============================================================================
# Monitor
# =============================================================================


class TestPikaMetricMonitorResolution:
    def test_resolve_job_from_slurm(self, fake_slurm):
        monitor = PikaMetricMonitor(slurm_info=fake_slurm)

        info = monitor._resolve_job()

        assert info == {"ok": True, "job_id": 12345, "job_start": 1000,
                        "partition": "batch"}

    def test_resolve_job_without_slurm(self):
        monitor = PikaMetricMonitor(slurm_info=None)

        info = monitor._resolve_job()

        assert info["ok"] is False

    def test_job_summary(self, fake_slurm):
        monitor = PikaMetricMonitor(slurm_info=fake_slurm)

        assert monitor._job_summary() == \
            "job_id=12345 start=1000 partition=batch"

    def test_metric_display_name(self):
        assert PikaMetricMonitor._get_metric_display_name(
            object(), "cpu_usage") == "CPU Usage"
        assert PikaMetricMonitor._get_metric_display_name(
            object(), "missing") == "missing"


class TestPikaMetricMonitorSelection:
    @pytest.fixture
    def monitor(self, fake_slurm):
        with patch.object(PikaMetricMonitor, "_build_process_figure",
                          return_value=widgets.HTML()):
            yield PikaMetricMonitor(slurm_info=fake_slurm)

    def test_add_and_remove_metric(self, monitor):
        monitor._add_metric("cpu_usage")

        assert "cpu_usage" in monitor._active
        assert monitor._fig is not None
        assert len(monitor._plots_box.children) == 1

        monitor._remove_metric("cpu_usage")

        assert "cpu_usage" not in monitor._active
        assert monitor._fig is None
        assert monitor._plots_box.children == ()

    def test_on_metric_changed_enforces_max_plots(self, fake_slurm):
        with patch.object(PikaMetricMonitor, "_build_process_figure",
                          return_value=widgets.HTML()):
            monitor = PikaMetricMonitor(slurm_info=fake_slurm, max_plots=1)
            monitor._on_metric_changed({"new": True}, "cpu_usage")
            monitor._on_metric_changed({"new": True}, "gpu_usage")

        assert list(monitor._active) == ["cpu_usage"]
        assert monitor._metric_boxes["gpu_usage"].value is False
        assert "Maximum 1 plots" in monitor._status.value

    def test_on_metric_changed_removes(self, monitor):
        monitor._on_metric_changed({"new": True}, "cpu_usage")
        monitor._on_metric_changed({"new": False}, "cpu_usage")

        assert monitor._active == {}

    def test_refresh_metric_list_filters_children(self, monitor):
        monitor._refresh_metric_list("cpu")

        shown = monitor._metric_list_box.children
        assert monitor._metric_boxes["cpu_usage"] in shown
        assert monitor._metric_boxes["cpu_usage"].layout.display == "flex"
        assert monitor._metric_boxes["gpu_usage"].layout.display == "none"

    def test_set_status_error_colour(self, monitor):
        monitor._set_status("boom", error=True)

        assert "#dc3545" in monitor._status.value
        assert "boom" in monitor._status.value

    def test_on_apply_reports_success_for_valid_job(self, monitor):
        detail = {"job_name": "my-job <tag>"}
        with patch.object(PikaClientLite, "get_job_detail",
                          return_value=detail):
            monitor._on_apply(None)

        assert "Verified!" in monitor._api_msg.value

    def test_on_apply_reports_invalid_on_error(self, monitor):
        with patch.object(PikaClientLite, "get_job_detail",
                          side_effect=Exception("nope")):
            monitor._on_apply(None)

        assert "Invalid!" in monitor._api_msg.value

    def test_collect_returns_empty_without_resolved_job(self, monitor):
        assert monitor.collect() == {}

    def test_collect_merges_timeline_data(self, monitor):
        monitor._job = {"job_id": 1, "job_start": 2, "partition": "p"}
        monitor._add_metric("cpu_usage")
        raw = {"unit": "u", "timestamps": [1, 2],
               "a": [[10, 20], {"mean": 15}]}

        with patch.object(monitor._client, "get_timeline", return_value=raw):
            results = monitor.collect()

        assert results["cpu_usage"]["vals"] == [10.0, 20.0]
        assert results["cpu_usage"]["mean"] == 15.0

    def test_collect_records_error_and_continues(self, monitor):
        monitor._job = {"job_id": 1, "job_start": 2, "partition": "p"}
        monitor._add_metric("cpu_usage")

        with patch.object(monitor._client, "get_timeline",
                          side_effect=Exception("boom")):
            results = monitor.collect()

        assert results == {}
        assert monitor._active["cpu_usage"]["error_shown"] is True
        assert "#dc3545" in monitor._status.value
