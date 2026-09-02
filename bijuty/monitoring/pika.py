"""Pika (Slurm job) timeline monitor with interactive Plotly visualization.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import ipywidgets as widgets
import plotly.graph_objects as go

from .dashboard import MetricDashboard
import logging
import time
logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

DEFAULT_BASE_URL = ""
DEFAULT_AUTH_URL = ""
MAX_PLOTS = 6
DEFAULT_REFRESH_INTERVAL = 5.0
MIN_REFRESH_INTERVAL = 2.0
MAX_REFRESH_INTERVAL = 60.0
MAX_POINTS_PER_PLOT = 500
SUBPLOT_COLS = 4
SUBPLOT_ROW_HEIGHT = 280


# =============================================================================
# Available metrics (keys match the backend TimelineMetric enum values).
# =============================================================================

AVAILABLE_METRICS: List[Tuple[str, str]] = [
    ("lustre_io", "Lustre IO"),
    ("lustre_io_meta", "Lustre IO Meta"),
    ("horse_read_bw", "FS Horse Read BW"),
    ("horse_write_bw", "FS Horse Write BW"),
    ("octopus_read_bw", "FS Octopus Read BW"),
    ("octopus_write_bw", "FS Octopus Write BW"),
    # ("scratch_read_bw", "Scratch Read BW"),
    # ("scratch_write_bw", "Scratch Write BW"),
    # ("highiops_read_bw", "HighIOPS Read BW"),
    # ("highiops_write_bw", "HighIOPS Write BW"),
    ("horse_read_requests", "FS Horse Read Requests"),
    ("horse_write_requests", "FS Horse Write Requests"),
    # ("horse_open", "FS Horse Open"),
    # ("horse_close", "FS Horse Close"),
    # ("horse_fsync", "FS Horse Fsync"),
    # ("horse_create", "FS Horse Create"),
    # ("horse_seek", "Horse Seek"),
    ("octopus_read_requests", "FS Octopus Read Requests"),
    ("octopus_write_requests", "FS Octopus Write Requests"),
    # ("octopus_open", "Octopus Open"),
    # ("octopus_close", "Octopus Close"),
    # ("octopus_fsync", "Octopus Fsync"),
    # ("octopus_create", "Octopus Create"),
    # ("octopus_seek", "Octopus Seek"),
    # ("scratch_read_requests", "Scratch Read Requests"),
    # ("scratch_write_requests", "Scratch Write Requests"),
    # ("scratch_open", "Scratch Open"),
    # ("scratch_close", "Scratch Close"),
    # ("scratch_fsync", "Scratch Fsync"),
    # ("scratch_create", "Scratch Create"),
    # ("scratch_seek", "Scratch Seek"),
    # ("highiops_read_requests", "HighIOPS Read Requests"),
    # ("highiops_write_requests", "HighIOPS Write Requests"),
    # ("highiops_open", "HighIOPS Open"),
    # ("highiops_close", "HighIOPS Close"),
    # ("highiops_fsync", "HighIOPS Fsync"),
    # ("highiops_create", "HighIOPS Create"),
    # ("highiops_seek", "HighIOPS Seek"),
    # ("local_io", "Local IO"),
    ("read_bw", "Read BW"),
    ("write_bw", "Write BW"),
    # ("local_io_meta", "Local IO Meta"),
    # ("read_ops", "Read Ops"),
    # ("write_ops", "Write Ops"),
    ("cpu_usage", "CPU Usage"),
    ("mem_used", "Memory Used"),
    ("ipc", "IPC"),
    ("flops", "FLOPS"),
    ("mem_bw", "Memory BW"),
    ("infiniband_bw", "InfiniBand BW"),
    ("gpu_usage", "GPU Usage"),
    ("gpu_power", "GPU Power"),
    ("gpu_mem", "GPU Memory"),
    ("gpu_temperature", "GPU Temperature"),
    ("gpu_flops", "GPU FLOPS"),
    ("gpu_flops_any", "GPU FLOPS Any"),
    ("gpu_flops_16", "GPU FLOPS 16"),
    ("gpu_flops_32", "GPU FLOPS 32"),
    ("gpu_flops_64", "GPU FLOPS 64"),
    ("gpu_flops_tensor", "GPU FLOPS Tensor"),
    ("cpu_power", "CPU Power"),
    # ("cpi", "CPI"),
    ("job_mem_used", "Job Memory Used"),
    ("ethernet_bw", "Ethernet BW"),
]
AVAILABLE_METRICS.sort(key=lambda x: x[1])

_METRIC_LABEL = {key: label for key, label in AVAILABLE_METRICS}
_METRIC_KEYS = [key for key, _ in AVAILABLE_METRICS]


# =============================================================================
# Lightweight client
# =============================================================================

class PikaClientLite:
    """Thin synchronous wrapper around the pika timeline / timeline_extern endpoints."""

    def __init__(self, base_url: str, token: Optional[str] = None,
                 timeout: float = 10.0, verify_ssl: bool = True):
        self.base_url = base_url
        self.token = token or ""
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        # Mount an HTTPAdapter with automatic retry on transient errors
        # (ConnectionError, RemoteDisconnected, 5xx, etc.) using
        # exponential backoff.  This makes the polling loop resilient to
        # keep-alive connection drops and pika backend rate-limits.
        if Retry is not None:
            retry = Retry(
                total=3,
                connect=3,
                read=3,
                redirect=0,
                backoff_factor=1.0,
                status_forcelist=(500, 502, 503, 504),
                allowed_methods=("HEAD", "GET", "OPTIONS", "POST"),
                raise_on_status=False,
                respect_retry_after_header=True,
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        self.session.headers.update({"accept": "application/json"})
        if self.token:
            self.session.headers.update(
                {"Authorization": f"Bearer {self.token}"})

    def _post(self, path: str, body: Optional[Dict] = None) -> Any:
        url = f"{self.base_url}{path}"
        resp = self.session.post(url, json=body or {}, timeout=self.timeout,
                                 verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json()

    def get_credential(self) -> Dict:
        """GET /credential - validate token and return current logged in user."""
        url = f"{self.base_url}/credential"
        resp = self.session.get(
            url, timeout=self.timeout, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json()

    def get_job_detail(self, job_id, job_start, partition) -> Dict:
        """POST /job/{job_id}/{job_start}/{partition} - validate token + server."""
        return self._post(
            f"/job/{job_id}/{job_start}/{partition}",
            {"format_result": False},
        )

    def get_timeline(self, metric: str, job_id, job_start, partition,
                     body: Optional[Dict] = None) -> Any:
        """POST /timeline/{metric}/{job_id}/{job_start}/{partition}."""
        return self._post(
            f"/timeline/{metric}/{job_id}/{job_start}/{partition}",
            body or {"mean_line": True},
        )

    def get_timeline_extern(self, metric: str, job_id, job_start, partition,
                            body: Optional[Dict] = None) -> Any:
        """POST /timeline_extern/{metric}/{job_id}/{job_start}/{partition}."""
        return self._post(
            f"/timeline_extern/{metric}/{job_id}/{job_start}/{partition}",
            body or {"mean_line": True},
        )


# =============================================================================
# Timeline parsing + de-duplication
# =============================================================================

def data_blocks(raw):
    """Yield (key, timestamps, values, meta) for each top-level data member.

    A pika timeline response looks like::

        {
          "unit": "...",
          "timestamps": [t1, t2, ...],
          "<name1>": [ [v1, v2, ...], {"mean": ..., "best_node": ..., "lowest_node": ...} ],
          ...
        }
    """
    if not isinstance(raw, dict):
        return
    timestamps = raw.get("timestamps")
    if not isinstance(timestamps, list):
        return
    for k, v in raw.items():
        if k in ("unit", "timestamps"):
            continue
        if not isinstance(v, list) or not v:
            continue
        if not isinstance(v[0], list):
            continue
        vals = v[0]
        meta = v[1] if len(v) >= 2 and isinstance(v[1], dict) else {}
        yield k, timestamps, vals, meta


def pick_data_block(raw):
    """Pick the (key, ts, vals, meta) block with the most non-null values."""
    best = (None, None, None, {})
    best_count = -1
    for k, ts, vals, meta in data_blocks(raw):
        non_null = sum(1 for v in vals if v is not None)
        if non_null > best_count:
            best_count = non_null
            best = (k, ts, vals, meta)
    if best[0] is None and isinstance(raw, dict):
        ts_only = raw.get("timestamps")
        if isinstance(ts_only, list):
            return (None, ts_only, [], {})
    return best


def iter_timeline_points(raw):
    """Yield (timestamp, value) pairs from the picked data block."""
    _key, ts, vals, _meta = pick_data_block(raw)
    if ts is None or vals is None:
        return
    for t, v in zip(ts, vals):
        if v is None:
            continue
        try:
            yield float(t), float(v)
        except (TypeError, ValueError):
            continue


def extract_timeline_meta(raw):
    """Extract (unit, mean, best_node, lowest_node) from the picked data block."""
    if not isinstance(raw, dict):
        return None, None, None, None
    unit = raw.get("unit")
    _key, _ts, _vals, meta = pick_data_block(raw)
    if not meta:
        return unit, None, None, None
    mean_v = meta.get("mean")
    try:
        mean = float(mean_v) if mean_v is not None else None
    except (TypeError, ValueError):
        mean = None
    return unit, mean, meta.get("best_node"), meta.get("lowest_node")


def extract_job_detail(raw):
    """Normalise /job/{id}/{start}/{partition} response (dict or list-of-dict)."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list) and raw:
        first = raw[0]
        if isinstance(first, dict):
            return first
    return {}


def merge_timeline(entry, raw):
    """Merge new timeline points into entry, de-duplicating by timestamp."""
    ts = entry["ts"]
    vals = entry["vals"]
    seen = entry["seen"]
    for t, v in iter_timeline_points(raw):
        if t in seen:
            continue
        seen.add(t)
        ts.append(t)
        vals.append(v)
    while len(ts) > MAX_POINTS_PER_PLOT:
        removed = ts.pop(0)
        vals.pop(0)
        seen.discard(removed)
    unit, mean, best_node, lowest_node = extract_timeline_meta(raw)
    if unit is not None:
        entry["unit"] = unit
    if mean is not None:
        entry["mean"] = mean
    if best_node is not None:
        entry["best_node"] = best_node
    if lowest_node is not None:
        entry["lowest_node"] = lowest_node
    key, _, _, _ = pick_data_block(raw)
    if key is not None:
        entry["data_key"] = key
    return ts, vals


# =============================================================================
# Monitor
# =============================================================================

class PikaMetricMonitor(MetricDashboard):
    """Interactive widget to graph pika job timeline metrics in a shared grid.

    The monitor itself implements the ``collect()`` contract required by
    :py:class:`MetricDashboard`: it iterates over currently-active metrics,
    polls ``/timeline/{metric}/...`` for each, de-duplicates by timestamp,
    and returns a ``Dict[metric_key, metric_data]`` that
    :py:meth:`_render_metrics` maps onto the shared subplot figure built by
    :py:meth:`MetricDashboard._build_process_figure`.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        slurm_info: Any = None,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
        max_plots: int = MAX_PLOTS,
    ) -> None:
        self._base_url = base_url or DEFAULT_BASE_URL
        self._token = token or ""
        self.slurm_info = slurm_info
        self.max_plots = max_plots

        # Pika-specific state (must exist before base __init__ so that
        # _build_dashboard / _collect_loop can reference them).
        self._active: Dict[str, Dict[str, Any]] = {}
        self._metric_boxes: Dict[str, widgets.Checkbox] = {}
        self._job: Optional[Dict[str, Any]] = None
        self._fig: Optional[go.FigureWidget] = None

        # Build pika-specific widgets before super().__init__() because
        # MetricDashboard.__init__ calls self._build_dashboard() which
        # references these.
        self._build_pika_widgets()

        # HTTP client used by collect() and by _on_apply().
        self._client = PikaClientLite(
            base_url=self._base_url, token=self._token)

        # self acts as its own collector — collect()/render_metrics()
        # are the MetricDashboard collector contract.
        # max_cols=4 → subplots laid out as 4 columns × N rows.
        super().__init__(
            collector=self,
            refresh_interval=refresh_interval,
            min_refresh=MIN_REFRESH_INTERVAL,
            max_refresh=MAX_REFRESH_INTERVAL,
            refresh_step=1.0,
            max_cols=SUBPLOT_COLS,
            plot_height=SUBPLOT_ROW_HEIGHT,
        )

    # ------------------------------------------------------------------
    # MetricDashboard layout override
    # ------------------------------------------------------------------

    def _build_dashboard(
        self, extra_header_widgets: Optional[List[widgets.Widget]] = None
    ) -> widgets.VBox:
        """Pika layout: controls + config row + job identity + selector + plots."""
        return widgets.VBox([
            self._controls,
            self._api_config_new,
            self._search,
            self._metric_list_box,
            self._status,
            self._plots_box,
        ])

    def _get_metric_display_name(self, metric: str) -> str:
        """Human-readable label for a metric key (delegates to _METRIC_LABEL)."""
        return _METRIC_LABEL.get(metric, metric)

    # ------------------------------------------------------------------
    # MetricDashboard start override
    # ------------------------------------------------------------------

    def _on_start(self, _b: widgets.Button) -> None:
        """Validate token + job identity, then start the collection thread."""
        try:
            self._token = self._token_w.value
        except Exception:
            pass
        try:
            self._base_url = self._url.value
        except Exception:
            pass

        info = self._resolve_job()
        if not info["ok"]:
            self._set_status(
                "Cannot resolve job identity from SlurmManager.", error=True)
            return
        if not self._token:
            self._set_status(
                "No API key provided - click 'Get API Key' and paste it.",
                error=True)
            return

        self._job = info
        self._client = PikaClientLite(
            base_url=self._base_url, token=self._token)
        self._start_collecting()

    # ------------------------------------------------------------------
    # MetricDashboard collector contract
    # ------------------------------------------------------------------

    def collect(self) -> Dict[str, Dict[str, Any]]:
        """Poll every active metric and return merged timeline data."""
        if not self._job:
            return {}
        results: Dict[str, Dict[str, Any]] = {}
        for key, entry in list(self._active.items()):
            try:
                raw = self._client.get_timeline(
                    key,
                    self._job["job_id"],
                    self._job["job_start"],
                    self._job["partition"],
                    {"mean_line": True},
                )
                merge_timeline(entry, raw)
                entry["error_shown"] = False
            except Exception as e:
                if not entry.get("error_shown"):
                    msg = f"{key}: {type(e).__name__}: {e}"
                    if len(msg) > 120:
                        msg = msg[:117] + "..."
                    self._set_status(msg, error=True)
                    entry["error_shown"] = True
                continue

            results[key] = {
                "ts": list(entry["ts"]),
                "vals": list(entry["vals"]),
                "unit": entry.get("unit", ""),
                "mean": entry.get("mean"),
                "best_node": entry.get("best_node"),
                "lowest_node": entry.get("lowest_node"),
                "data_key": entry.get("data_key", ""),
            }

        return results

    def _render_metrics(self, metrics: Dict[str, Dict[str, Any]]) -> None:
        """Update the shared subplot figure with latest merged timeline data."""
        if not metrics or self._fig is None:
            return
        fig = self._fig
        trace_map = {t.name: i for i, t in enumerate(fig.data)}
        active_keys = list(self._active.keys())

        with fig.batch_update():
            for key in active_keys:
                data = metrics.get(key)
                if not data:
                    continue
                # ts = data["ts"]
                ts = [time.strftime('%H:%M:%S', time.localtime(x))
                      for x in data["ts"]]
                vals = data["vals"]
                if not vals:
                    continue
                y_mean = [sum(vals) / len(vals)] * len(vals)
                y_max = [max(vals)] * len(vals)
                y_min = [min(vals)] * len(vals)

                for trace_suffix, y_vals in (
                    ("data", vals),
                    ("mean", y_mean),
                    ("max", y_max),
                    ("min", y_min),
                ):
                    idx = trace_map.get(f"{trace_suffix}_{key}")
                    if idx is not None:
                        fig.data[idx].x = ts
                        fig.data[idx].y = y_vals

    # ------------------------------------------------------------------
    # Figure rebuild (called when active metric set changes)
    # ------------------------------------------------------------------

    def _rebuild_figure(self) -> None:
        """Rebuild the single shared subplot figure from the active metric set."""
        active_keys = list(self._active.keys())
        if not active_keys:
            self._fig = None
            self._plots_box.children = ()
            return
        self._fig = self._build_process_figure("pika", active_keys)
        self._plots_box.children = (self._fig,)

    # ------------------------------------------------------------------
    # job identity from SlurmManager
    # ------------------------------------------------------------------

    def _job_summary(self) -> str:
        info = self._resolve_job()
        if info["ok"]:
            return (f"job_id={info['job_id']} start={info['job_start']} "
                    f"partition={info['partition']}")
        return "not resolved (start inside a Slurm job)"

    def _resolve_job(self) -> Dict[str, Any]:
        si = self.slurm_info
        if si is None:
            return {"ok": False, "job_id": None,
                    "job_start": None, "partition": None}
        try:
            job_id = int(getattr(si, "job_id", None))
            job_start = int(getattr(si, "_start_time", None)
                            or getattr(si, "start_time", None))
            partition = getattr(si, "_partition", None) or getattr(
                si, "partition", None) or "local"
            return {"ok": bool(job_id) and bool(job_start), "job_id": job_id,
                    "job_start": job_start, "partition": partition}
        except Exception:
            return {"ok": False, "job_id": None,
                    "job_start": None, "partition": None}

    # ------------------------------------------------------------------
    # widget construction
    # ------------------------------------------------------------------

    def _build_pika_widgets(self) -> None:
        # config row with api key entry + redirect-to-auth button
        self._url = widgets.Text(value=self._base_url, description="Pika URL:")
        self._url.add_class("pikaConfigRow-url")
        self._url.observe(self._on_url_change, names="value")
        self._token_w = widgets.Password(
            value=self._token, description="API Key:")
        self._token_w.add_class("pikaConfigRow-api-token")
        self._token_w.observe(self._on_token_change, names="value")
        self._get_key_btn = widgets.Button(description="Get API Key")
        self._get_key_btn.add_class("pikaConfigRow-api-get-btn")
        self._get_key_btn.on_click(self._on_get_key)
        self._apply_btn = widgets.Button(
            description="Apply", layout=widgets.Layout(width="80px"))
        self._apply_btn.on_click(self._on_apply)
        self._api_msg = widgets.HTML(value="")
        self._api_msg.add_class("pikaConfigRow-api-message")
        self._api_config_tmp = widgets.HBox([self._token_w,
                                             self._apply_btn,
                                             self._get_key_btn])
        self._api_config_tmp.add_class("pikaConfigRow-api")
        # self._api_config = widgets.VBox([self._api_config_tmp, self._api_msg])
        # self._api_config.add_class("pikaConfigRow-api")
        self._config_row = widgets.HBox([self._url, self._api_config_tmp])
        self._config_row.add_class("pikaConfigRow")
        self._api_config_new = widgets.VBox([self._config_row, self._api_msg])

        # searchable metric selector
        self._search = widgets.Text(value="", description="Search metric:")
        self._search.add_class("pika-metric-search")
        self._search.observe(self._on_search, names="value")
        self._metric_list_box = widgets.VBox()
        self._metric_list_box.add_class("pika-metric-list-box")
        for key in _METRIC_KEYS:
            cb = widgets.Checkbox(
                value=False, description=_METRIC_LABEL.get(key, key),
                indent=False)
            cb.observe(lambda change, k=key: self._on_metric_changed(
                change, k), names="value")
            self._metric_boxes[key] = cb
        self._refresh_metric_list("")

        self._status = widgets.HTML(value="")
        self._plots_box = widgets.VBox([])

    def _refresh_metric_list(self, query: str) -> None:
        q = query.lower()
        shown = []
        for key, cb in self._metric_boxes.items():
            label = _METRIC_LABEL.get(key, key).lower()
            match = (q in key.lower()) or (q in label)
            cb.layout.display = "flex" if match else "none"
            if match:
                shown.append(cb)
        self._metric_list_box.children = tuple(shown)

    def _on_search(self, change: Dict[str, Any]) -> None:
        self._refresh_metric_list(change["new"])

    def _on_get_key(self, _: widgets.Button) -> None:
        import webbrowser
        try:
            webbrowser.open(DEFAULT_AUTH_URL)
        except Exception as e:
            self._set_status(f"Could not open browser: {e}", error=True)

    def _on_url_change(self, change: Dict[str, Any]) -> None:
        """Keep ``self._base_url`` in sync with the URL widget and invalidate the check indicator."""
        self._base_url = change["new"]
        try:
            self._api_msg.value = (
                "<span style=\"color:#999\">Apply to re-validate</span>")
        except Exception:
            pass

    def _on_token_change(self, change: Dict[str, Any]) -> None:
        """Keep ``self._token`` in sync with the Password widget."""
        self._token = change["new"]
        try:
            self._api_msg.value = (
                "<span style=\"color:#999\">Apply to re-validate</span>")
        except Exception:
            pass

    def _on_apply(self, _: widgets.Button) -> None:
        self._base_url = self._url.value
        self._token = self._token_w.value
        self._client = PikaClientLite(
            base_url=self._base_url, token=self._token)
        info = self._resolve_job()
        if not info["ok"]:
            self._api_msg.value = (
                "<span style=\"color:#dc3545;font-weight:bold\">"
                "Invalid!</span>")
            return
        self._api_msg.value = ("<span style=\"color:#999\">Checking…</span>")
        try:
            job_id = info["job_id"]
            detail = self._client.get_job_detail(
                job_id, info["job_start"], info["partition"])
        except Exception as e:
            self._api_msg.value = (
                f"<span style=\"color:#dc3545;font-weight:bold\">Invalid! Check URL or API Key.</span>")
            return
        d = extract_job_detail(detail)
        job_name = d.get("job_name") or d.get("name") or ""
        # job_state = d.get("job_state") or d.get("state") or ""
        safe_name = (job_name.replace("&", "&amp;").replace("<", "&lt;").replace(
            ">", "&gt;").replace("\"", "&quot;")) if job_name else ""
        if safe_name:
            self._api_msg.value = (
                f"<span style=\"color:#28a745;font-weight:bold\">"
                f"Verified!</span>")

    def _set_status(self, msg: str, error: bool = False) -> None:
        color = "#dc3545" if error else "#28a745"
        self._status.value = f'<span style="color:{color}">{msg}</span>'

    # ------------------------------------------------------------------
    # selection / add & remove metrics (rebuilds the shared figure)
    # ------------------------------------------------------------------

    def _on_metric_changed(self, change: Dict[str, Any], key: str) -> None:
        if change["new"]:
            if len(self._active) >= self.max_plots:
                self._metric_boxes[key].value = False
                self._set_status(
                    f"Maximum {self.max_plots} plots reached.", error=True)
                return
            self._add_metric(key)
        else:
            self._remove_metric(key)

    def _add_metric(self, key: str) -> None:
        self._active[key] = {
            "ts": [],
            "vals": [],
            "seen": set(),
            "unit": "",
            "mean": None,
            "best_node": None,
            "lowest_node": None,
            "data_key": "",
            "error_shown": False,
        }
        self._rebuild_figure()

    def _remove_metric(self, key: str) -> None:
        self._active.pop(key, None)
        if key in self._metric_boxes:
            self._metric_boxes[key].value = False
        self._rebuild_figure()
