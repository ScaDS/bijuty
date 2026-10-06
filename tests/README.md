# BiJuTy Test Suite

This document explains the automated test suite for the **bijuty** package:
what is covered, how it is organised, how the tests stay hermetic (no SLURM,
no network), and how to run and extend them.

The suite is built with [pytest](https://docs.pytest.org/) and contains
**307 collected test cases** (294 test functions, some parametrized into
multiple cases) across **16 modules** plus shared fixtures.

---

## Table of contents

1. [Goals and scope](#goals-and-scope)
2. [Quick start](#quick-start)
3. [Directory layout](#directory-layout)
4. [Conventions](#conventions)
5. [Test doubles and mocking strategy](#test-doubles-and-mocking-strategy)
6. [Module-by-module coverage](#module-by-module-coverage)
7. [Detailed file reference](#detailed-file-reference)
8. [Integration tests](#integration-tests)
9. [Coverage configuration](#coverage-configuration)
10. [Known limitations and surfaced issues](#known-limitations-and-surfaced-issues)
11. [Adding new tests](#adding-new-tests)
12. [Continuous integration](#continuous-integration)

---

## Goals and scope

The suite follows standard unit/integration testing practice:

- **Fast, deterministic, hermetic.** No test requires a SLURM allocation, a
  running Spark/Flink cluster, a Jupyter kernel, an `anywidget` install, or
  network access. Every external boundary (`requests`, `psutil`, `subprocess`,
  `time`, SLURM) is replaced by a scripted double or `unittest.mock` patch.
- **Unit tests dominate.** Pure logic (parsers, dataclasses, config, env-file
  rewriting, collectors) is tested in isolation under `tests/unit/`.
- **A thin integration layer.** `tests/integration/` builds the real widget
  tree headlessly to verify orchestration wiring that unit tests cannot.
- **Edge and error paths are first-class.** Timeouts, non-zero return codes,
  HTTP failures, missing keys, empty inputs, zombie/denied processes and
  rollover of fixed-size history buffers are all asserted explicitly.

Out of scope: rendering fidelity of Plotly figures, browser-side widget
behaviour, and the vendored `frameworks/` trees (Spark/Flink sources).

---

## Quick start

Run everything from the repository root (`pyproject.toml` sets
`pythonpath = ["."]`, so the `bijuty` package is importable without install):

```bash
# Full suite
pytest

# Unit tests only (fast)
pytest tests/unit

# Integration tests only
pytest -m integration

# Skip the slower GUI workflow tests
pytest -m "not integration"

# A single module
pytest tests/unit/test_utils.py

# Filter by keyword
pytest -k "spark or flink"

# Verbose, showing each test name
pytest -v

# Stop at first failure
pytest -x
```

Coverage requires `pytest-cov` (listed in `requirements.txt`):

```bash
pytest --cov=bijuty --cov-report=term-missing
pytest --cov=bijuty --cov-report=html   # writes htmlcov/
```

Typical result:

```
307 passed, 97 warnings in ~2s
```

(The warnings come from the package's own `ipywidgets`/`traitlets` usage and
are pre-existing; they are unrelated to the tests.)

---

## Directory layout

```
tests/
├── README.md                         # this file
├── conftest.py                       # shared fixtures + test doubles
├── test_notebooks.py                 # structural validation of example/*.ipynb
├── unit/
│   ├── test_package.py               # top-level bijuty package API
│   ├── test_utils.py                 # bijuty/utils.py
│   ├── test_templates.py             # bijuty/templates.py
│   ├── test_slurm_utils.py           # bijuty/slurm_utils.py
│   ├── test_big_data_manager.py      # bijuty/big_data_manager.py
│   ├── test_config.py                # bijuty/gui/config.py
│   ├── test_widgets.py               # bijuty/gui/widgets.py
│   ├── test_html.py                  # bijuty/gui/html.py
│   ├── test_env_setup.py             # bijuty/gui/env_setup.py
│   ├── test_process_monitor.py       # bijuty/monitoring/process.py
│   ├── test_spark_monitor.py         # bijuty/monitoring/spark.py
│   ├── test_flink_monitor.py         # bijuty/monitoring/flink.py
│   ├── test_pika.py                  # bijuty/monitoring/pika.py
│   └── test_dashboard.py             # bijuty/monitoring/dashboard.py
└── integration/
    └── test_gui_workflow.py          # GUIMain + MultiFrameworkManager
```

Mirroring the source tree (`test_<module>.py` for `bijuty/<module>.py`) makes
it obvious where the tests for a given file live and where new ones belong.

---

## Conventions

### Naming

- Files: `test_<module>.py`, located to mirror the package path.
- Classes: `Test<Unit>` grouping the tests for one class or function
  (for example `TestJobResources`, `TestVerifyClusterWorkers`).
- Functions: `test_<unit>_<scenario>_<expected>`, for example
  `test_run_bash_command_timeout_returns_124`,
  `test_spark_reports_mismatch_when_worker_missing`.

### Structure

Every test follows **Arrange–Act–Assert** with a blank line between the three
phases. Tests assert observable behaviour (return values, raised exceptions,
recorded calls) rather than implementation details, except where the wire
format is itself the contract (for example the REST URL/body assertions).

### Isolation

`tests/conftest.py` defines an `autouse` fixture, `isolate_slurm_env`, that
deletes every `SLURM_*`, `SPARK_*`, `FLINK_*` and `PYFLINK*` environment
variable before each test. This is necessary because `SlurmManager.__init__`
uses `os.environ.setdefault`, which would otherwise leak state between tests
and make them order-dependent.

### Markers

| Marker        | Meaning                                                        |
| ------------- | -------------------------------------------------------------- |
| `integration` | End-to-end orchestration tests that build the widget tree.     |

The marker is registered in `pyproject.toml`; `--strict-markers` is enabled so
typos are caught. Unit tests carry no marker.

### Parametrization

Multi-case logic uses `@pytest.mark.parametrize` instead of copy-pasted tests,
for example:

- `test_config.py` — expected ports per framework.
- `test_slurm_utils.py` — numeric vs non-numeric `SLURM_JOB_ID` values.
- `test_pika.py` — invalid `/job` payload shapes.
- `test_utils.py` — timeout return code.

---

## Test doubles and mocking strategy

All doubles live in `tests/conftest.py` (importable as `tests.conftest`).
Reusing them keeps tests short and consistent.

| Double / fixture        | Replaces                          | Used by |
| ----------------------- | --------------------------------- | ------- |
| `isolate_slurm_env`     | ambient environment               | all tests (autouse) |
| `fake_resources`        | `slurm_utils.JobResources`        | SLURM, managers, GUI |
| `FakeSlurm` / `fake_slurm` | `slurm_utils.SlurmManager`     | managers, monitors, GUI |
| `framework_registry`    | `gui.config.FRAMEWORK_REGISTRY`   | config assertions |
| `FakePsutilProcess`     | `psutil.Process`                  | monitoring collectors |
| `fake_psutil_process`   | a default `FakePsutilProcess`     | monitoring collectors |
| `make_process_snapshot` / `process_snapshot_factory` | `ProcessMetricsSnapshot` | monitoring |

### Patching external boundaries

| Boundary     | Patch target (examples)                                  |
| ------------ | -------------------------------------------------------- |
| HTTP         | `bijuty.monitoring.spark.requests.get`, `...flink.requests.get`, `bijuty.big_data_manager.requests.get`, `requests.get` (widgets) |
| Session HTTP | `patch.object(client.session, "post"/"get")` in Pika tests |
| Process table| `bijuty.big_data_manager.psutil.process_iter`, `bijuty.monitoring.process.psutil.process_iter`, `bijuty.big_data_manager.psutil.Process`, `...psutil.wait_procs` |
| Subprocess   | `bijuty.utils.subprocess.run`, `bijuty.big_data_manager.run_bash_command`, `bijuty.slurm_utils.run_bash_command` |
| Sockets      | `bijuty.utils.socket.socket` (port discovery)            |
| Clock        | `timeout=0` / immediate returns instead of sleeping      |
| GUI network  | `bijuty.gui.main.fetch_image`, `bijuty.gui.main.SlurmManager` |
| Plotly widget| `MetricDashboard._build_process_figure` patched to return a real widget in dashboard and Pika monitor tests |

> **Note on `anywidget`.** `plotly` raises `ImportError` when constructing a
> `FigureWidget` if `anywidget` is not installed. Tests that would build a
> figure patch `_build_process_figure` to return a lightweight real widget, so
> the suite passes with or without `anywidget`.

### `FakeSlurm`

`FakeSlurm` is a duck-typed stand-in exposing exactly what the production code
touches:

```python
resources            # JobResources(node_list=["node1", "node2"], cpus_per_task=4, ...)
in_slurm_job = True
user = "tester"
job_id = "12345"
login_node = "login1.example.org"
_start_time = "1000"; _partition = "batch"
start_time = "1000";  partition = "batch"
```

### `FakePsutilProcess`

Implements the slice of `psutil.Process` used by the collectors: `status`,
`username`, `cmdline`, `oneshot` (context manager), `cpu_percent`,
`memory_info`, `memory_percent`, `num_threads`, `io_counters`, plus `.pid` and
`.info`. Values are injectable so byte→MB conversions and rollover can be
asserted exactly.

---

## Module-by-module coverage

| Test module | Production module | Test functions | Focus |
| ----------- | ----------------- | --------------: | ----- |
| `test_package.py` | `bijuty/__init__.py` | 8 | `set_log_level`, exports, log formatter |
| `test_utils.py` | `bijuty/utils.py` | 14 | subprocess wrapper, file read, port discovery |
| `test_templates.py` | `bijuty/templates.py` | 15 | factory template discovery, copy, CLI |
| `test_slurm_utils.py` | `bijuty/slurm_utils.py` | 23 | env parsing, `JobResources`, `SlurmManager` |
| `test_big_data_manager.py` | `bijuty/big_data_manager.py` | 40 | init, discovery, status, start/stop |
| `test_config.py` | `bijuty/gui/config.py` | 13 | dataclasses, registry, colour scheme |
| `test_widgets.py` | `bijuty/gui/widgets.py` | 20 | factory helpers, containers, checkbox |
| `test_html.py` | `bijuty/gui/html.py` | 10 | HTML/card/viz/ssh/cluster-info generators |
| `test_env_setup.py` | `bijuty/gui/env_setup.py` | 14 | env builders, conf rewriting, pyflink jar |
| `test_process_monitor.py` | `bijuty/monitoring/process.py` | 20 | snapshots, collector, monitor |
| `test_spark_monitor.py` | `bijuty/monitoring/spark.py` | 16 | REST parsing, app-id, conversions |
| `test_flink_monitor.py` | `bijuty/monitoring/flink.py` | 20 | URL resolution, overview parsing |
| `test_pika.py` | `bijuty/monitoring/pika.py` | 39 | HTTP client, timeline parsers, monitor |
| `test_dashboard.py` | `bijuty/monitoring/dashboard.py` | 12 | refresh loop, controls, plot wiring |
| `test_gui_workflow.py` | GUI orchestration | 26 | headless launch, observers, buttons, tabs |
| `test_notebooks.py` | `example/*.ipynb` | 4 | nbformat/source/output structure |

"Test functions" counts the distinct test functions per module; `pytest`
collects more cases where functions are parametrized (307 cases in total).

---

## Detailed file reference

### `unit/test_package.py`

- **TestSetLogLevel** — string levels, numeric levels, unknown string
  fallback to `INFO` (with restore in `finally`).
- **TestPackageExports** — every name in `__all__` is importable; expected
  public symbols present; the `bijuty` logger has exactly one handler and
  level `INFO`.
- **TestLoggerFormatter** — formatted output contains the message and level
  name, and honours a custom `datefmt`.

### `unit/test_utils.py`

- **TestRunBashCommand** — list and shell commands, stdout/stderr stripping,
  non-zero propagation, missing executable → `127` + "Executable not found",
  timeout → `124` + "Timeout expired", generic `OSError` → `1`, and that a
  copy of the environment is passed to `subprocess.run`.
- **TestGetFileContent** — UTF-8 read, relative-path resolution, missing file
  returns an `"An error occurred: ..."` string.
- **TestFindFirstAvailablePort** — first bindable port, skipping busy ports,
  `RuntimeError` when none available, host defaulting via `gethostname`.

### `unit/test_templates.py`

- **TestAvailableTemplates** — Spark and Flink are listed, names are lowercase
  and sorted.
- **TestFactoryTemplatePath** — case-insensitive resolution to an existing
  directory for both frameworks; unknown framework raises `ValueError`.
- **TestInitTemplate** — copies the factory tree to the destination, preserves
  the executable bit on scripts, expands `~`, rejects a non-empty destination
  without `overwrite`, merges with `overwrite=True`, accepts a pre-existing
  empty directory, and rejects a destination that is a file.
- **TestMainCli** — default `./<framework>-template` destination, explicit
  `--destination`, unknown framework exits via argparse, and copy errors are
  reported through argparse.

### `unit/test_slurm_utils.py`

- **TestLocalHostHelpers** — CPU count fallback to 1; `/proc/meminfo`
  `MemAvailable` parsing (via `mock_open`), zero when absent, raising when
  unreadable.
- **TestJobResources** — explicit vs computed memory-per-node, totals, empty
  node list.
- **TestSlurmManagerDetection** — numeric vs non-numeric `SLURM_JOB_ID`,
  legacy `SLURM_JOBID`, raising outside a job, and local-machine mode.
- **TestSlurmManagerInsideJob** — `scontrol --json` payload parsing, resource
  extraction, and error paths (non-zero return code, invalid JSON).
- **TestSlurmManagerHelpers** — node-list normalisation (list/str/empty),
  login-host derivation from FQDN, missing env context, `__repr__`.

### `unit/test_big_data_manager.py`

- **TestInitializeUserInput** — field population, Spark/Flink REST ports,
  wrapped exceptions for missing/unknown frameworks, log-path helper.
- **TestGetFrameworkProcesses** — empty before init, master/worker tuple,
  `all_procs=True` list.
- **TestFindClusterProcesses** — matching current user, ignoring other users
  and empty cmdlines.
- **TestTerminateProcesses** — graceful terminate + hard-kill of survivors,
  tolerance of already-exited processes.
- **TestVerifyClusterWorkers** — empty workers guard, Spark `ALIVE` counting,
  mismatch reporting, Flink taskmanager counting, request exceptions, unknown
  framework.
- **TestIsClusterUp** — guards for uninitialized/fewer-than-two processes,
  master+workers up, workers down, master missing.
- **TestCleanupCluster** — no patterns, no matching processes, terminates
  found processes.
- **TestWaitHelpers** — init/stop wait success and timeout (`timeout=0`).
- **TestStartCluster / TestStopCluster** — not-initialized guards, successful
  start, stop-before-start, command failure, init timeout, graceful stop,
  cleanup on stop timeout, exception handling.

### `unit/test_config.py`

`FrameworkConfig` (name helpers, template path existence, frozen dataclass,
optional-field defaults), `ResourceAllocation` (`to_dict` short keys),
`FRAMEWORK_REGISTRY` (Spark/Flink present, ports, processes, distinct
patterns) and `COLOR_SCHEME`.

### `unit/test_widgets.py`

- **TestFetchImage** — success bytes, request failure, HTTP error status.
- **TestWidgetFactoryButtons** — description/class, style+layout overrides,
  redirect HTML with `target="_blank"`.
- **TestUpdateWidgetState** — disable/enable toggling and idempotency.
- **TestWidgetFactoryInputs** — slider defaults/custom style, dropdown, text
  `disabled`, checkbox type.
- **TestContainerMixin** — `VBox`/`HBox` disable/enable/toggle.
- **TestCustomCheckbox** — value link propagation, label/description
  property, `is_checked`.

### `unit/test_html.py`

Header markup, recursive card visualisation (leaf, nested, default children),
`generate_viz_template` node/coordinator/resource-pool counts, SSH
instructions, and framework cluster info for Spark/Flink and the
`is_config_set=False` branch.

### `unit/test_env_setup.py`

- **TestEnvBuilders** — Spark and Flink environment-variable dictionaries.
- **TestWorkerAndTemplateFiles** — worker file contents, default vs custom
  template selection, config destination directory creation.
- **TestUpdateSparkEnvFile** — replacing active exports, uncommenting
  commented exports, appending missing variables, `log4j2.properties`
  placeholder substitution, environment propagation.
- **TestInitializeBigDataManager** — payload forwarded to
  `BigDataManager.initialize_user_input`.
- **TestEnsurePyflinkJar** — no framework home, no lib dir, missing pyflink
  (via `sys.modules["pyflink"] = None`), and successful jar copy.

### `unit/test_process_monitor.py`

Snapshot/history rollover, collector normalisation and history keys,
`_match_process` (match, zombie, other user, non-match, access denied),
`_extract_metrics` byte→MB and timestamp format, `collect`, monitor display
names, `set_process_names`, and `_render_metrics` no-op/update paths.

### `unit/test_spark_monitor.py`

History append, `_get` URL/raise_for_status, `_fetch_current_app_id`
(first incomplete, last, empty, error), `_compute_snapshot` counts and
MiB conversions (including the `usedOnHeapStorageMemory` fallback), `collect`
history/result and app-id reset, monitor display names and `set_monitor`
variants.

### `unit/test_flink_monitor.py`

History append, `_resolve_url` (provided / proxy / localhost / allocated node
/ fallback), `_check_url`, `_get`, `_fetch_running_job_id`,
`_compute_snapshot` (status counts, slot arithmetic, memory conversions —
note Flink's `canceled` spelling), `collect` keying (`job-1` / `cluster`) and
reset, monitor display names and `set_monitor`.

### `unit/test_pika.py`

- **TestPikaClientLite** — headers (including `Authorization: Bearer`), retry
  adapters, `_post` default body + `raise_for_status`, `get_credential`,
  `get_job_detail`, `get_timeline` (default body), `get_timeline_extern`.
- **Timeline parser tests** — `data_blocks`, `pick_data_block` (densest block,
  timestamp-only fallback), `iter_timeline_points` (null/invalid skipping),
  `extract_timeline_meta` (unit/mean/nodes, non-dict, missing/non-numeric
  mean), `extract_job_detail`, `merge_timeline` (de-duplication and trimming
  to `MAX_POINTS_PER_PLOT` with `seen` synchronisation).
- **TestPikaMetricMonitorResolution** — `_resolve_job` with/without SLURM,
  `_job_summary`, display names.
- **TestPikaMetricMonitorSelection** — add/remove metric, `max_plots`
  enforcement, observer removal, metric-list search filtering, status colour,
  `_on_apply` success/error, and `collect` (empty, merged data, recorded error
  continues).

### `unit/test_dashboard.py`

Covers the shared :class:`MetricDashboard` refresh loop that every framework
monitor builds on. Figure construction is patched (`_build_process_figure`) so
the tests stay independent of `anywidget`, and the background thread is faked.

- **TestDashboardConstruction** — default interval/running state, `get_ui`
  shape, extra header widgets, and that the base
  `_get_metric_display_name` stays abstract.
- **TestRefreshInterval** — the interval slider observer updates
  `refresh_interval`, both through the traitlet and via `_on_interval_change`.
- **TestCollectionLifecycle** — `_start_collecting` flips `running` and the
  start/stop button state and launches the loop thread; `_start_collecting` is
  idempotent; `_stop_collecting` resets state, joins the thread and sets the
  stop event.
- **TestRendering** — empty payload is a no-op, `_update_plot` returns early
  when no plots exist, a new plot id is created and registered, and an existing
  plot dispatches to `_update_plot`.

### `integration/test_gui_workflow.py`

See [Integration tests](#integration-tests).

### `tests/test_notebooks.py`

Structural validation of the shipped `example/*.ipynb` notebooks. Full
`nbval` execution is intentionally avoided (see
[Known limitations](#known-limitations-and-surfaced-issues)); instead each
notebook is parsed and checked for valid nbformat, well-formed cells,
syntactically valid Python in magic-free code cells, and absence of committed
error outputs.

---

## Integration tests

`tests/integration/test_gui_workflow.py` builds the *real* widget tree
headlessly. To keep it deterministic and offline it patches two boundaries:

```python
monkeypatch.setattr(mainmod, "SlurmManager", lambda *a, **k: fake_slurm)
monkeypatch.setattr(mainmod, "fetch_image", lambda url: b"")
```

- **TestGUILaunch** — container shape, default Spark selection, resource
  defaults from the SLURM double, config/log destinations, default-template
  flag, visualisation proportions, process-visualisation rendering, and
  cluster-info rendering.
- **TestRandomizeMasterPort** — the "Randomize Master Port" option resolves a
  free port for both Spark and Flink using the selected master host.
- **TestFrameworkSwitching** — switching to Flink updates derived ports,
  config destination, and the framework-home label.
- **TestClusterButtonToggles** — `all_disabled` and per-button flags.
- **TestObserverDrivenState** — changing driver CPU/memory propagates to the
  worker ranges, changing worker CPU/memory propagates to the executor ranges,
  a parameter edit marks the configuration stale (`is_config_set`), and
  changing the master host refreshes the cluster-info label. These assert the
  observer wiring attached in `_attach_widget_observers` /
  `_setup_dynamic_ranges`.
- **TestClusterButtonHandlers** — the start/stop click handlers delegate to
  `BigDataManager`, flip the button states on success, and swallow engine
  failures while restoring the previous control state (with
  `_set_environment` and the monitor start/stop side effects stubbed out).
- **TestMultiFrameworkManager** — add/close tab behaviour (a single tab can
  never be closed) and `get_gui`, using a fake `GUIMain`.

These tests are the only ones that create real `ipywidgets` objects and are
marked `integration`.

---

## Coverage configuration

Coverage is configured in `pyproject.toml` (used only when `--cov` is passed):

```toml
[tool.coverage.run]
source = ["bijuty"]
branch = true
omit = ["example/*", "frameworks/*"]

[tool.coverage.report]
show_missing = true
skip_covered = false
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
]
```

- **Branch coverage** is enabled to catch untested conditional paths.
- Vendored framework sources and standalone examples are excluded.
- Because `source = ["bijuty"]`, any untested module appears with `0%`, which
  is intentional visibility rather than an omission.

---

## Known limitations and surfaced issues

- **`find_first_available_port` signature.** `bijuty/utils.py` defines
  `find_first_available_port(start_port=..., end_port=..., host=...)` as a
  module-level function, and its caller in `bijuty/gui/main.py` passes the
  selected master host as `host`. The "Randomize Master Port" option now works
  for both Spark and Flink. The tests call the same public signature.
- **`anywidget` optional in test runs.** Figure-building paths are patched, so
  the suite does not require `anywidget` even though it is a runtime
  dependency of `plotly` figure widgets.
- **Widget rendering is not asserted.** Tests verify widget construction,
  state transitions and call wiring; they do not assert rendered pixels.
- **Notebooks are not executed.** `nbval`/browser (Playwright/Galata) notebook
  runs are intentionally omitted: `example/use-case.ipynb` downloads external
  datasets and drives a live Spark cluster whose executor environment only
  exists inside a BiJuTy-managed SLURM allocation, so top-to-bottom execution
  is not hermetic. `tests/test_notebooks.py` performs structural validation
  (valid nbformat, well-formed cells, compilable sources, no stored error
  outputs) instead. The package ships no custom front-end JavaScript bundle,
  so there is no Galata DOM surface to assert.
- **Pre-existing warnings.** `ipywidgets`/`traitlets` emit deprecation
  warnings from the package's own `CustomCheckbox`/`Layout` usage. They are
  reported by pytest but are not test failures.

---

## Adding new tests

1. **Pick the right file.** Mirror the production module under `tests/unit/`
   (or `tests/integration/` for widget orchestration).
2. **Reuse the doubles.** Import from `tests.conftest`
   (`FakeSlurm`, `FakePsutilProcess`) or request the fixtures instead of
   hand-rolling mocks.
3. **Mock at the boundary.** Patch where the name is *looked up*
   (for example `bijuty.monitoring.flink.requests.get`, not `requests.get`).
4. **Cover the unhappy path.** Add cases for empty input, errors, timeouts
   and boundaries alongside the happy path.
5. **Name and structure.** Follow
   `test_<unit>_<scenario>_<expected>` and Arrange–Act–Assert.
6. **Keep it hermetic.** If a test needs the clock or a socket, patch it; never
   sleep or bind a real port.
7. **Run the whole suite** to confirm no cross-test leakage:

   ```bash
   pytest
   ```

### Example template

```python
from unittest.mock import patch

from tests.conftest import FakePsutilProcess


class TestMyThing:
    def test_my_thing_returns_none_when_input_empty(self):
        thing = MyThing()

        result = thing.compute([])

        assert result is None

    def test_my_thing_counts_matching_processes(self):
        thing = MyThing(patterns=["worker"])
        proc = FakePsutilProcess(cmdline=["java", "worker"])

        with patch("bijuty.monitoring.process.psutil.process_iter",
                   return_value=[proc]):
            result = thing.compute_metrics()

        assert "worker" in result
```

---

## Continuous integration

A minimal CI job (assuming a Linux runner with the project dependencies):

```yaml
- name: Install
  run: pip install -r requirements.txt

- name: Run tests
  run: pytest -m "not integration"

- name: Run tests with coverage
  run: pytest --cov=bijuty --cov-report=xml --cov-report=term-missing
```

`pytest-cov` and `pytest` are already declared in `requirements.txt` under
"Development and testing dependencies".
