# Changelog

All notable changes to **BiJuTy** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The project was originally developed under the working name *big-data-framework-tools-for-jupyterhub*
and the package `big_data_utils`, later renamed to **BiJuTy** (`bijuty`).

## [Unreleased]

### Added

- `bijuty.init_template` / `bijuty-template` to copy a bundled factory
  configuration template into an editable directory for use as a custom
  template, plus `available_templates` and `factory_template_path` helpers.
- Pytest and coverage configuration in `pyproject.toml` (`--strict-markers`,
  `--strict-config`, branch coverage, integration marker). (`a9702ee`)

### Changed

- Bumped the supported stack to Python 3.12.3, PySpark 4.1.3, and PyFlink 2.2.1.
- Cleaned up the Spark and Flink configuration templates: removed the legacy
  `flink-conf.yaml` (unsupported since Flink 2.0) and other unused/backup
  template files, and reduced the remaining templates to their relevant,
  documented settings.
- Cleanup refinements across the codebase. (`218dc9e`)
- Updated `.gitignore`. (`8b7922a`)

### Removed

- Removed `requirements.txt`; dependencies are fully declared in `pyproject.toml`. (`38beddd`)
- Removed unused `flink-conf_cp.yaml`. (`edcfc9e`)

## [0.1.x] – 2026-08-13 to 2026-09-03

### Added

- Apache Flink / PyFlink support (package modified for PyFlink). (`83d5ba2`)
- Pika external metric timeline monitor. (`c1aebec`)
- Local-machine fallback to `SlurmManager` using host CPU/memory detection,
  including local machine support and configuration. (`832f30b`, `4e9e72f`)
- Use-case example and cleanup of existing use cases. (`b34b701`)

### Changed

- Refactored and parametrized `style.css`. (`541ac14`)
- Unified `SlurmManager` resource access via a property. (`4e9e72f`)
- Privatized `BigDataManager` internals and scoped tab CSS. (`4e9e72f`)
- Simplified utilities: replaced custom `CommandResult` with
  `subprocess.CompletedProcess`; moved inline widget styles to CSS classes
  (`gui-button`, `slider`, `tabs`); renamed `update_process_viz` to
  `_update_process_viz`; added `shlex.quote` path safety in `big_data_manager`. (`832f30b`)
- Passed `slurm_info` into `FlinkMetricMonitor` instead of instantiating a new one. (`832f30b`)
- Updated installation instructions and README. (`efd7bd8`, `39aed14`)

### Removed

- Removed outdated `setup.py`. (`93a24b1`)
- Removed unused `DEFAULT_LABEL_STYLE` and `debug_write_to_file`. (`832f30b`)

## [0.1.x] – 2026-06-03 to 2026-06-29

### Added

- Fallback implementation for Spark. (`e9bc582`)
- Updated framework version support. (`1c7cef8`)

### Fixed

- Fixed worker host list. (`6022876`)
- Fixed PyFlink integration. (`a90731e`)
- Removed trailing whitespace at end of lines. (`538e5bb`)
- Refactoring and corrections for tab management. (`d8f3102`)

### Changed

- Logging refactoring, removed magic extension, and cleaned up HTML generation. (`926ea52`)
- Turned off debug mode. (`29df11f`)
- Updated README. (`c73be29`, `c2c16b9`)

## [0.1.x] – 2026-05-08 to 2026-05-29

### Added

- Multi-framework support. (`62b8d11`)
- Provision to insert `LD_LIBRARY_PATH` into `spark-env.sh`. (`6a885da`)
- Missing `cmd.sh` for Spark. (`5a3001f`)
- Pika web interface redirect button to the `ProcessMonitor` dashboard, and exposed
  Slurm job name, start time, and partition metadata via `SlurmManager`. (`ebab1b7`)
- Interactive metric checkboxes in `ProcessMonitor` to toggle visible plots per process. (`50d1132`)
- `CustomCheckbox` widget with traitlet-based value linking and styled label positioning. (`50d1132`)
- `ContainerMixin` with enable/disable support for `VBox`/`HBox` dynamic GUI state. (`50d1132`)
- Use-case example and executed notebook. (`2d6964d`, `8be9db7`)

### Changed

- Relicensed from MIT to GNU GPL v3.0 or later, and added new Spark metrics
  (`total_disk_usage_mb`, `total_duration_ms`). (`0fc010e`)
- Renamed package from `big_data_utils` to `bijuty`; updated package metadata. (`5a3001f`)
- Refactored cluster control, monitoring, and metric collection:
  replaced hardcoded framework start/stop commands with a unified `cmd.sh`
  executed from the configuration directory; extracted process visualization from
  `process_monitor` into the new `metric_plotter` module; added
  `spark_metric_collector` with a new `metrics.properties` template wired into the
  GUI dashboard; only auto-display `MultiFrameworkManager` inside an IPython kernel. (`e9fc97b`)
- Reworked `FRAMEWORK_REGISTRY` process patterns from plain strings to structured
  dicts with `title` and `pattern` keys. (`50d1132`)
- Enabled/disabled Framework GUI and Performance Metric sections based on cluster
  start/stop state; fixed config template selection to respect custom templates;
  ensured config destination directory is created before framework setup. (`50d1132`)
- Improved CSS: disable/grayscale states, plot row styling, sub-container hover glow,
  and responsive layout; adjusted card HTML semantics and tightened the color palette. (`50d1132`)
- Updated CPU metric collection; disabled extra metric-panel buttons; added extra big
  data cluster info; modified log style and dashboard layout. (`dacb05e`, `c5d9dd3`)
- Fixed updating of elements based on the selected framework; fixed visualization panel;
  separated the styling file. (`3290574`)
- Modified labels and background visuals. (`62b8d11`)
- Code refactoring and reorganizing; cleaned up files; corrected file paths for
  framework configuration and package inclusion. (`065f68f`, `c745468`, `b8dbe26`, `5af009e`)
- Updated README, corrected GitHub link, added PySpark as a development requirement,
  and fixed demo.gif reference. (`8ae24d9`, `b002834`, `b0c7967`, `d79ac1b`)

### Fixed

- Fixed updating elements based on selected framework and visualization panel. (`3290574`)
- Corrected cleanup process extraction. (`c745468`)
- Fixed cluster info widget to show `"-"` when the cluster is stopped; updated metric
  display labels (memory, disk usage, execution time). (`0fc010e`)

### Removed

- Removed obsolete `ClusterConfig`, dead config-setup helpers, and the unused
  `style.css` from `big_data_manager`. (`e9fc97b`)

## [0.1.x] – 2026-03-05 to 2026-04-29

### Added

- Initial GUI for cluster configuration. (`041c01a`)
- Slurm utilities and refactoring. (`0c07bd0`)
- Process visualization and cluster buttons; GUI loading on import; GUI set as start point. (`cf9e328`)
- New features and code refactoring. (`c129e8c`)
- Option to add a custom Spark home path. (`9276caa`)
- Login node discovery. (`f70f0d0`)

### Fixed

- Fixed logging and output area. (`9276caa`)
- Fixed plot issue. (`a6bf12d`)
- Fixed framework GUI browser issue by opening it in a new browser window. (`e92f925`, `448f255`)
- Process metric printing debug. (`3c23f67`)

### Changed

- Cleanup and login node discovery. (`f70f0d0`)

## [0.1.x] – 2025-04-23 to 2025-04-24

### Added

- Re-added port-forwarding information. (`ce0271d`)

### Fixed

- Improved error handling for an existing Spark cluster. (`eddcc9b`, `9b4e6bf`, `50ac1a5`, `643dabe`)
- Improved check-status command. (`8573c40`, `739132b`, `ba883ec`)
- Improved error handling and cluster status checking. (`1033d7d`)
- Changed an erroneous debug log to info. (`770228c`)
- Minor improvements and code refactoring. (`1894260`, `e311fea`, `55fdadb`)

## [0.0.x] – 2024-05-16 to 2024-06-13

### Added

- Initial commit. (`78589ba`)
- `LICENSE`. (`838f516`)
- Cluster utilities to start/stop a cluster (`cluster_utils.py`). (`12354cb`)
- Utility functions in `utils.py`. (`2d56f0f`)
- Functionality to modify the worker file. (`7d1435b`)

### Changed

- Converted the environment setup function into a class for better management. (`43c7eff`)
- Configuration now returns a dictionary for further use. (`d9c2ba1`)
- Restructured the files. (`23c9a34`)
- Modified `setup.py` and README. (`445b6cb`, `6e60ed0`, `6a5f294`, `80fd8a1`, `43f703d`)
- Updated port-forwarding information and printed output. (`3d8de7e`, `7203b6b`, `a6ee7ce`, `0e54609`)
- General corrections. (`0f2f249`, `f64c4d4`, `840c953`, `ec73963`, `cd67316`, `1cdecf7`)

### Fixed

- Added missing libraries and logger for `cluster_utils.py`. (`4287547`, `ada5b5d`, `8e3b005`)
- Corrected a logging typo so the info message is formatted correctly. (`a1178ae`)

[Unreleased]: https://github.com/ScaDS/bijuty/compare/main...HEAD
