# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added pre-commit hooks and CI quality gates for mypy, Ruff, Bandit, and pytest
  coverage.
- Added strict release-tag validation against the package version before publishing.

### Changed

- Modernized type annotations and imports across the package.
- Updated the package workflow to test supported Python versions before publishing.
- Updated the project documentation URL and README badges.

### Fixed

- Parameterized the GeoParquet path passed to DuckDB when querying stations.
- Correctly remove duplicate station geometries before creating the result
  `MultiPoint`.
- Raise `ValueError` instead of relying on an assertion when a CQL2 filter contains
  an unsupported value.

## [0.12.0] - 2026-04-29

### Fixed

- Run the scheduled station-dump command through Hatch so the project environment
  is used consistently.
- Increased the scheduled station-dump timeout to avoid `httpx.ConnectTimeout`
  failures.

## [0.11.0] - 2026-04-28

This is the first tagged release. It consolidates the project history from its
initial development through April 2026.

### Added

- Added CQL2 JSON evaluation for AERONET site, product, output-format, spatial, and
  temporal filters.
- Added APIs and a command-line interface for searching AERONET observations,
  discovering stations, and dumping station metadata.
- Added CSV and GeoParquet output, including STAC Items, the STAC Table extension,
  and an AERONET STAC extension.
- Added station queries from published GeoParquet data and station/match-up use
  cases.
- Added Docker and CWL packaging, automated station-data updates, package publishing,
  tests, and documentation workflows.
- Added configurable AERONET API base URLs, HTTP verbosity, dry-run support, and
  request timeouts.

### Changed

- Separated the public APIs from the CLI and made the evaluator responsible for
  translating CQL2 expressions.
- Reworked the HTTP transport and CSV parsing for more robust AERONET responses.
- Standardized dependencies, build tasks, quality checks, and release packaging.
- Relicensed the project under the Apache License 2.0.
- Limited supported Python versions to 3.10 through 3.13 because of the pinned
  `pygeofilter-duckdb` dependency constraints.

### Fixed

- Corrected station longitude/latitude handling and restricted spatial results to
  points inside the requested geometry.
- Fixed date and time parsing for supported AERONET column-name variants, including
  minute fields.
- Fixed STAC links, row counts, and extension attachment in generated results.
- Fixed station querying, generated-client signatures and types, package module
  naming, documentation builds, and station-update automation.

[Unreleased]: https://github.com/Terradue/pygeofilter-aeronet/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/Terradue/pygeofilter-aeronet/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/Terradue/pygeofilter-aeronet/releases/tag/v0.11.0
