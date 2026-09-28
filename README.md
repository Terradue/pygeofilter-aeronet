
# pygeofilter-aeronet

[![Documentation](https://img.shields.io/badge/docs-online-blue.svg)](https://terradue.github.io/pygeofilter-aeronet/)
[![PyPI - Version](https://img.shields.io/pypi/v/pygeofilter-aeronet.svg)](https://pypi.org/project/pygeofilter-aeronet)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pygeofilter-aeronet.svg)](https://pypi.org/project/pygeofilter-aeronet)
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/terradue/pygeofilter-aeronet/package.yaml?branch=develop&event=push&label=build&logo=githubactions)](https://github.com/terradue/pygeofilter-aeronet/actions/workflows/package.yaml?query=branch%3Adevelop)
[![Code coverage](https://img.shields.io/codecov/c/github/terradue/pygeofilter-aeronet/develop?logo=codecov)](https://app.codecov.io/gh/terradue/pygeofilter-aeronet/tree/develop)

**pygeofilter-aeronet** provides a [pygeofilter](https://github.com/geopython/pygeofilter) extension for querying NASA’s [AERONET](https://aeronet.gsfc.nasa.gov/) aerosol optical depth datasets through the [AERONET Web Service v3 API](https://aeronet.gsfc.nasa.gov/print_web_data_help_v3.html).

It enables filtering AERONET observations using the same spatial and temporal operators as OGC APIs (CQL2 filters), making it easier to integrate AERONET data in geospatial workflows, data lakes, and cloud pipelines.

## Features

- Evaluate **CQL2 expressions** (spatial, temporal, and attribute filters) directly on AERONET datasets  
- Parse and normalize AERONET text responses into **GeoPandas DataFrames**  
- Support for:
  - `AOD10`, `AOD15`, `AOD20` — Aerosol Optical Depth (Levels 1.0–2.0)
  - `SDA10`, `SDA15`, `SDA20` — Size Distribution Analysis
  - `TOT10`, `TOT15`, `TOT20` — Total Optical Depth
- Simple API for combining AERONET product types and date ranges
- Compatible with **pygeofilter**, **pandas**, and **geopandas**

## Installation

```bash
pip install pygeofilter-aeronet
```

or directly from GitHub:

```
pip install git+https://github.com/Terradue/pygeofilter-aeronet.git
```

## Documentation

The [documentation site](https://terradue.github.io/pygeofilter-aeronet/) is organized by purpose:

- [Tutorials](docs/tutorials/index.md): complete your first query.
- [How-to guides](docs/how-to/index.md): download data, discover stations, and match satellite acquisitions.
- [Reference](docs/reference/index.md): CLI options, supported filters, and Python functions.
- [Explanation](docs/explanation/index.md): query processing and software design.

Start with [Your first AERONET query](docs/tutorials/first-query.md) for an executable Python example.

## Development

See [development setup and documentation preview](docs/how-to/development.md).

## License

[![Apache License, Version 2.0](https://img.shields.io/badge/license-Apache%20License%202.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)
