
# pygeofilter-aeronet

[![Documentation](https://img.shields.io/badge/docs-online-blue.svg)](https://terradue.github.io/pygeofilter-aeronet/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

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

## Quick example

```python
import pandas as pd
from io import StringIO
from pygeofilter_aeronet.evaluator import (
    to_aeronet_api_querystring,
    http_invoke
)

cql2_filter = {
            "op": "and",
            "args": [
                {"op": "eq", "args": [{"property": "site"}, "Cart_Site"]},
                {"op": "eq", "args": [{"property": "data_type"}, "AOD10"]},
                {"op": "eq", "args": [{"property": "format"}, "csv"]},
                {"op": "eq", "args": [{"property": "data_format"}, "daily-average"]},
                {
                    "op": "t_after",
                    "args": [
                        {"property": "time"},
                        {"timestamp": "2023-02-01T00:00:00Z"},
                    ],
                },
                {
                    "op": "t_before",
                    "args": [
                        {"property": "time"},
                        {"timestamp": "2023-02-28T23:59:59Z"},
                    ],
                },
            ],
        }

# print the AERONET API querystring:
print(to_aeronet_api(cql2_filter=cql2_filter))

# dry-run the HTTP request:
http_invoke(cql2_filter=cql2_filter, dry_run=True)

# query the AERONET API
raw_data = http_invoke(cql2_filter=cql2_filter, dry_run=False)
df = pd.read_csv(StringIO(raw_data), skiprows=5)

print(df.head(5))
```

## Documentation

User guide and examples available at: [https://terradue.github.io/pygeofilter-aeronet/](https://terradue.github.io/pygeofilter-aeronet/)

## Development

```console
git clone https://github.com/Terradue/pygeofilter-aeronet.git
cd pygeofilter-aeronet
hatch shell
```

