# Your first AERONET query

In this tutorial you will retrieve daily AOD observations for Cart_Site from 1–14 June 2000 and open the resulting GeoParquet file.

## Install the client

Use Python 3.10–3.13 and an internet connection. Create an environment and install the package:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install pygeofilter-aeronet
```

On Windows, activate with `.venv\Scripts\activate` instead.

## Build a filter

Save the following as `first_query.py`:

```python
from pathlib import Path
from geopandas import read_parquet
from pygeofilter_aeronet import aeronet_search, dry_run_aeronet_search

cql2_filter = {
    "op": "and",
    "args": [
        {"op": "eq", "args": [{"property": "site"}, "Cart_Site"]},
        {"op": "eq", "args": [{"property": "data_type"}, "AOD10"]},
        {"op": "eq", "args": [{"property": "format"}, "csv"]},
        {"op": "eq", "args": [{"property": "data_format"}, "daily-average"]},
        {"op": "t_after", "args": [
            {"property": "time"}, {"timestamp": "2000-06-01T00:00:00Z"}
        ]},
        {"op": "t_before", "args": [
            {"property": "time"}, {"timestamp": "2000-06-14T23:59:59Z"}
        ]},
    ],
}

dry_run_aeronet_search(cql2_filter)
```

Run `python first_query.py`. The log shows a URL containing `site=Cart_Site`, `AOD10=1`, and `AVG=20`, followed by the date parameters. This previews the observation request. Importing the package still accesses the remote station catalogue and initializes DuckDB's spatial extension.

## Download observations

Append this code to the same file and run it again:

```python
item = aeronet_search(cql2_filter, output_dir=Path("results"), timeout=30)
print(item.assets["csv"].href)
print(item.assets["geoparquet"].href)

observations = read_parquet(item.assets["geoparquet"].href)
print(observations.head())
```

The `results` directory now contains a CSV file and a GeoParquet file with the same generated identifier. The printed table includes AERONET measurement columns and point geometries. The returned STAC Item describes those files; its JSON is not automatically saved.

You have built a filter, previewed its request, and loaded the downloaded observations. Continue with [CLI workflows](../how-to/cli.md) or look up [supported filters](../reference/filters.md).
