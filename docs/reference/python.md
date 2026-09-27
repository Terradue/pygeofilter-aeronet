# Python API reference

Import the functions below from `pygeofilter_aeronet`, except `to_aeronet_api`, which is in `pygeofilter_aeronet.evaluator`. Filters accept a CQL2 JSON string or a mapping.

| Function | Result / side effects |
| --- | --- |
| `to_aeronet_api(cql2_filter)` | Returns `(query_string, query_parameters)` for the generated HTTP client |
| `dry_run_aeronet_search(cql2_filter, url=AERONET_API_BASE_URL)` | Logs the observation URL; returns `None` |
| `aeronet_search(cql2_filter, output_dir, url=AERONET_API_BASE_URL, verbose=False, timeout=None)` | Writes CSV and GeoParquet; returns a `pystac.Item` with `csv` and `geoparquet` assets |
| `get_aeronet_stations(url=AERONET_API_BASE_URL, verbose=False, timeout=None)` | Downloads station metadata; returns a list of `pystac.Item` objects |
| `dump_items(items, output_file)` | Writes STAC Items to GeoParquet; returns `None` |
| `query_stations_from_parquet(file_path, cql2_filter=None)` | Returns `(sql_query, items)`; no filter selects all stations |

Pass `pathlib.Path` objects for `output_dir` and `output_file`, and a string path or URL for `file_path`. Python HTTP helpers default to no timeout; the CLI defaults to 30 seconds.

`AERONET_API_BASE_URL` is `https://aeronet.gsfc.nasa.gov`. `DEFAULT_STATIONS_PARQUET_URL` points to `https://github.com/Terradue/pygeofilter-aeronet/raw/refs/heads/stations-update/stations.parquet`.

Importing the package installs/loads DuckDB's spatial extension and reads the hosted station catalogue to validate site names. Even query translation and dry runs therefore require those resources.

For a complete working example, follow [Your first AERONET query](../tutorials/first-query.md).
