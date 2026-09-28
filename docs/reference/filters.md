# Filter reference

Observation filters are CQL2 JSON mappings or JSON strings. The observation evaluator supports the following translations; this is not a complete CQL2 implementation.

| Operator / property | Accepted values | AERONET parameter |
| --- | --- | --- |
| `and` | Supported expressions | Joins query parameters |
| `eq` / `site` | Name in the hosted station catalogue | `site` |
| `eq` / `data_type` | `AOD10`, `AOD15`, `AOD20`, `SDA10`, `SDA15`, `SDA20`, `TOT10`, `TOT15`, `TOT20` | Product name set to `1` |
| `eq` / `format` | `csv`, `html` | `if_no_html=1` or `0` |
| `eq` / `data_format` | `all-points`, `daily-average` | `AVG=10` or `20` |
| `t_after` / `time` | Timestamp | `year`, `month`, `day`, `hour` |
| `t_before` / `time` | Timestamp | `year2`, `month2`, `day2`, `hour2` |
| `s_intersects` / `geometry` | GeoJSON geometry | Bounding box: `lon1`, `lat1`, `lon2`, `lat2` |

Use UTC timestamps. Minutes and seconds are discarded when producing API parameters. Observation spatial filtering uses the geometry's bounding box, rather than its exact outline. See [query processing](../explanation/index.md).

`format=html` is available for URL generation; `aeronet_search` expects CSV. Invalid values for the enumerated properties raise `ValueError`. Repeated constraints on the same parameter do not implement arbitrary Boolean logic; use one product, aggregation, and time range per request.

Station filters instead use the pygeofilter DuckDB backend against GeoParquet columns, such as `geometry`, `aeronet:site_name`, and `aeronet:land_use_type`. The observation property `site` is not the station column name. See [the station-query example](../how-to/cli.md#find-stations-inside-a-polygon).
