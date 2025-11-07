# Command Line Interface

`pygeofilter-aeronet` can be used as a Command Line Interface (CLI).

```
$ aeronet-client --help
Usage: aeronet-client [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  search
```

## Search

```
$ aeronet-client search --help
Usage: aeronet-client search [OPTIONS] URL

Options:
  --filter TEXT                   Filter on queryables using language
                                  specified in filter-lang parameter
                                  [required]
  --filter-lang [cql2_json|cql2_text]
                                  Filter language used within the filter
                                  parameter  [default: cql2_json]
  --dry-run                       Just print the invoking URL with the built
                                  filter and exits
  --format [geoparquet|csv]       Output format  [default: geoparquet]
  --output-file FILE              Output file path  [required]
  --help                          Show this message and exit
```

i.e.

```
$ aeronet-client search \
--format geoparquet \
--output-file output.parquet \
--filter-lang cql2_json \
--filter '{"op":"and","args":[{"op":"eq","args":[{"property":"site"},"Cart_Site"]},{"op":"eq","args":[{"property":"data_type"},"AOD20"]},{"op":"eq","args":[{"property":"format"},"csv"]},{"op":"eq","args":[{"property":"data_format"},"daily-average"]},{"op":"t_after","args":[{"property":"time"},{"timestamp":"2000-06-01T00:00:00Z"}]},{"op":"t_before","args":[{"property":"time"},{"timestamp":"2000-06-14T23:59:59Z"}]}]}' --format csv --output-file output.csv

2025-11-07 16:01:23.356 | SUCCESS  | pygeofilter_aeronet.cli:search:112 - Query on https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3 successfully obtained data:
2025-11-07 16:01:23.373 | SUCCESS  | pygeofilter_aeronet.cli:search:117 - Data saved to GeoParquet file: /home/user/Documents/pygeofilter/pygeofilter-aeronet/output.parquet
   AERONET_Site Date(dd:mm:yyyy) Time(hh:mm:ss)  ...  Site_Latitude(Degrees)  Site_Longitude(Degrees)  Site_Elevation(m)
0     Cart_Site       31:05:2000       12:00:00  ...                36.60667                -97.48639              318.0
1     Cart_Site       01:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
2     Cart_Site       04:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
3     Cart_Site       05:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
4     Cart_Site       06:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
5     Cart_Site       07:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
6     Cart_Site       08:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
7     Cart_Site       09:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
8     Cart_Site       11:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
9     Cart_Site       12:06:2000       12:00:00  ...                36.60667                -97.48639              318.0
10    Cart_Site       13:06:2000       12:00:00  ...                36.60667                -97.48639              318.0

[11 rows x 82 columns]
```
