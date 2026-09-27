# Download observations and query stations with the CLI

Install `pygeofilter-aeronet` and use CQL2 JSON filters. See the [command reference](../cli.md) for all arguments and defaults.

## Preview an observation request

In a Bash-compatible shell, store your filter in a variable:

```bash
filter='{"op":"and","args":[{"op":"eq","args":[{"property":"site"},"Cart_Site"]},{"op":"eq","args":[{"property":"data_type"},"AOD20"]},{"op":"eq","args":[{"property":"format"},"csv"]},{"op":"eq","args":[{"property":"data_format"},"daily-average"]},{"op":"t_after","args":[{"property":"time"},{"timestamp":"2000-06-01T00:00:00Z"}]},{"op":"t_before","args":[{"property":"time"},{"timestamp":"2000-06-14T23:59:59Z"}]}]}'
aeronet-client search --dry-run --filter-lang cql2-json --filter "$filter"
```

The log contains a URL you can open in a browser. No observation files are written. Package initialization still requires the station catalogue and DuckDB spatial extension.

## Save observations and their STAC metadata

Using the filter above:

```bash
aeronet-client search --filter "$filter" --output-dir ./results > item.json
```

The command writes CSV and GeoParquet assets into `results`; stdout is the STAC Item saved here as `item.json`. Diagnostic logs go to stderr. Asset paths are relative to the working directory when a relative output directory is used.

## Create a station catalogue

```bash
aeronet-client dump-stations --output-file ./stations.parquet
```

This downloads the station list and saves STAC station records in GeoParquet. Parent directories are created as needed.

## Find stations inside a polygon

Query the catalogue created above:

```bash
aeronet-client query-stations ./stations.parquet \
  --filter-lang cql2-json \
  --filter '{"op":"s_intersects","args":[{"property":"geometry"},{"type":"Polygon","coordinates":[[[7.5,47.5],[10.5,47.5],[10.5,49.8],[7.5,49.8],[7.5,47.5]]]}]}' \
  --format stac > stations.json
```

`stations.json` contains a STAC ItemCollection. Choose `--format jsonl` for one station Item per line. Omit `./stations.parquet` to use the default hosted catalogue.
