# Command-line reference

The installed entry point is `aeronet-client`. Run `aeronet-client COMMAND --help` for command help. For procedures, see [CLI workflows](how-to/cli.md).

## Commands

| Command | Purpose |
| --- | --- |
| `search [OPTIONS] [URL]` | Download observations and print a STAC Item |
| `dump-stations [OPTIONS] [URL]` | Download stations into GeoParquet |
| `query-stations [OPTIONS] [FILE_PATH]` | Filter a station catalogue |

`URL` defaults to `https://aeronet.gsfc.nasa.gov` and can also be supplied through `AERONET_API_BASE_URL`. An explicit argument takes precedence. `FILE_PATH` accepts a local path or URL and defaults to the repository's hosted `stations-update/stations.parquet` catalogue.

## Shared search and station-query options

| Option | Default | Meaning |
| --- | --- | --- |
| `--filter TEXT` | Required | CQL2 filter |
| `--filter-lang [cql2-json|cql2-text]` | `cql2-json` | Advertised input language selector |
| `--help` | | Show help and exit |

Use `cql2-json`: both current backends parse CQL2 JSON. Although the CLI advertises `cql2-text`, it does not currently select a text parser.

## Search options

| Option | Default | Meaning |
| --- | --- | --- |
| `--dry-run` | Off | Log the observation URL without downloading observations |
| `--output-dir DIRECTORY` | Current directory | Directory for CSV and GeoParquet assets |
| `--verbose` | Off | Trace HTTP traffic |
| `--timeout INTEGER` | `30` | HTTP timeout in seconds |

A normal search prints a STAC Item as JSON on stdout. Dry-run output is logged on stderr. Use `format=csv` in the filter because downloaded data is parsed as CSV.

## Dump-stations options

| Option | Default | Meaning |
| --- | --- | --- |
| `--output-file FILE` | Required | Destination GeoParquet file |
| `--verbose` | Off | Trace HTTP traffic |
| `--timeout INTEGER` | `30` | HTTP timeout in seconds |
| `--help` | | Show help and exit |

## Query-stations options

| Option | Default | Meaning |
| --- | --- | --- |
| `--format [jsonl|stac]` | `jsonl` | One Item per line or a STAC ItemCollection |

Diagnostics are logged on stderr. The current command wrapper logs caught exceptions as `FAIL` without re-raising them, so an exit status of zero alone does not establish that a download succeeded.
