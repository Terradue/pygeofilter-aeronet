# Command Line Interface

`pygeofilter-aeronet` can be used as a Command Line Interface (CLI).

```
$ aeronet-client --help

Usage: aeronet-client [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  dump-stations
  query-stations
  search
```

## Dump AERONET Stations

```
$ aeronet-client dump-stations --help
Usage: aeronet-client dump-stations [OPTIONS] URL

Options:
  --output-file FILE  Output file path  [required]
  --verbose           Traces the HTTP protocol.
  --help              Show this message and exit.
```

i.e.

```
$ aeronet-client dump-stations --output-file=./pygeofilter_aeronet/data/aeronet_locations_extended_v3.parquet
2025-11-12 12:51:02.075 | INFO     | pygeofilter_aeronet.cli:wrapper:59 - Started at: 2025-11-12T12:51:02.075
2025-11-12 12:51:02.927 | INFO     | pygeofilter_aeronet:get_aeronet_stations:104 - Converting CSV data to STAC Items:
2025-11-12 12:51:03.039 | SUCCESS  | pygeofilter_aeronet:get_aeronet_stations:153 - CSV data converted to STAC Items
2025-11-12 12:51:03.040 | INFO     | pygeofilter_aeronet:dump_items:84 - Converting the STAC Items pyarrow Table...
2025-11-12 12:51:03.103 | SUCCESS  | pygeofilter_aeronet:dump_items:87 - STAC Items converted to pyarrow Table
2025-11-12 12:51:03.103 | INFO     | pygeofilter_aeronet:dump_items:89 - Saving the GeoParquet data to /home/stripodi/Documents/pygeofilter/pygeofilter-aeronet/pygeofilter_aeronet/data/aeronet_locations_extended_v3.parquet...
2025-11-12 12:51:03.106 | SUCCESS  | pygeofilter_aeronet:dump_items:92 - GeoParquet data saved to /home/stripodi/Documents/pygeofilter/pygeofilter-aeronet/pygeofilter_aeronet/data/aeronet_locations_extended_v3.parquet
2025-11-12 12:51:03.107 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:64 - ------------------------------------------------------------------------
2025-11-12 12:51:03.107 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:65 - SUCCESS
2025-11-12 12:51:03.107 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:66 - ------------------------------------------------------------------------
2025-11-12 12:51:03.107 | INFO     | pygeofilter_aeronet.cli:wrapper:75 - Total time: 1.0318 seconds
2025-11-12 12:51:03.107 | INFO     | pygeofilter_aeronet.cli:wrapper:76 - Finished at: 2025-11-12T12:51:03.107
```

## Query the AERONET Stations

## Search

```
$ aeronet-client search --help
Usage: aeronet-client search [OPTIONS] URL

Options:
  --filter TEXT                   Filter on queryables using language
                                  specified in filter-lang parameter
                                  [required]
  --filter-lang [cql2-json|cql2-text]
                                  Filter language used within the filter
                                  parameter  [default: cql2-json]
  --dry-run                       Just print the invoking URL with the built
                                  filter and exits
  --output-dir DIRECTORY          Output file path  [required]
  --verbose                       Traces the HTTP protocol.
  --help                          Show this message and exit.
```

i.e.

```
$ aeronet-client search \
--filter-lang cql2-json \
--filter '{"op":"and","args":[{"op":"eq","args":[{"property":"site"},"Cart_Site"]},{"op":"eq","args":[{"property":"data_type"},"AOD20"]},{"op":"eq","args":[{"property":"format"},"csv"]},{"op":"eq","args":[{"property":"data_format"},"daily-average"]},{"op":"t_after","args":[{"property":"time"},{"timestamp":"2000-06-01T00:00:00Z"}]},{"op":"t_before","args":[{"property":"time"},{"timestamp":"2000-06-14T23:59:59Z"}]}]}' \
 --output-dir .

2025-11-12 12:57:22.097 | INFO     | pygeofilter_aeronet.cli:wrapper:59 - Started at: 2025-11-12T12:57:22.097
2025-11-12 12:57:23.076 | SUCCESS  | pygeofilter_aeronet:aeronet_search:196 - Query on https://aeronet.gsfc.nasa.gov successfully obtained data:
2025-11-12 12:57:23.077 | SUCCESS  | pygeofilter_aeronet:aeronet_search:204 - Data saved to to CSV file: /home/stripodi/Documents/pygeofilter/pygeofilter-aeronet/0d0ebbb5-4c36-436b-af73-c6202008e99f.csv
2025-11-12 12:57:23.086 | SUCCESS  | pygeofilter_aeronet:aeronet_search:215 - Data saved to GeoParquet file: /home/stripodi/Documents/pygeofilter/pygeofilter-aeronet/0d0ebbb5-4c36-436b-af73-c6202008e99f.parquet
{
  "type": "Feature",
  "stac_version": "1.1.0",
  "stac_extensions": [],
  "id": "urn:uuid:0d0ebbb5-4c36-436b-af73-c6202008e99f",
  "geometry": {
    "type": "Point",
    "coordinates": [
      -97.48639,
      36.60667
    ]
  },
  "bbox": [
    -97.48639,
    36.60667,
    -97.48639,
    36.60667
  ],
  "properties": {
    "datetime": "2025-11-12T12:57:23.124952Z"
  },
  "links": [
    {
      "rel": "related",
      "href": "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?site=Cart_Site&AOD20=1&if_no_html=1&AVG=20&year=2000&month=6&day=1&hour=0&year2=2000&month2=6&day2=14&hour2=23",
      "type": "text/csv",
      "title": "AERONET Web Service search"
    }
  ],
  "assets": {
    "csv": {
      "href": "0d0ebbb5-4c36-436b-af73-c6202008e99f.csv",
      "type": "text/csv",
      "description": "Search result - CVS Format"
    },
    "geoparquet": {
      "href": "0d0ebbb5-4c36-436b-af73-c6202008e99f.parquet",
      "type": "application/vnd.apache.parquet",
      "description": "Search result - GeoParquet Format"
    }
  }
}
2025-11-12 12:57:23.126 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:64 - ------------------------------------------------------------------------
2025-11-12 12:57:23.126 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:65 - SUCCESS
2025-11-12 12:57:23.126 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:66 - ------------------------------------------------------------------------
2025-11-12 12:57:23.126 | INFO     | pygeofilter_aeronet.cli:wrapper:75 - Total time: 1.0294 seconds
2025-11-12 12:57:23.126 | INFO     | pygeofilter_aeronet.cli:wrapper:76 - Finished at: 2025-11-12T12:57:23.126
```
