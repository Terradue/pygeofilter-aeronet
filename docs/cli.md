# Command Line Interface

## Installation

```
pip install pygeofilter-aeronet
```

## Usage

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

### Dump AERONET Stations

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
$ aeronet-client \
dump-stations \
--output-file=./pygeofilter_aeronet/data/aeronet_locations_extended_v3.parquet

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

### Query the AERONET Stations

```
$ aeronet-client query-stations --help
Usage: aeronet-client query-stations [OPTIONS] FILE_PATH

Options:
  --filter TEXT                   Filter on queryables using language
                                  specified in filter-lang parameter
                                  [required]
  --filter-lang [cql2-json|cql2-text]
                                  Filter language used within the filter
                                  parameter  [default: cql2-json]
  --format [jsonl|stac]           Output format  [default: jsonl]
  --help                          Show this message and exit.
```

i.e.

```
aeronet-client \
query-stations \
--filter-lang cql2-json \
--filter '{"op":"and","args":[{"op":"s_intersects","args":[{"property":"geometry"},{"type":"Polygon","coordinates":[[[7.5113934084,47.5338000528],[10.4918239143,47.5338000528],[10.4918239143,49.7913749328],[7.5113934084,49.7913749328],[7.5113934084,47.5338000528]]]}]}]}' \
--format stac

2025-11-12 17:06:38.391 | INFO     | pygeofilter_aeronet.cli:wrapper:60 - Started at: 2025-11-12T17:06:38.391
2025-11-12 17:06:39.425 | INFO     | pygeofilter_aeronet.cli:query_stations:234 - Filtered data with `SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) as geometry FROM 'https://github.com/Terradue/pygeofilter-aeronet/raw/refs/heads/stations-update/stations.parquet' WHERE ST_Intersects("geometry",ST_GeomFromHEXEWKB('0103000000010000000500000034DFB1B6AA0B1E4085B0648F53C44740509E1658D0FB244085B0648F53C44740509E1658D0FB244006A017C64BE5484034DFB1B6AA0B1E4006A017C64BE5484034DFB1B6AA0B1E4085B0648F53C44740'))` query on https://github.com/Terradue/pygeofilter-aeronet/raw/refs/heads/stations-update/stations.parquet parquet file:
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM0435FRA",
      "geometry": {
        "type": "Point",
        "coordinates": [
          7.62475,
          48.335139,
          167.0
        ]
      },
      "bbox": [
        7.62475,
        48.335139,
        7.62475,
        48.335139
      ],
      "properties": {
        "aeronet:L10": 28,
        "aeronet:L15": 27,
        "aeronet:L20": 27,
        "aeronet:land_use_type": "Croplands",
        "aeronet:moon_L20": 0,
        "aeronet:site_name": "Rossfeld",
        "title": "Rossfeld",
        "end_datetime": "2003-06-17T02:00:00.000000Z",
        "start_datetime": "2003-05-15T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:01.787419Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERB0525DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          8.4279,
          49.0933,
          140.0
        ]
      },
      "bbox": [
        8.4279,
        49.0933,
        8.4279,
        49.0933
      ],
      "properties": {
        "aeronet:L10": 3792,
        "aeronet:L15": 2486,
        "aeronet:L20": 2173,
        "aeronet:land_use_type": "Urban_and_Built-Up",
        "aeronet:moon_L20": 357,
        "aeronet:site_name": "Karlsruhe",
        "title": "Karlsruhe",
        "end_datetime": "2025-11-12T01:00:00.000000Z",
        "start_datetime": "2005-03-21T01:00:00.000000Z",
        "datetime": "2025-11-12T10:15:01.801543Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM0640FRA",
      "geometry": {
        "type": "Point",
        "coordinates": [
          7.5425,
          48.442778,
          161.0
        ]
      },
      "bbox": [
        7.5425,
        48.442778,
        7.5425,
        48.442778
      ],
      "properties": {
        "aeronet:L10": 22,
        "aeronet:L15": 15,
        "aeronet:L20": 15,
        "aeronet:land_use_type": "Croplands",
        "aeronet:moon_L20": 0,
        "aeronet:site_name": "OBERNAI",
        "title": "OBERNAI",
        "end_datetime": "2008-02-12T01:00:00.000000Z",
        "start_datetime": "2007-05-29T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:01.819720Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM0687DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          8.396867,
          48.54005,
          511.0
        ]
      },
      "bbox": [
        8.396867,
        48.54005,
        8.396867,
        48.54005
      ],
      "properties": {
        "aeronet:L10": 36,
        "aeronet:L15": 16,
        "aeronet:L20": 16,
        "aeronet:land_use_type": "Evergreen_Needleleaf_Forest",
        "aeronet:moon_L20": 0,
        "aeronet:site_name": "Black_Forest_AMF",
        "title": "Black_Forest_AMF",
        "end_datetime": "2007-12-31T01:00:00.000000Z",
        "start_datetime": "2007-09-13T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:01.827096Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERB1290DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          8.4279,
          49.0933,
          140.0
        ]
      },
      "bbox": [
        8.4279,
        49.0933,
        8.4279,
        49.0933
      ],
      "properties": {
        "aeronet:L10": 87,
        "aeronet:L15": 76,
        "aeronet:L20": 0,
        "aeronet:land_use_type": "Urban_and_Built-Up",
        "aeronet:moon_L20": 18,
        "aeronet:site_name": "KITcube_Karlsruhe",
        "title": "KITcube_Karlsruhe",
        "end_datetime": "2015-10-05T02:00:00.000000Z",
        "start_datetime": "2015-06-03T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:01.947976Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM1507DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          10.314133,
          47.715833,
          718.0
        ]
      },
      "bbox": [
        10.314133,
        47.715833,
        10.314133,
        47.715833
      ],
      "properties": {
        "aeronet:L10": 180,
        "aeronet:L15": 142,
        "aeronet:L20": 142,
        "aeronet:land_use_type": "Grasslands",
        "aeronet:moon_L20": 0,
        "aeronet:site_name": "KEMPTEN_UAS",
        "title": "KEMPTEN_UAS",
        "end_datetime": "2019-07-24T02:00:00.000000Z",
        "start_datetime": "2018-08-15T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:01.982111Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM1622DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          7.8486,
          48.0011,
          323.5
        ]
      },
      "bbox": [
        7.8486,
        48.0011,
        7.8486,
        48.0011
      ],
      "properties": {
        "aeronet:L10": 149,
        "aeronet:L15": 104,
        "aeronet:L20": 104,
        "aeronet:land_use_type": "Urban_and_Built-Up",
        "aeronet:moon_L20": 58,
        "aeronet:site_name": "Freiburg_ALU",
        "title": "Freiburg_ALU",
        "end_datetime": "2021-06-15T02:00:00.000000Z",
        "start_datetime": "2020-10-20T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:02.000190Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM1638DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          8.955358,
          48.489074,
          339.0
        ]
      },
      "bbox": [
        8.955358,
        48.489074,
        8.955358,
        48.489074
      ],
      "properties": {
        "aeronet:L10": 146,
        "aeronet:L15": 128,
        "aeronet:L20": 128,
        "aeronet:land_use_type": "Croplands",
        "aeronet:moon_L20": 19,
        "aeronet:site_name": "KITcube_Rottenburg",
        "title": "KITcube_Rottenburg",
        "end_datetime": "2021-10-06T02:00:00.000000Z",
        "start_datetime": "2021-04-27T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:02.002698Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM1742DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          8.48647,
          48.05614,
          765.0
        ]
      },
      "bbox": [
        8.48647,
        48.05614,
        8.48647,
        48.05614
      ],
      "properties": {
        "aeronet:L10": 67,
        "aeronet:L15": 57,
        "aeronet:L20": 57,
        "aeronet:land_use_type": "Croplands",
        "aeronet:moon_L20": 14,
        "aeronet:site_name": "KITcube_VS",
        "title": "KITcube_VS",
        "end_datetime": "2023-10-10T02:00:00.000000Z",
        "start_datetime": "2023-05-30T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:02.019005Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERB 1793DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          8.67465,
          49.41727,
          144.0
        ]
      },
      "bbox": [
        8.67465,
        49.41727,
        8.67465,
        49.41727
      ],
      "properties": {
        "aeronet:L10": 187,
        "aeronet:L15": 146,
        "aeronet:L20": 146,
        "aeronet:land_use_type": "Urban_and_Built-Up",
        "aeronet:moon_L20": 91,
        "aeronet:site_name": "IUP_Heidelberg_DE",
        "title": "IUP_Heidelberg_DE",
        "end_datetime": "2025-05-16T02:00:00.000000Z",
        "start_datetime": "2024-08-16T02:00:00.000000Z",
        "datetime": "2025-11-12T10:15:02.027017Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    },
    {
      "type": "Feature",
      "stac_version": "1.1.0",
      "stac_extensions": [
        "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"
      ],
      "id": "ERM1813DEU",
      "geometry": {
        "type": "Point",
        "coordinates": [
          8.10794,
          48.59729,
          199.0
        ]
      },
      "bbox": [
        8.10794,
        48.59729,
        8.10794,
        48.59729
      ],
      "properties": {
        "aeronet:L10": -999,
        "aeronet:L15": -999,
        "aeronet:L20": -999,
        "aeronet:land_use_type": "Mixed_Forest",
        "aeronet:moon_L20": -999,
        "aeronet:site_name": "KLOCX_Kappelrodeck",
        "title": "KLOCX_Kappelrodeck",
        "end_datetime": "1970-01-01T01:00:00.000000Z",
        "start_datetime": "1970-01-01T01:00:00.000000Z",
        "datetime": "2025-11-12T10:15:02.030132Z"
      },
      "links": [],
      "assets": {
        "source": {
          "href": "https://aeronet.gsfc.nasa.gov/aeronet_locations_extended_v3.txt",
          "type": "text/csv",
          "description": "Data source"
        }
      }
    }
  ]
2025-11-12 17:06:39.427 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:65 - ------------------------------------------------------------------------
2025-11-12 17:06:39.427 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:66 - SUCCESS
2025-11-12 17:06:39.427 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:67 - ------------------------------------------------------------------------
2025-11-12 17:06:39.427 | INFO     | pygeofilter_aeronet.cli:wrapper:76 - Total time: 1.0355 seconds
2025-11-12 17:06:39.427 | INFO     | pygeofilter_aeronet.cli:wrapper:77 - Finished at: 2025-11-12T17:06:39.427
```

### Search

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
$ aeronet-client \
search \
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

#### Dry Run search

It may happens users just want to browse data on screen:

```
$ aeronet-client \
search \
--dry-run \
--filter-lang cql2-json \
--filter '{"op":"and","args":[{"op":"eq","args":[{"property":"site"},"Cart_Site"]},{"op":"eq","args":[{"property":"data_type"},"AOD20"]},{"op":"eq","args":[{"property":"format"},"csv"]},{"op":"eq","args":[{"property":"data_format"},"daily-average"]},{"op":"t_after","args":[{"property":"time"},{"timestamp":"2000-06-01T00:00:00Z"}]},{"op":"t_before","args":[{"property":"time"},{"timestamp":"2000-06-14T23:59:59Z"}]}]}'

2025-11-13 10:37:54.650 | INFO     | pygeofilter_aeronet.cli:wrapper:61 - Started at: 2025-11-13T10:37:54.650
2025-11-13 10:37:54.650 | WARNING  | pygeofilter_aeronet.cli:search:148 - DRY RUN: True
2025-11-13 10:37:54.678 | INFO     | pygeofilter_aeronet:dry_run_aeronet_search:214 - You can browse data on: https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?site=Cart_Site&AOD20=1&if_no_html=1&AVG=20&year=2000&month=6&day=1&hour=0&year2=2000&month2=6&day2=14&hour2=23
2025-11-13 10:37:54.678 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:66 - ------------------------------------------------------------------------
2025-11-13 10:37:54.678 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:67 - SUCCESS
2025-11-13 10:37:54.678 | SUCCESS  | pygeofilter_aeronet.cli:wrapper:68 - ------------------------------------------------------------------------
2025-11-13 10:37:54.678 | INFO     | pygeofilter_aeronet.cli:wrapper:77 - Total time: 0.0273 seconds
2025-11-13 10:37:54.678 | INFO     | pygeofilter_aeronet.cli:wrapper:78 - Finished at: 2025-11-13T10:37:54.678
```
