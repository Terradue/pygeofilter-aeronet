# How queries become geospatial data

The client has two query paths. Observation searches translate CQL2 JSON into AERONET Web Service parameters. Station discovery translates CQL2 JSON into SQL evaluated by DuckDB against a GeoParquet catalogue.

## Observation searches

The evaluator adapts supported CQL2 expressions to the upstream API's parameter model. This makes filters convenient to use alongside geospatial workflows, but does not give the upstream service every CQL2 operation. In particular, spatial intersection becomes a bounding box, and time limits are reduced to whole hours.

A polygon search can therefore include observations outside the polygon's exact outline. Workflows needing an exact footprint must filter the downloaded points afterward, as the [satellite matchup notebook](../use_cases/matchup.ipynb) demonstrates. Time comparisons likewise need further filtering if sub-hour precision matters.

## Station discovery

Station records describe sites, including location, coverage dates, and AERONET properties. DuckDB's spatial backend filters their geometries directly. A station inside a satellite footprint is a candidate for an observation search; its presence does not guarantee measurements for the acquisition time.

This distinction motivates the [station-first matchup workflow](../use_cases/stations_matchup.ipynb): discover candidate stations, then request observations for each station and time window.

## CSV, GeoParquet, and STAC

Observation responses are parsed into tabular data and deduplicated. CSV preserves the measurement table. GeoParquet adds point geometry in EPSG:4326 and a parsed observation datetime where date columns are available.

A returned STAC Item connects those two assets and describes their extent and table columns. Its `datetime` records Item creation time; observation times belong to the data rows. Station catalogue Items likewise carry station coverage dates separately from their creation datetime.

For the implementation structure, see [software design](../design.md). For exact function contracts, see [Python reference](../reference/python.md).
