# How-to guides

These guides assume you can construct a CQL2 JSON filter. For a first exercise, use the [tutorial](../tutorials/first-query.md).

- [Download observations and query stations with the CLI](cli.md).
- [Set up development and preview documentation](development.md).

## Query and visualize observations in notebooks

Use an installed notebook environment with `pygeofilter-aeronet`, `jupyterlab`, and `folium`. Run cells in order; network access and a writable working directory are required.

- [Retrieve Level 1.5 AOD from all sites](../samples/level_15.ipynb).
- [Retrieve Level 2.0 AOD daily averages](../samples/level_20_aod.ipynb).
- [Retrieve Level 2.0 SDA daily averages](../samples/level_20_sda.ipynb).
- [Search observations within a geographic bounding box](../samples/geo_search.ipynb).

## Match observations to satellite imagery

These notebooks also fetch a Sentinel-3 STAC Item from the Copernicus Data Space catalogue. Replace the example Item URL to work with another scene.

- [Discover stations in a scene footprint](../use_cases/stations.ipynb).
- [Find observations around a satellite acquisition](../use_cases/matchup.ipynb).
- [Discover stations, then query each station](../use_cases/stations_matchup.ipynb).

For the different spatial behavior of station and observation queries, see [query processing](../explanation/index.md).
