"""Isolate package import from remote catalogues and DuckDB extension downloads."""

from importlib import import_module
from unittest.mock import patch

from stac_geoparquet.arrow import stac_table_to_items

with (
    patch("duckdb.install_extension"),
    patch("duckdb.load_extension"),
    patch("duckdb.execute"),
    patch("stac_geoparquet.arrow.stac_table_to_items", return_value=[]),
):
    aeronet = import_module("pygeofilter_aeronet")

# The package imports this function by name, so restore its binding as well.
vars(aeronet)["stac_table_to_items"] = stac_table_to_items
