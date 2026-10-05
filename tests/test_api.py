from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING
from unittest.mock import patch

import duckdb
import geopandas
import pandas
import pytest
from httpx import Timeout
from pystac import Asset, Item
from pystac.extensions.table import TableExtension

import pygeofilter_aeronet as aeronet

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def station() -> Item:
    return Item(
        id="GSFC",
        assets={"source": Asset(href="https://example.test/stations.csv", media_type="text/csv")},
        geometry={"type": "Point", "coordinates": [-76.8, 39.0]},
        bbox=[-76.8, 39.0, -76.8, 39.0],
        datetime=datetime(2025, 1, 1, tzinfo=timezone.utc),
        properties={"aeronet:site_name": "GSFC"},
    )


def test_filter_language_values() -> None:
    assert aeronet.FilterLang.CQL2_JSON.value == "cql2-json"
    assert aeronet.FilterLang.CQL2_TEXT.value == "cql2-text"


def test_dump_items_creates_parent_and_writes_parquet(tmp_path: Path, station: Item) -> None:
    output_file = tmp_path / "nested" / "stations.parquet"
    aeronet.dump_items([station], output_file)
    stored = geopandas.read_parquet(output_file)
    assert stored["id"].tolist() == ["GSFC"]
    assert stored.geometry.iloc[0].wkt == "POINT (-76.8 39)"


@pytest.mark.parametrize("filtered", [False, True])
def test_query_stations_uses_bound_path_and_optional_filter(station: Item, filtered: bool) -> None:
    expression = {"op": "=", "args": [{"property": "id"}, "GSFC"]} if filtered else None
    with (
        patch.object(duckdb, "execute") as execute,
        patch.object(aeronet, "stac_table_to_items", return_value=[station.to_dict()]) as convert,
    ):
        query, items = aeronet.query_stations_from_parquet("station's.parquet", expression)
    expected = "SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) AS geometry FROM read_parquet(?)"
    if filtered:
        expected += " WHERE (\"id\" = 'GSFC')"
    assert query == expected
    execute.assert_called_once_with(expected, ["station's.parquet"])
    convert.assert_called_once_with(execute.return_value.fetch_arrow_table.return_value)
    assert [item.to_dict() for item in items] == [station.to_dict()]


def test_read_site_list_extracts_names(station: Item) -> None:
    with patch.object(
        aeronet, "query_stations_from_parquet", return_value=("query", [station])
    ) as query:
        assert aeronet._read_aeronet_site_list() == ["GSFC"]
    query.assert_called_once_with(aeronet.DEFAULT_STATIONS_PARQUET_URL)


@pytest.mark.parametrize("verbose", [False, True])
def test_get_stations_converts_csv_to_stac(verbose: bool) -> None:
    csv = (
        "AERONET stations\n"
        "New_Site_ID,Name,Latitude(decimal_degrees),Longitude(decimal_degrees),Altitude(Meters),"
        "Data_Start_date(dd-mm-yyyy),Data_End_Date(dd-mm-yyyy),Land_Use_type,"
        "Number_of_days_L1,Number_of_days_L1.5,Number_of_days_L2,Number_of_days_Moon_L1.5\n"
        "GSFC_ID,GSFC,39,-76.8,87,01-02-2020,31-03-2025,urban,10,9,8,7\n"
    )
    with (
        patch.object(aeronet, "AeronetClient") as client_class,
        patch.object(aeronet, "get_stations", return_value=csv) as fetch,
        patch.object(aeronet, "verbose_client") as enable_verbose,
    ):
        items = aeronet.get_aeronet_stations(
            url="https://example.test", timeout=12, verbose=verbose
        )
    client_class.assert_called_once_with(base_url="https://example.test", timeout=Timeout(12))
    client = client_class.return_value.__enter__.return_value
    fetch.assert_called_once_with(client=client)
    assert enable_verbose.called is verbose
    if verbose:
        enable_verbose.assert_called_once_with(client.get_httpx_client.return_value)
    (item,) = items
    assert item.id == "GSFC_ID"
    assert item.bbox == [-76.8, 39, -76.8, 39]
    assert item.geometry == {"type": "Point", "coordinates": (-76.8, 39.0, 87.0)}
    assert item.properties["start_datetime"] == "2020-02-01T00:00:00Z"
    assert item.properties["end_datetime"] == "2025-03-31T00:00:00Z"
    assert item.properties["aeronet:site_name"] == "GSFC"
    assert item.properties["title"] == "GSFC"
    assert item.assets["source"].href == "https://example.test/aeronet_locations_extended_v3.txt"


def test_dry_run_logs_url_without_searching() -> None:
    with (
        patch.object(aeronet, "logger") as logger,
        patch.object(aeronet, "aeronet_client_search") as search,
    ):
        aeronet.dry_run_aeronet_search(
            {"op": "=", "args": [{"property": "format"}, "csv"]}, url="https://example.test"
        )
    logger.info.assert_called_once_with(
        "You can browse data on: https://example.test/cgi-bin/print_web_data_v3?if_no_html=1"
    )
    search.assert_not_called()


@pytest.mark.parametrize(
    "date_column,time_column",
    [
        ("Date(dd:mm:yyyy)", "Time(hh:mm:ss)"),
        ("Date_(dd:mm:yyyy)", "Time_(hh:mm:ss)"),
        ("Date(dd:mm:yyyy)", None),
        (None, None),
    ],
)
def test_search_writes_deduplicated_assets_and_metadata(
    tmp_path: Path, date_column: str | None, time_column: str | None
) -> None:
    columns = ["Site_Longitude(Degrees)", "Site_Latitude(Degrees)", "AOD_500nm"]
    first = ["10", "45", "0.1"]
    second = ["12", "47", "0.2"]
    if date_column:
        columns.append(date_column)
        first.append("02:01:2025")
        second.append("03:01:2025")
    if time_column:
        columns.append(time_column)
        first.append("03:04:05")
        second.append("06:07:08")
    csv = "metadata\n" * 5 + "\n".join(",".join(row) for row in [columns, first, first, second])
    output_dir = tmp_path / "results"
    with (
        patch.object(aeronet, "AeronetClient") as client_class,
        patch.object(aeronet, "aeronet_client_search", return_value=csv) as search,
        patch.object(aeronet, "verbose_client") as enable_verbose,
    ):
        item = aeronet.aeronet_search(
            {"op": "=", "args": [{"property": "format"}, "csv"]},
            output_dir,
            url="https://example.test",
            verbose=bool(time_column),
            timeout=15,
        )
    client_class.assert_called_once_with(base_url="https://example.test", timeout=Timeout(15))
    search.assert_called_once_with(
        client=client_class.return_value.__enter__.return_value, if_no_html=1
    )
    assert enable_verbose.called is bool(time_column)
    csv_data = pandas.read_csv(item.assets["csv"].href)
    parquet_data = geopandas.read_parquet(item.assets["geoparquet"].href)
    assert csv_data["AOD_500nm"].tolist() == [0.1, 0.2]
    assert parquet_data["AOD_500nm"].tolist() == [0.1, 0.2]
    assert parquet_data.crs is not None
    assert parquet_data.crs.to_authority() == ("EPSG", "4326")
    if date_column:
        expected = "2025-01-02 03:04:05" if time_column else "2025-01-02 00:00:00"
        assert str(parquet_data["datetime"].iloc[0]) == expected
    else:
        assert "datetime" not in parquet_data.columns
    assert item.bbox == [10.0, 45.0, 12.0, 47.0]
    assert item.geometry is not None
    assert item.geometry["type"] == "Polygon"
    for asset in item.assets.values():
        assert TableExtension.ext(asset).row_count == len(csv_data)
        assert asset.href.startswith(str(output_dir / item.id.removeprefix("urn:uuid:")))
    assert item.links[0].target == "https://example.test/cgi-bin/print_web_data_v3?if_no_html=1"
    assert TableExtension.has_extension(item)


def test_search_failure_does_not_create_output(tmp_path: Path) -> None:
    output_dir = tmp_path / "results"
    with (
        patch.object(aeronet, "AeronetClient"),
        patch.object(
            aeronet, "aeronet_client_search", side_effect=RuntimeError("service unavailable")
        ),
        pytest.raises(RuntimeError, match="service unavailable"),
    ):
        aeronet.aeronet_search({"op": "=", "args": [{"property": "format"}, "csv"]}, output_dir)
    assert not output_dir.exists()
