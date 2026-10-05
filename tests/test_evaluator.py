from __future__ import annotations

import json
from datetime import date, datetime

import pytest
from pygeofilter import ast

from pygeofilter_aeronet.aeronet_client.models.search_avg import SearchAVG
from pygeofilter_aeronet.evaluator import (
    AERONET_DATA_TYPES,
    SUPPORTED_VALUES,
    AeronetEvaluator,
    to_aeronet_api,
)


@pytest.mark.parametrize("data_type", AERONET_DATA_TYPES)
def test_data_types_become_enabled_flags(data_type: str) -> None:
    assert to_aeronet_api({"op": "=", "args": [{"property": "data_type"}, data_type]}) == (
        f"{data_type}=1",
        {data_type.lower(): 1},
    )


@pytest.mark.parametrize(
    "property_name,value,query,parameters",
    [
        ("format", "csv", "if_no_html=1", {"if_no_html": 1}),
        ("format", "html", "if_no_html=0", {"if_no_html": 0}),
        ("data_format", "daily-average", "AVG=20", {"avg": SearchAVG.VALUE_20}),
        ("data_format", "all-points", "AVG=10", {"avg": SearchAVG.VALUE_10}),
        ("CUSTOM", "value", "CUSTOM=value", {"custom": "value"}),
        ("flag", "lunar_merge", "lunar_merge=1", {"lunar_merge": 1}),
    ],
)
def test_equal_translates_parameters(
    property_name: str, value: str, query: str, parameters: dict[str, str | int]
) -> None:
    assert to_aeronet_api({"op": "=", "args": [{"property": property_name}, value]}) == (
        query,
        parameters,
    )


@pytest.mark.parametrize("property_name", ["format", "data_format", "data_type", "site"])
def test_rejects_unsupported_values(property_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(SUPPORTED_VALUES, "site", ["GSFC"])
    with pytest.raises(ValueError, match=f"not supported value for '{property_name}'"):
        to_aeronet_api({"op": "=", "args": [{"property": property_name}, "invalid"]})


def test_conjunction_and_json_string_input(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(SUPPORTED_VALUES, "site", ["GSFC"])
    expression = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "site"}, "GSFC"]},
            {"op": "=", "args": [{"property": "format"}, "csv"]},
        ],
    }
    assert to_aeronet_api(json.dumps(expression)) == (
        "site=GSFC&if_no_html=1",
        {"site": "GSFC", "if_no_html": 1},
    )
    assert to_aeronet_api({"op": "=", "args": [{"property": "format"}, "html"]}) == (
        "if_no_html=0",
        {"if_no_html": 0},
    )


def test_temporal_interval() -> None:
    expression = {
        "op": "and",
        "args": [
            {
                "op": "t_after",
                "args": [{"property": "time"}, {"timestamp": "2023-02-01T04:05:06Z"}],
            },
            {
                "op": "t_before",
                "args": [{"property": "time"}, {"timestamp": "2024-03-31T23:59:59Z"}],
            },
        ],
    }
    assert to_aeronet_api(expression) == (
        "year=2023&month=2&day=1&hour=4&year2=2024&month2=3&day2=31&hour2=23",
        {
            "year": 2023,
            "month": 2,
            "day": 1,
            "hour": 4,
            "year2": 2024,
            "month2": 3,
            "day2": 31,
            "hour2": 23,
        },
    )


def test_geometry_uses_enclosing_bounds() -> None:
    expression = {
        "op": "s_intersects",
        "args": [
            {"property": "geometry"},
            {
                "type": "Polygon",
                "coordinates": [[[10, 45], [12, 46], [11, 47], [10, 45]]],
            },
        ],
    }
    assert to_aeronet_api(expression) == (
        "lon1=10.0&lat1=45.0&lon2=12.0&lat2=47.0",
        {"lon1": 10.0, "lat1": 45.0, "lon2": 12.0, "lat2": 47.0},
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        (2, 2),
        (2.5, 2.5),
        (True, True),
        ("GSFC", "GSFC"),
        (date(2025, 1, 2), "2025-01-02T00:00:00"),
        (datetime(2025, 1, 2, 3, 4, 5), "2025-01-02T03:04:05"),
    ],
)
def test_literal_conversion(value: object, expected: object) -> None:
    assert AeronetEvaluator({}).literal(value) == expected


def test_attribute_mapping() -> None:
    evaluator = AeronetEvaluator({"station": "site"})
    assert evaluator.attribute(ast.Attribute("station")) == "site"
    with pytest.raises(KeyError, match="unknown"):
        evaluator.attribute(ast.Attribute("unknown"))


def test_rejects_unsupported_operator() -> None:
    with pytest.raises(NotImplementedError):
        to_aeronet_api({"op": ">", "args": [{"property": "site"}, "GSFC"]})
