# Copyright 2025 Terradue
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import os
import json
import numbers
import requests
import shapely
from datetime import date, datetime
from pygeofilter import ast, values
from pygeofilter.backends.evaluator import Evaluator, handle
from pygeofilter.parsers.cql2_json import parse as json_parse
from pygeofilter.util import IdempotentDict
from typing import Mapping, Optional, Sequence


def read_aeronet_site_list(filepath: str) -> Sequence[str]:
    """
    Example of AERONET site list file content:

    AERONET_Database_Site_List,Num=2,Date_Generated=06:11:2025
    Site_Name,Longitude(decimal_degrees),Latitude(decimal_degrees),Elevation(meters)
    Cuiaba,-56.070214,-15.555244,234.000000
    Alta_Floresta,-56.104453,-9.871339,277.000000
    Jamari,-63.068552,-9.199070,129.000000
    Tucson,-110.953003,32.233002,779.000000
    GSFC,-76.839833,38.992500,87.000000
    Kolfield,-74.476387,39.802223,50.000000
    """

    site_list = []
    with open(filepath) as file:
        next(file)

        csv_reader = csv.DictReader(file)
        for line in csv_reader:
            site_list.append(line['Site_Name'])

    return site_list


AERONET_API_BASE_URL = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3"

AERONET_DATA_TYPES = [
    "AOD10",
    "AOD15",
    "AOD20",
    "SDA10",
    "SDA15",
    "SDA20",
    "TOT10",
    "TOT15",
    "TOT20",
]

TRUE_VALUE_LIST = [
    *AERONET_DATA_TYPES,
    "if_no_html",
    "lunar_merge",
]  # values that need <parameter>=1

AERONET_SITE_LIST = read_aeronet_site_list(
    os.path.join(os.path.dirname(__file__), "data", "aeronet_locations_v3.txt")
)

SUPPORTED_VALUES = {
    "format": ["csv", "html"],
    "data_type": AERONET_DATA_TYPES,
    "site": AERONET_SITE_LIST,
    "data_format": ["all-points", "daily-average"],
}


class AeronetEvaluator(Evaluator):
    def __init__(self, attribute_map: Mapping[str, str], function_map: Mapping[str, str]):
        self.attribute_map = attribute_map
        self.function_map = function_map

    @handle(ast.Attribute)
    def attribute(self, node: ast.Attribute):
        return self.attribute_map[node.name]

    @handle(*values.LITERALS)
    def literal(self, node):
        if isinstance(node, numbers.Number):
            return node
        elif isinstance(node, date) or isinstance(node, datetime):
            return node.strftime(
                "%Y-%m-%dT%H:%M:%S%Z"
            )  # Implicit UTC timezone, explicit not supported by the backend
        else:
            # TODO:
            return str(node)

    @handle(ast.Equal)
    def equal(self, node, lhs, rhs):
        supported_values = SUPPORTED_VALUES.get(lhs)

        if supported_values is not None:
            assert (
                rhs in supported_values
            ), f"'{rhs}' is not supported value for '{lhs}', expected one of {supported_values}"

        is_value_supported = rhs in TRUE_VALUE_LIST

        if lhs in ["format"]:
            return f"if_no_html=1" if rhs == "csv" else f"if_no_html=0"

        if lhs in ["data_format"]:
            return f"AVG=20" if rhs == "daily-average" else "AVG=10"

        if is_value_supported:
            return f"{rhs}=1"
        return f"{lhs}={rhs}"

    @handle(ast.And)
    def combination(self, node, lhs, rhs):
        return f"{lhs}&{rhs}"

    @handle(ast.TimeAfter)
    def timeAfter(self, node, lhs, rhs):
        date = datetime.strptime(str(rhs), "%Y-%m-%dT%H:%M:%SZ")
        return f"year={date.year}&month={date.month}&day={date.day}&hour={date.hour}&minute={date.minute}"

    @handle(ast.TimeBefore)
    def timeBefore(self, node, lhs, rhs):
        date = datetime.strptime(str(rhs), "%Y-%m-%dT%H:%M:%SZ")
        return f"year2={date.year}&month2={date.month}&day2={date.day}&hour2={date.hour}&minute2={date.minute}"

    @handle(values.Geometry)
    def geometry(self, node: values.Geometry):
        jeometry = json.dumps(node.geometry)
        geometry = shapely.from_geojson(jeometry)
        return shapely.from_wkt(str(geometry)).bounds

    @handle(ast.GeometryIntersects, subclasses=True)
    def geometry_intersects(self, node, lhs, rhs):
        # note for maintainers:
        # we evaluate as the bounding box of the geometry
        return f"lon1={rhs[0]}&lat1={rhs[1]}&lon2={rhs[2]}&lat2={rhs[3]}"


def to_aeronet_api_querystring(
    root: ast.AstType,
    field_mapping: Mapping[str, str],
    function_map: Optional[Mapping[str, str]] = None,
) -> str:
    return AeronetEvaluator(field_mapping, function_map or {}).evaluate(root)


def to_aeronet_api(cql2_filter: str | dict) -> str:
    return to_aeronet_api_querystring(json_parse(cql2_filter), IdempotentDict())


def http_invoke(
    cql2_filter: str | dict,
    base_url: str = AERONET_API_BASE_URL,
    dry_run: bool = False,
) -> str:
    current_filter = to_aeronet_api(cql2_filter)
    url = f"{base_url}?{current_filter}"
    if dry_run:
        print(url)
        return ''

    response = requests.get(url)
    response.raise_for_status()  # Raise an error for HTTP error codes
    data = response.text

    return data
