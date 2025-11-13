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

from .aeronet_client.models.search_avg import SearchAVG
from datetime import (
    date,
    datetime
)
from dateutil import parser as date_parser
from pygeofilter import ast, values
from pygeofilter.backends.evaluator import Evaluator, handle
from pygeofilter.parsers.cql2_json import parse as json_parse
from pygeofilter.util import IdempotentDict
from typing import (
    Any,
    Mapping,
    MutableMapping,
    Optional,
    Tuple
)

import json
import numbers
import shapely


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

SUPPORTED_VALUES = {
    "format": ["csv", "html"],
    "data_type": AERONET_DATA_TYPES,
    "site": [],
    "data_format": ["all-points", "daily-average"],
}


class AeronetEvaluator(Evaluator):
    def __init__(
        self,
        attribute_map: Mapping[str, str],
        function_map: Optional[Mapping[str, str]] = None
    ):
        self.attribute_map = attribute_map
        self.function_map = function_map

        self.query_parameters: MutableMapping[str, Any] = {}

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
            assert rhs in supported_values, (
                f"'{rhs}' is not supported value for '{lhs}', expected one of {supported_values}"
            )

        is_value_supported = rhs in TRUE_VALUE_LIST

        if lhs in ["format"]:
            self.query_parameters['if_no_html'] = 1 if 'csv' == rhs else 0
            return "if_no_html=1" if rhs == "csv" else "if_no_html=0"

        if lhs in ["data_format"]:
            avg = SearchAVG.VALUE_20 if 'daily-average' == rhs else SearchAVG.VALUE_10
            self.query_parameters['avg'] = avg
            return f"AVG={avg}"

        if is_value_supported:
            self.query_parameters[rhs.lower()] = 1
            return f"{rhs}=1"
        
        self.query_parameters[lhs.lower()] = rhs
        return f"{lhs}={rhs}"

    @handle(ast.And)
    def combination(self, node, lhs, rhs):
        return f"{lhs}&{rhs}"

    @handle(ast.TimeAfter)
    def timeAfter(self, node, lhs, rhs):
        date = date_parser.parse(str(rhs))

        self.query_parameters['year'] = date.year
        self.query_parameters['month'] = date.month
        self.query_parameters['day'] = date.day
        self.query_parameters['hour'] = date.hour

        return f"year={date.year}&month={date.month}&day={date.day}&hour={date.hour}"

    @handle(ast.TimeBefore)
    def timeBefore(self, node, lhs, rhs):
        date = date_parser.parse(str(rhs))

        self.query_parameters['year2'] = date.year
        self.query_parameters['month2'] = date.month
        self.query_parameters['day2'] = date.day
        self.query_parameters['hour2'] = date.hour

        return f"year2={date.year}&month2={date.month}&day2={date.day}&hour2={date.hour}"

    @handle(values.Geometry)
    def geometry(self, node: values.Geometry):
        jeometry = json.dumps(node.geometry)
        geometry = shapely.from_geojson(jeometry)
        return shapely.from_wkt(str(geometry)).bounds

    @handle(ast.GeometryIntersects, subclasses=True)
    def geometry_intersects(self, node, lhs, rhs):
        # note for maintainers:
        # we evaluate as the bounding box of the geometry
        self.query_parameters['lon1'] = rhs[0]
        self.query_parameters['lat1'] = rhs[1]
        self.query_parameters['lon2'] = rhs[2]
        self.query_parameters['lat2'] = rhs[3]

        return f"lon1={rhs[0]}&lat1={rhs[1]}&lon2={rhs[2]}&lat2={rhs[3]}"

def to_aeronet_api(
    cql2_filter: str | Mapping[str, Any]
) -> Tuple[str, Mapping[str, Any]]:
    evaluator: AeronetEvaluator = AeronetEvaluator(IdempotentDict())
    root: ast.AstType = json_parse(cql2_filter) # type: ignore
    querystring: str = evaluator.evaluate(root)
    query_parameters: Mapping[str, Any] = evaluator.query_parameters
    return (querystring, query_parameters)
