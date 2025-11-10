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

import json
import numbers
import os
from datetime import date, datetime
from io import StringIO
from typing import Any, Mapping, MutableMapping, Optional, Sequence

import shapely

from .aeronet_client import Client as AeronetClient
from .aeronet_client.models.search_avg import SearchAVG
from .aeronet_client.api.default.search import sync as aeronet_search
from functools import wraps
from http import HTTPStatus
from httpx import (
    Client,
    Headers,
    Request,
    RequestNotRead,
    Response
)
from loguru import logger
from pandas import DataFrame, read_csv
from pygeofilter import ast, values
from pygeofilter.backends.evaluator import Evaluator, handle
from pygeofilter.parsers.cql2_json import parse as json_parse
from pygeofilter.util import IdempotentDict

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
        data_frame: DataFrame = read_csv(file, skiprows=1)
        for _, row in data_frame.iterrows():
            site_list.append(row["Site_Name"])

    return site_list


AERONET_API_BASE_URL = "https://aeronet.gsfc.nasa.gov"

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
        date = datetime.strptime(str(rhs), "%Y-%m-%dT%H:%M:%SZ")

        self.query_parameters['year'] = date.year
        self.query_parameters['month'] = date.month
        self.query_parameters['day'] = date.day
        self.query_parameters['hour'] = date.hour

        return f"year={date.year}&month={date.month}&day={date.day}&hour={date.hour}"

    @handle(ast.TimeBefore)
    def timeBefore(self, node, lhs, rhs):
        date = datetime.strptime(str(rhs), "%Y-%m-%dT%H:%M:%SZ")

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


def to_aeronet_api_querystring(
    root: ast.AstType,
    field_mapping: Mapping[str, str],
    function_map: Optional[Mapping[str, str]] = None,
) -> str:
    return AeronetEvaluator(field_mapping, function_map).evaluate(root)


def to_aeronet_api(cql2_filter: str | dict) -> str:
    return to_aeronet_api_querystring(json_parse(cql2_filter), IdempotentDict())


def _decode(value):
    if not value:
        return ''

    if isinstance(value, str):
        return value

    return value.decode("utf-8")


def _log_request(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        request: Request = func(*args, **kwargs)

        logger.warning(f"{request.method} {request.url}")

        headers: Headers = request.headers
        for name, value in headers.raw:
            logger.warning(f"> {_decode(name)}: {_decode(value)}")

        logger.warning('>')
        try:
            if request.content:
                logger.warning(_decode(request.content))
        except RequestNotRead as r:
            logger.warning('[REQUEST BUILT FROM STREAM, OMISSING]')

        return request
    return wrapper


def _log_response(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response: Response = func(*args, **kwargs)

        if HTTPStatus.MULTIPLE_CHOICES._value_ <= response.status_code:
            log = logger.error
        else:
            log = logger.success

        status: HTTPStatus = HTTPStatus(response.status_code)
        log(f"< {status._value_} {status.phrase}")

        headers: Mapping[str, str] = response.headers
        for name, value in headers.items():
            log(f"< {_decode(name)}: {_decode(value)}")

        log('')

        if response.content:
            log(_decode(response.content))

        if HTTPStatus.MULTIPLE_CHOICES._value_ <= response.status_code:
            raise RuntimeError(f"A server error occurred when invoking {kwargs['method'].upper()} {kwargs['url']}, read the logs for details")
        return response
    return wrapper


def http_invoke(
    cql2_filter: str | dict,
    base_url: str = AERONET_API_BASE_URL
) -> DataFrame:
    evaluator: AeronetEvaluator = AeronetEvaluator(IdempotentDict())
    string_filter = evaluator.evaluate(json_parse(cql2_filter))

    logger.debug(f"AERONET service filter: {string_filter}")

    query_parameters = evaluator.query_parameters

    with AeronetClient(base_url=base_url) as aeronet_client:
        http_client: Client = aeronet_client.get_httpx_client()
        http_client.build_request = _log_request(http_client.build_request) # type: ignore
        http_client.request = _log_response(http_client.request) # type: ignore
        raw_data = aeronet_search(client=aeronet_client, **query_parameters)

    return read_csv(StringIO(raw_data), skiprows=5)
