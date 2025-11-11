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

from .aeronet_client import Client as AeronetClient
from .aeronet_client.api.default.search import sync as aeronet_search
from .evaluator import to_aeronet_api
from .utils import to_geoparquet
from enum import Enum, auto
from functools import wraps
from http import HTTPStatus
from httpx import (
    Client,
    Headers,
    Request,
    RequestNotRead,
    Response
)
from io import StringIO
from loguru import logger
from pandas import (
    DataFrame,
    read_csv
)
from pathlib import Path

import json
import click

AERONET_API_BASE_URL = "https://aeronet.gsfc.nasa.gov"


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


class FilterLang(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name.lower().replace("_", "-")

    CQL2_JSON = auto()
    CQL2_TEXT = auto()


class OutputFormat(Enum):
    GEOPARQUET = auto()
    CSV = auto()


@click.group()
def main():
    pass


@main.command(context_settings={"show_default": True})
@click.argument(
    "url",
    type=click.STRING,
    required=True,
    envvar="AERONET_API_BASE_URL",
    default=AERONET_API_BASE_URL,
)
@click.option(
    "--filter",
    type=click.STRING,
    required=True,
    help="Filter on queryables using language specified in filter-lang parameter",
)
@click.option(
    "--filter-lang",
    type=click.Choice([f.value for f in FilterLang], case_sensitive=False),
    required=False,
    default=FilterLang.CQL2_JSON.value,
    help="Filter language used within the filter parameter",
)
@click.option(
    "--dry-run",
    type=click.BOOL,
    is_flag=True,
    default=False,
    help="Just print the invoking URL with the built filter and exits",
)
@click.option(
    "--format",
    type=click.Choice(OutputFormat, case_sensitive=False),
    default=OutputFormat.GEOPARQUET.name.lower(),
    help="Output format",
)
@click.option(
    "--output-file",
    type=click.Path(writable=True, dir_okay=False, path_type=Path),
    required=True,
    help="Output file path",
)
@click.option(
    "--verbose",
    is_flag=True,
    required=False,
    default=False,
    help="Traces the HTTP protocol."
)
def search(
    url: str,
    filter: str,
    filter_lang: FilterLang,
    dry_run: bool,
    format: str,
    output_file: Path,
    verbose: bool
):
    cql2_filter: str | dict = filter

    if FilterLang.CQL2_JSON == filter_lang:
        cql2_filter = json.loads(filter)

    filter, query_parameters = to_aeronet_api(cql2_filter)

    if dry_run:
        logger.info(f"You can browse data on: {url}?{to_aeronet_api(filter)}")
        return

    try:
        with AeronetClient(base_url=url) as aeronet_client:
            if verbose:
                http_client: Client = aeronet_client.get_httpx_client()
                http_client.build_request = _log_request(http_client.build_request) # type: ignore
                http_client.request = _log_response(http_client.request) # type: ignore
            raw_data = aeronet_search(client=aeronet_client, **query_parameters)
            data: DataFrame = read_csv(StringIO(raw_data), skiprows=5)

        logger.success(f"Query on {url} successfully obtained data:")

        print(data)

        output_file.parent.mkdir(parents=True, exist_ok=True)
        if OutputFormat.GEOPARQUET == format:
            to_geoparquet(data, output_file)
            logger.success(f"Data saved to GeoParquet file: {output_file.absolute()}")
        else:
            data.to_csv(output_file, index=False)
            logger.success(f"Data saved to to CSV file: {output_file.absolute()}")
    except Exception as e:
        logger.error(e)

@main.command(context_settings={"show_default": True})
@click.argument(
    "url",
    type=click.STRING,
    required=True,
    envvar="AERONET_API_BASE_URL",
    default=AERONET_API_BASE_URL,
)
@click.option(
    "--verbose",
    is_flag=True,
    required=False,
    default=False,
    help="Traces the HTTP protocol."
)
def dump_stations():

    pass
