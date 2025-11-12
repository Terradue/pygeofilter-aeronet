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
from .aeronet_client.api.default.get_stations import sync as get_stations
from .evaluator import to_aeronet_api
from .utils import (
    to_geoparquet,
    query_from_parquet
)
from datetime import datetime
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
from pystac import (
    Asset,
    Extent,
    Item,
    ItemCollection
)
from shapely.geometry import (
    Point,
    mapping
) 
from stac_geoparquet.arrow import (
    parse_stac_items_to_arrow,
    to_parquet
)
from typing import (
    List,
    Mapping
)
import click
import json
import os
import sys
import time    
import uuid


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


class SearchOutputFormat(Enum):
    GEOPARQUET = auto()
    CSV = auto()


class QueryOutputFormat(Enum):
    JSONL = auto()
    STAC = auto()


def _track(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()

        logger.info(f"Started at: {datetime.fromtimestamp(start_time).isoformat(timespec='milliseconds')}")

        try:
            func(*args, **kwargs)

            logger.success('------------------------------------------------------------------------')
            logger.success('SUCCESS')
            logger.success('------------------------------------------------------------------------')
        except Exception as e:
            logger.error('------------------------------------------------------------------------')
            logger.error('FAIL')
            logger.error(e)
            logger.error('------------------------------------------------------------------------')

        end_time = time.time()

        logger.info(f"Total time: {end_time - start_time:.4f} seconds")
        logger.info(f"Finished at: {datetime.fromtimestamp(end_time).isoformat(timespec='milliseconds')}")

    return wrapper


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
    type=click.Choice(SearchOutputFormat, case_sensitive=False),
    default=SearchOutputFormat.GEOPARQUET.name.lower(),
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
        if SearchOutputFormat.GEOPARQUET == format:
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
    "--output-file",
    type=click.Path(writable=True, dir_okay=False, path_type=Path),
    required=True,
    default=Path(os.path.join(os.path.dirname(__file__), "data", "aeronet_locations_extended_v3.parquet")),
    help="Output file path",
)
@click.option(
    "--verbose",
    is_flag=True,
    required=False,
    default=False,
    help="Traces the HTTP protocol."
)
def dump_stations(
    url: str,
    output_file: Path,
    verbose: bool
):
    with AeronetClient(base_url=url) as aeronet_client:
        if verbose:
            http_client: Client = aeronet_client.get_httpx_client()
            http_client.build_request = _log_request(http_client.build_request) # type: ignore
            http_client.request = _log_response(http_client.request) # type: ignore
        raw_data = get_stations(client=aeronet_client)
        data_frame: DataFrame = read_csv(StringIO(raw_data), skiprows=1)

        logger.info('Converting CSV data to STAC Items:')

        items: List[Item] = []

        for _, row in data_frame.iterrows():
            def _to_date(column: str):
                return datetime.strptime(row[column], "%d-%m-%Y")

            latitude = row['Latitude(decimal_degrees)']
            longitude = row['Longitude(decimal_degrees)']
            altitude = row['Altitude(Meters)']
            start_datetime = _to_date('Data_Start_date(dd-mm-yyyy)')
            end_datetime = _to_date('Data_End_Date(dd-mm-yyyy)')

            current_item: Item = Item(
                id=row['New_Site_ID'],
                stac_extensions=[
                    'https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json'
                ],
                assets={
                    'source': Asset(
                        href=f"{url}/aeronet_locations_extended_v3.txt",
                        media_type='text/csv',
                        description='Data source'
                    )
                },
                bbox=[
                    longitude,
                    latitude,
                    longitude,
                    latitude
                ],
                datetime=datetime.now(),
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                geometry=mapping(Point([longitude, latitude, altitude])),
                properties={
                    'title': row['Name'],
                    'aeronet:site_name': row['Name'],
                    'aeronet:land_use_type': row['Land_Use_type'],
                    'aeronet:L10': row['Number_of_days_L1'],
                    'aeronet:L15': row['Number_of_days_L1.5'],
                    'aeronet:L20': row['Number_of_days_L2'],
                    'aeronet:moon_L20': row['Number_of_days_Moon_L1.5'],
                }
            )

            items.append(current_item)

        logger.success('CSV data converted to STAC Items')

        logger.info('Converting the STAC Items pyarrow Table...')
        record_batch_reader = parse_stac_items_to_arrow(items)
        table = record_batch_reader.read_all()
        logger.success('STAC Items converted to pyarrow Table')

        logger.info(f"Saving the GeoParquet data to {output_file.absolute()}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        to_parquet(table, output_path=output_file)
        logger.success(f"GeoParquet data saved to {output_file.absolute()}")


def _support_datetime_serialization(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


@main.command(context_settings={"show_default": True})
@click.argument(
    "file_path",
    type=click.Path(writable=True, dir_okay=False, path_type=Path),
    required=True
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
    "--format",
    type=click.Choice(QueryOutputFormat, case_sensitive=False),
    default=QueryOutputFormat.JSONL.name.lower(),
    help="Output format",
)
def query(
    file_path: Path,
    filter: str,
    filter_lang: FilterLang,
    format: QueryOutputFormat
):
    cql2_filter: str | dict = filter

    if FilterLang.CQL2_JSON == filter_lang:
        cql2_filter = json.loads(filter)

    sql_query, items = query_from_parquet(file_path, cql2_filter)

    logger.info(f"Filtered data with `{sql_query}` query on {file_path.absolute()} parquet file:")

    match format:
        case QueryOutputFormat.JSONL:
            for item in items:
                json.dump(item.to_dict(), sys.stdout, default=_support_datetime_serialization)
                print()

        case QueryOutputFormat.STAC:
            collection: ItemCollection = ItemCollection(
                items=items
            )
            json.dump(
                collection.to_dict(),
                sys.stdout,
                default=_support_datetime_serialization,
                indent=2
            )

        case _:
            logger.error(f"It's not you, it's us: output format {format} not supported")

for command in [dump_stations, query, search]:
    command.callback = _track(command.callback)
