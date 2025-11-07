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

from .evaluator import (
    AERONET_API_BASE_URL,
    http_invoke,
    to_aeronet_api
)
from enum import (
    auto,
    Enum
)
from io import StringIO
from loguru import logger
from pandas import DataFrame
import click
import json
from .utils import to_geoparquet

class FilterLang(Enum):
    CQL2_JSON = auto()
    CQL2_TEXT = auto()

@click.group()
def main():
    pass

@main.command(context_settings={'show_default': True})
@click.argument(
    'url',
    type=click.STRING,
    required=True,
    default=AERONET_API_BASE_URL
)
@click.option(
    '--filter',
    type=click.STRING,
    required=True,
    help="Filter on queryables using language specified in filter-lang parameter"
)
@click.option(
    '--filter-lang',
    type=click.Choice(
        FilterLang,
        case_sensitive=False
    ),
    required=False,
    default=FilterLang.CQL2_JSON,
    help="Filter language used within the filter parameter"
)
@click.option(
    '--dry-run',
    type=click.BOOL,
    is_flag=True,
    default=False,
    help="Just print the invoking URL with the built filter and exits"
)
@click.option(
    "--format",
    type=click.Choice(["geoparquet", "csv"], case_sensitive=False),
    default="geoparquet",
    help="Output format (csv or geoparquet)"
)
@click.option(
    "--output-file",
    type=click.Path(writable=True, dir_okay=False, path_type=str),
    required=True,
    help="Output file path"
)
def search(
    url: str,
    filter: str,
    filter_lang: FilterLang,
    dry_run: bool,
    format: str, 
    output_file: str
):
    cql2_filter: str | dict = filter

    if FilterLang.CQL2_JSON == filter_lang:
        cql2_filter = json.loads(filter)

    if dry_run:
        print(to_aeronet_api(filter))
        return

    data: DataFrame | None = http_invoke(cql2_filter, url)

    logger.success(f"Query on {url} successfully obtained data:")

    if format == "geoparquet":
        to_geoparquet(data, output_file)
        logger.success(f"Data saved to GeoParquet file: {output_file}")
    else:
        data.to_csv(output_file, index=False)
        logger.success(f"Data saved to to CSV file: {output_file}")
    print(data)
