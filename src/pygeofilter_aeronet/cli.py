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
from enum import Enum, auto
from pathlib import Path

import click
from loguru import logger
from pandas import DataFrame

from .evaluator import AERONET_API_BASE_URL, http_invoke, to_aeronet_api
from .utils import to_geoparquet


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
def search(
    url: str,
    filter: str,
    filter_lang: FilterLang,
    dry_run: bool,
    format: str,
    output_file: Path,
):
    cql2_filter: str | dict = filter

    if FilterLang.CQL2_JSON == filter_lang:
        cql2_filter = json.loads(filter)

    if dry_run:
        logger.info(f"You can browse data on: {url}?{to_aeronet_api(filter)}")
        return

    try:
        data: DataFrame = http_invoke(cql2_filter, url)

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
