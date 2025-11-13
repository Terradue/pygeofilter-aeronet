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

from . import (
    AERONET_API_BASE_URL,
    DEFAULT_STATIONS_PARQUET_URL,
    FilterLang,
    aeronet_search,
    dry_run_aeronet_search,
    dump_items,
    get_aeronet_stations,
    query_stations_from_parquet
)
from .utils import json_dump
from .aeronet_client import Client as AeronetClient
from .evaluator import to_aeronet_api
from datetime import datetime
from enum import (
    Enum,
    auto
)
from functools import wraps
from loguru import logger
from pathlib import Path
from pystac import (
    Item,
    ItemCollection
)
from typing import (
    List,
    Mapping
)

import click
import json
import os
import time    


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

def _parse_filter(
    filter: str,
    filter_lang: FilterLang
) -> str | Mapping[str, Mapping]:
    cql2_filter: str | Mapping[str, Mapping] = filter

    if FilterLang.CQL2_JSON == filter_lang:
        cql2_filter = json.loads(filter)
    
    return cql2_filter

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
    "--output-dir",
    type=click.Path(writable=True, file_okay=False, dir_okay=True, path_type=Path),
    default=Path('.'),
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
    output_dir: Path,
    verbose: bool
):
    logger.warning(f"DRY RUN: {dry_run}")

    cql2_filter: str | Mapping[str, Mapping] = _parse_filter(filter=filter, filter_lang=filter_lang)

    if dry_run:
        dry_run_aeronet_search(
            url=url,
            cql2_filter=cql2_filter
        )
        return

    current_item: Item | None = aeronet_search(
        url=url,
        cql2_filter=cql2_filter,
        output_dir=output_dir,
        verbose=verbose
    )

    if current_item:
        json_dump(
            obj=current_item.to_dict(),
            pretty_print=True
        )

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
    items: List[Item] = get_aeronet_stations(
        url=url,
        verbose=verbose
    )

    dump_items(
        items=items,
        output_file=output_file
    )        

@main.command(context_settings={"show_default": True})
@click.argument(
    "file_path",
    type=click.STRING,
    required=True,
    default=DEFAULT_STATIONS_PARQUET_URL
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
def query_stations(
    file_path: str,
    filter: str,
    filter_lang: FilterLang,
    format: QueryOutputFormat
):  
    cql2_filter: str | Mapping[str, Mapping] = _parse_filter(filter=filter, filter_lang=filter_lang)

    sql_query, items = query_stations_from_parquet(file_path, cql2_filter)

    logger.info(f"Filtered data with `{sql_query}` query on {file_path} parquet file:")

    match format:
        case QueryOutputFormat.JSONL:
            for item in items:
                json_dump(item.to_dict())
                print()

        case QueryOutputFormat.STAC:
            collection: ItemCollection = ItemCollection(
                items=items
            )
            json_dump(
                obj=collection.to_dict(),
                pretty_print=True
            )

        case _:
            logger.error(f"It's not you, it's us: output format {format} not supported")

for command in [dump_stations, query_stations, search]:
    command.callback = _track(command.callback)
