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
from .aeronet_client.api.default.search import sync as aeronet_client_search
from .aeronet_client.api.default.get_stations import sync as get_stations
from .evaluator import (
    to_aeronet_api,
    SUPPORTED_VALUES
)
from .utils import verbose_client
from datetime import datetime
from enum import (
    Enum,
    auto
)
from geopandas import (
    GeoDataFrame,
    GeoSeries
)
from io import StringIO
from loguru import logger
from pandas import (
    DataFrame,
    to_datetime,
    read_csv
)
from pathlib import Path
from pystac import (
    Asset,
    Item,
    Link
)
from pygeofilter_duckdb import to_sql_where
from pygeofilter.parsers.cql2_json import parse as json_parse
from pygeofilter.util import IdempotentDict
from shapely.geometry import (
    Point,
    MultiPoint,
    mapping
)
from stac_geoparquet.arrow import (
    parse_stac_items_to_arrow,
    stac_table_to_items,
    to_parquet
)
from typing import (
    Any,
    List,
    Mapping,
    Optional,
    Tuple
)

import duckdb
import geopandas
import uuid


AERONET_API_BASE_URL = "https://aeronet.gsfc.nasa.gov"
DEFAULT_STATIONS_PARQUET_URL = "https://github.com/Terradue/pygeofilter-aeronet/raw/refs/heads/stations-update/stations.parquet"


duckdb.install_extension("spatial")
duckdb.load_extension("spatial")


class FilterLang(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name.lower().replace("_", "-")

    CQL2_JSON = auto()
    CQL2_TEXT = auto()

def dump_items(
    items: List[Item],
    output_file: Path
):
    logger.info('Converting the STAC Items pyarrow Table...')
    record_batch_reader = parse_stac_items_to_arrow(items)
    table = record_batch_reader.read_all()
    logger.success('STAC Items converted to pyarrow Table')

    logger.info(f"Saving the GeoParquet data to {output_file.absolute()}...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    to_parquet(table, output_path=output_file)
    logger.success(f"GeoParquet data saved to {output_file.absolute()}")

def get_aeronet_stations(
    url: str = AERONET_API_BASE_URL,
    verbose: bool = False
) -> List[Item]:
    with AeronetClient(base_url=url) as aeronet_client:
        if verbose:
            verbose_client(aeronet_client.get_httpx_client())
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

        return items

def query_stations_from_parquet(
    file_path: str,
    cql2_filter: str | Mapping[str, Any] | None = None
) -> Tuple[str, List[Item]]:
    sql_query = f"SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) as geometry FROM '{file_path}'"

    if cql2_filter:
        sql_where = to_sql_where(
            root=json_parse(cql2_filter), # type: ignore
            field_mapping=IdempotentDict() # type: ignore
        )
        sql_query += f" WHERE {sql_where}"

    results_set = duckdb.query(sql_query)
    results_table = results_set.fetch_arrow_table()

    items: List[Item] = []
    
    for item in stac_table_to_items(results_table):
        items.append(Item.from_dict(item))

    return (sql_query, items)

def _read_aeronet_site_list() -> List[str]:
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

    site_list: List[str] = []
    
    _, items = query_stations_from_parquet(DEFAULT_STATIONS_PARQUET_URL)
    for item in items:
        site_list.append(item.properties['aeronet:site_name'])
    return site_list

SUPPORTED_VALUES['site'] = _read_aeronet_site_list()

def dry_run_aeronet_search(
    cql2_filter: str | Mapping[str, Any],
    url: str = AERONET_API_BASE_URL
):
    filter, _ = to_aeronet_api(cql2_filter)
    logger.info(f"You can browse data on: {url}/cgi-bin/print_web_data_v3?{filter}")

def aeronet_search(
    cql2_filter: str | Mapping[str, Any],
    output_dir: Path,
    url: str = AERONET_API_BASE_URL,
    verbose: bool = False
) -> Item:
    filter, query_parameters = to_aeronet_api(cql2_filter)

    with AeronetClient(base_url=url) as aeronet_client:
        if verbose:
            verbose_client(aeronet_client.get_httpx_client())
        raw_data = aeronet_client_search(client=aeronet_client, **query_parameters)
        data: DataFrame = read_csv(StringIO(raw_data), skiprows=5)

    logger.success(f"Query on {url} successfully obtained data:")

    output_dir.mkdir(parents=True, exist_ok=True)

    id = uuid.uuid4()

    csv_output_file = Path(output_dir, f"{id}.csv")
    data.to_csv(csv_output_file, index=False)
    logger.success(f"Data saved to to CSV file: {csv_output_file.absolute()}")

    parquet_output_file = Path(output_dir, f"{id}.parquet")
    gdf = GeoDataFrame(
        data,
        geometry=geopandas.points_from_xy(
            data["Site_Longitude(Degrees)"], data["Site_Latitude(Degrees)"]
        ),
    )
    gdf.set_crs("EPSG:4326", inplace=True)

    date_col = "Date(dd:mm:yyyy)"
    time_col = "Time(hh:mm:ss)"

    # pick first matching date column
    date_col: Optional[str] = next((c for c in ["Date(dd:mm:yyyy)", "Date_(dd:mm:yyyy)"] if c in gdf.columns), None)
    time_col: Optional[str] = next((c for c in ["Time(hh:mm:ss)", "Time_(hh:mm:ss)"] if c in gdf.columns), None)

    if date_col in data.columns:
        if time_col in data.columns:
            gdf["datetime"] = to_datetime(
                data[date_col] + " " + data[time_col],
                format="%d:%m:%Y %H:%M:%S"
            )
        else:
            gdf["datetime"] = to_datetime(
                data[date_col] + " 00:00:00",
                format="%d:%m:%Y %H:%M:%S"
            )
    
    gdf.to_parquet(parquet_output_file, engine="pyarrow", compression="gzip")
    logger.success(f"Data saved to GeoParquet file: {parquet_output_file.absolute()}")

    dataframe = geopandas.read_parquet(parquet_output_file)
    unique_points: GeoSeries = dataframe.geometry
    unique_points.drop_duplicates()

    multipoint = MultiPoint(points=unique_points) # type: ignore TODO

    current_item: Item = Item(
        id=f"urn:uuid:{id}",
        assets={
            'csv': Asset(
                href=str(csv_output_file),
                media_type='text/csv',
                description='Search result - CVS Format'
            ),
            'geoparquet': Asset(
                href=str(parquet_output_file),
                media_type='application/vnd.apache.parquet',
                description='Search result - GeoParquet Format'
            )
        },
        bbox=list(multipoint.envelope.bounds),
        datetime=datetime.now(),
        geometry=mapping(multipoint.envelope),
        properties={}
    )

    current_item.links = [
        Link(
            rel='related',
            media_type='text/csv',
            title='AERONET Web Service search',
            target=f"{url}/cgi-bin/print_web_data_v3?{filter}"
        )
    ]

    return current_item
