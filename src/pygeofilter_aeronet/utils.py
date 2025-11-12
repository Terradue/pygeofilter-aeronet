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

from geopandas import GeoDataFrame
from pandas import DataFrame
from pathlib import Path
from pygeofilter.parsers.cql2_json import parse as json_parse
from pygeofilter_duckdb import to_sql_where
from pygeofilter.util import IdempotentDict
from pystac.item import Item
from stac_geoparquet.arrow import stac_table_to_items
from typing import (
    List,
    Tuple
)

import duckdb
import geopandas as gpd

duckdb.install_extension("spatial")
duckdb.load_extension("spatial")

def to_geoparquet(data: DataFrame, file_path: Path) -> None:
    # convert DataFrame to GeoParquet and save to file_path
    # the lat/lon columns are Site_Latitude(Degrees) and Site_Longitude(Degrees)
    gdf = GeoDataFrame(
        data,
        geometry=gpd.points_from_xy(
            data["Site_Longitude(Degrees)"], data["Site_Latitude(Degrees)"]
        ),
    )
    gdf.set_crs("EPSG:4326", inplace=True)
    gdf.to_parquet(file_path, engine="pyarrow", compression="gzip")

def query_from_parquet(
    file_path: Path,
    cql2_filter: str | dict
) -> Tuple[str, List[Item]]:
    sql_where = to_sql_where(
        root=json_parse(cql2_filter), # type: ignore
        field_mapping=IdempotentDict() # type: ignore
    )
    sql_query = f"SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) as geometry FROM '{file_path.absolute()}' WHERE {sql_where}"
    results_set = duckdb.query(sql_query)
    results_table = results_set.fetch_arrow_table()

    items: List[Item] = []
    
    for item in stac_table_to_items(results_table):
        #item["assets"] = json.loads(item["assets"])
        items.append(Item.from_dict(item))

    return (sql_query, items)
