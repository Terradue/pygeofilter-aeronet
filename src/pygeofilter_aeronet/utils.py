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

from pandas import DataFrame
from geopandas import GeoDataFrame
import geopandas as gpd
from pathlib import Path


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
