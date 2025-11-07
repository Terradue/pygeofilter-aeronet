from pandas import DataFrame
from geopandas import GeoDataFrame
import geopandas as gpd
def to_geoparquet(data: DataFrame, file_path: str) -> None:
    # convert DataFrame to GeoParquet and save to file_path
    # the lat/lon columns are Site_Latitude(Degrees) and Site_Longitude(Degrees)
    gdf = GeoDataFrame(data, geometry=gpd.points_from_xy(data["Site_Longitude(Degrees)"], data["Site_Latitude(Degrees)"]))
    gdf.set_crs("EPSG:4326", inplace=True)
    gdf.to_parquet(file_path, engine="pyarrow", compression="gzip")
