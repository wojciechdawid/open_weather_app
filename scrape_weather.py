import geopandas as gpd

from config.api_config import API_KEYS, DB_CONFIG
from operations import DbConnector


if __name__ == "__main__":
    # DbConnector.query_db_file('create_10km_table')
    table_name = DB_CONFIG["tables"]["10k"]
    query = f"Select * from {table_name}"
    df = gpd.GeoDataFrame.from_postgis(query, DbConnector.connect_db())

    for i in range(len(df)):
        one_row = df.iloc[i]
        id_row = one_row.id
        lat = one_row.geom.centroid.y
        long = one_row.geom.centroid.x
        res_api = DbConnector.get_api_values(lat=lat, long=long)
        DbConnector.insert_values(id_row, DB_CONFIG["tables"]["current_10k"], res_api)

# TODO: expedite the process: maybe calculate lat and long in postgis



