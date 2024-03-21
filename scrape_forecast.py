import geopandas as gpd
import time
import warnings
from progressbar import ProgressBar
from psycopg2.errors import SyntaxError
from sqlalchemy.sql import text

from config.api_config import DB_CONFIG
from operations import DbConnector

warnings.filterwarnings("ignore")

if __name__ == "__main__":
    table_name = DB_CONFIG["tables"]["maz_10k"]
    query = f"Select * from {table_name}"
    df = gpd.GeoDataFrame.from_postgis(query, DbConnector.connect_db())
    bar = ProgressBar(max_value=len(df), widgets=DbConnector.widgets).start()

    df["lat"] = df.geom.centroid.y
    df["long"] = df.geom.centroid.x

    db_conn = DbConnector.connect_db()
    with db_conn.connect() as con:
        con.execute(text(f"TRUNCATE {DB_CONFIG['tables']['forecast_maz_10k']}"))
        for i in range(len(df)):
            one_row = df.iloc[i]
            id_row = one_row.id
            res_api = DbConnector.get_api_values(i=i, current=False, lat=one_row.lat, long=one_row.long)
            time.sleep(0.1)
            sql_query = DbConnector.insert_values_forecast(id_row, DB_CONFIG["tables"]["forecast_maz_10k"], res_api)
            for query in sql_query:
                try:
                    con.execute(text(query))
                except SyntaxError:
                    raise SyntaxError
            bar.update(i)
        con.execute(text("COMMIT;"))

    # df = DbConnector.create_gdf_result()
    # print(df)




