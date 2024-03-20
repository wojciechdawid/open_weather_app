import datetime

import geopandas as gpd
import time
import progressbar
import psycopg2
from sqlalchemy.sql import text

from config.api_config import DB_CONFIG
from operations import DbConnector

if __name__ == "__main__":
    table_name = DB_CONFIG["tables"]["maz_10k"]
    query = f"Select * from {table_name}"
    df = gpd.GeoDataFrame.from_postgis(query, DbConnector.connect_db())
    bar = progressbar.ProgressBar(max_value=len(df), widgets=DbConnector.widgets).start()

    df["lat"] = df.geom.centroid.y
    df["long"] = df.geom.centroid.x
    last_update = datetime.datetime.now()

    db_conn = DbConnector.connect_db()
    with db_conn.connect() as con:
        con.execute(text(f"TRUNCATE {DB_CONFIG['tables']['current_maz_10k']}"))
        for i in range(len(df)):
            one_row = df.iloc[i]
            id_row = one_row.id
            res_api = DbConnector.get_api_values(i=i, lat=one_row.lat, long=one_row.long)
            time.sleep(0.1)
            sql_query = DbConnector.insert_values(id_row, last_update, DB_CONFIG["tables"]["current_maz_10k"], res_api)
            try:
                con.execute(text(sql_query))
            except psycopg2.errors.SyntaxError:
                raise SyntaxError
            bar.update(i)
        con.execute(text("COMMIT;"))
        update_rows, del_rows = DbConnector.update_history(DB_CONFIG['tables']['current_maz_10k'], DB_CONFIG['tables']['history_maz_10k'])
        con.execute(text(update_rows))
        con.execute(text(del_rows))
        con.execute(text("COMMIT;"))

    # df = DbConnector.create_gdf_result()
    # print(df)




