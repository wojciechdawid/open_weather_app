import time

import pandas as pd
import progressbar
from sqlalchemy import Engine, create_engine, CursorResult
from sqlalchemy.sql import text
import random
import requests
import json
import datetime
import geopandas as gpd


from config.api_config import DB_CONFIG, API_KEYS


class DbConnector:

    widgets = [' [',
               progressbar.Timer(format='elapsed time: %(elapsed)s'),
               '] ',
               progressbar.Bar('*'), ' (',
               progressbar.ETA(), ') ',
               ]

    api_nr = len(API_KEYS)

    @staticmethod
    def connect_db(db_name: str = DB_CONFIG['db_name']['weather'], echo: bool = False) -> Engine:
        return create_engine(f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{db_name}", echo=echo)

    @staticmethod
    def query_db(query: str) -> CursorResult:
        db_conn = DbConnector.connect_db()
        with db_conn.connect() as conn:
            statement = text(query)
            return conn.execute(statement)

    @staticmethod
    def query_db_file(file_name: str) -> CursorResult:
        db_conn = DbConnector.connect_db()
        with db_conn.connect() as conn:
            with open(file_name) as file:
                query = text(file.read())
                conn.execute(query)

    @staticmethod
    def get_api_values(i: int, current: bool, lat: float, long: float) -> dict:
        api_key = API_KEYS[i % DbConnector.api_nr]
        if current:
            weather_type = "weather"
        else:
            weather_type = "forecast"
        response_api = requests.get(f'https://api.openweathermap.org/data/2.5/{weather_type}?lat='
                                    f'{lat}&lon={long}&units=metric&appid={api_key}')
        data = response_api.text
        return json.loads(data)

    @staticmethod
    def insert_values_current(geom_id: int, last_update, table_name: str, json_api: dict) -> str:
        temperature = json_api["main"]["temp"]
        temperature_max = json_api["main"]["temp_max"]
        temperature_min = json_api["main"]["temp_min"]
        humidity = json_api["main"]["humidity"]
        pressure = json_api["main"]["pressure"]
        wind_speed = json_api["wind"]["speed"]
        wind_direction = json_api["wind"]["deg"]
        clouds = json_api["clouds"]["all"]
        visibility = json_api["visibility"]
        sunrise = datetime.datetime.fromtimestamp(json_api["sys"]["sunrise"])
        sunset = datetime.datetime.fromtimestamp(json_api["sys"]["sunset"])
        last_update = last_update
        name = json_api["name"]
        country = json_api["sys"]["country"]
        weather_type = json_api["weather"][0]["main"]
        weather_code = json_api["weather"][0]["id"]
        weather_desc = json_api["weather"][0]["description"]
        if "rain" in json_api:
            precip_type = "rain"
            precip_value = json_api["rain"]["1h"]
        elif "snow" in json_api:
            precip_type = "snow"
            precip_value = json_api["snow"]["1h"]
        else:
            precip_type = "no"
            precip_value = 0

        sql_query = f"INSERT INTO {table_name} (id_geom, temperature, temperature_max, temperature_min, humidity, pressure, " \
                    f"wind_speed, wind_direction, clouds, visibility, sunrise, sunset, last_update, name, country, weather_type, " \
                    f"weather_code, weather_desc, precip_type, precip_value) VALUES ({geom_id}, {temperature}, {temperature_max}, " \
                    f"{temperature_min}, {humidity}, {pressure}, {wind_speed}, {wind_direction}, {clouds}, {visibility}, " \
                    f"'{sunrise}', '{sunset}', '{last_update}', '{name}', '{country}', '{weather_type}', {weather_code}, '{weather_desc}', " \
                    f"'{precip_type}', {precip_value});\n"

        output = {geom_id, temperature, temperature_max, temperature_min, humidity, pressure, wind_speed, wind_direction, clouds,
                  visibility, sunrise, sunset, last_update, name, country, weather_type, weather_code, weather_desc, precip_type, precip_value}

        return sql_query

    @staticmethod
    def insert_values_forecast(geom_id: int, table_name: str, json_api: dict) -> list[str]:
        weather_list = json_api["list"]
        name = json_api["city"]["name"]
        country = json_api["city"]["country"]
        sunrise = datetime.datetime.fromtimestamp(json_api["city"]["sunrise"])
        sunset = datetime.datetime.fromtimestamp(json_api["city"]["sunset"])

        forecast_list = []

        for i in weather_list:
            temperature = i["main"]["temp"]
            temperature_max = i["main"]["temp_max"]
            temperature_min = i["main"]["temp_min"]
            humidity = i["main"]["humidity"]
            pressure = i["main"]["pressure"]
            wind_speed = i["wind"]["speed"]
            wind_direction = i["wind"]["deg"]
            clouds = i["clouds"]["all"]
            if "visibility" not in i:
                visibility = 0
            else:
                visibility = i["visibility"]
            last_update = datetime.datetime.fromtimestamp(i["dt"])
            weather_type = i["weather"][0]["main"]
            weather_code = i["weather"][0]["id"]
            weather_desc = i["weather"][0]["description"]
            probability_precipitation = i["pop"]

            if "rain" in i:
                precip_type = "rain"
                precip_value = i["rain"]["3h"]
            elif "snow" in i:
                precip_type = "snow"
                precip_value = i["snow"]["3h"]
            else:
                precip_type = "no"
                precip_value = 0

            sql_query = f"INSERT INTO {table_name} (id_geom, temperature, temperature_max, temperature_min, humidity, pressure, " \
                    f"wind_speed, wind_direction, clouds, visibility, sunrise, sunset, last_update, name, country, weather_type, " \
                    f"weather_code, weather_desc, precip_type, precip_value, precip_chance) VALUES ({geom_id}, {temperature}, {temperature_max}, " \
                    f"{temperature_min}, {humidity}, {pressure}, {wind_speed}, {wind_direction}, {clouds}, {visibility}, " \
                    f"'{sunrise}', '{sunset}', '{last_update}', '{name}', '{country}', '{weather_type}', {weather_code}, '{weather_desc}', " \
                    f"'{precip_type}', {precip_value}, {probability_precipitation});\n"

            output = {geom_id, temperature, temperature_max, temperature_min, humidity, pressure, wind_speed,
                  wind_direction, clouds,
                  visibility, sunrise, sunset, last_update, name, country, weather_type, weather_code, weather_desc,
                  precip_type, precip_value}

            forecast_list.append(sql_query)

        return forecast_list

    @staticmethod
    # TODO!!
    def create_gdf_result():
        table_name = DB_CONFIG["tables"]["maz_10k"]
        query = f"Select * from {table_name}"
        df = gpd.GeoDataFrame.from_postgis(query, DbConnector.connect_db())
        bar = progressbar.ProgressBar(max_value=len(df), widgets=DbConnector.widgets).start()

        df["lat"] = df.geom.centroid.y
        df["long"] = df.geom.centroid.x
        final_dict = dict()

        for i in range(10):
            one_row = df.iloc[i]
            id_row = one_row.id
            res_api = DbConnector.get_api_values(lat=one_row.lat, long=one_row.long)
            time.sleep(0.1)
            output = DbConnector.insert_values(id_row, DB_CONFIG["tables"]["current_10k"], res_api)
            print(output)
            final_dict.update({i+1: output})

        return pd.DataFrame.from_dict(final_dict, orient="index")

    @staticmethod
    def update_history(table_current: str, table_history: str) -> tuple:
        update = f"INSERT INTO {table_history} SELECT * FROM {table_current};"
        delete = f"DELETE FROM {table_history} WHERE last_update < now() - interval '7 days';"
        return update, delete


