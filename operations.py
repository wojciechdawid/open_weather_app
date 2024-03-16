import psycopg2.errors
from sqlalchemy import Engine, create_engine, CursorResult
from sqlalchemy.sql import text
import random
import requests
import json
import datetime

from config.api_config import DB_CONFIG, API_KEYS


class DbConnector:

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
            with open(f"sql_scripts/{file_name}.sql") as file:
                query = text(file.read())
                conn.execute(query)

    @staticmethod
    def get_api_values(lat: float, long: float) -> dict:
        api_nr = random.randint(0, 9)
        api_key = API_KEYS[api_nr]
        response_api = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat='
                                    f'{lat}&lon={long}&units=metric&appid={api_key}')
        data = response_api.text
        return json.loads(data)

    @staticmethod
    def insert_values(geom_id: int, table_name: str, json_api: dict) -> None:
        id_geom = geom_id
        temperature = json_api["main"]["feels_like"]
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
        last_update = datetime.datetime.now()
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
                    f"weather_code, weather_desc, precip_type, precip_value) VALUES ({id_geom}, {temperature}, {temperature_max}, " \
                    f"{temperature_min}, {humidity}, {pressure}, {wind_speed}, {wind_direction}, {clouds}, {visibility}, " \
                    f"'{sunrise}', '{sunset}', '{last_update}', '{name}', '{country}', '{weather_type}', {weather_code}, '{weather_desc}', " \
                    f"'{precip_type}', {precip_value}); " \
                    f"COMMIT;"

        try:
            DbConnector.query_db(sql_query)
        except psycopg2.errors.SyntaxError:
            raise SyntaxError
