import requests
import json
from sqlalchemy.sql import text
import pandas as pd

from config.api_config import API_KEYS, DB_CONFIG
from operations import DbConnector
"""
API_key = API_KEYS[0]

response_api = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat=44.34&lon=10.99&appid={API_key}')
data = response_api.text

parse_json = json.loads(data)

print(parse_json)
"""
table_name = DB_CONFIG["tables"]["10k"]
query = f"Select count(*) from {table_name}"

results = DbConnector.query_db(query)

df = pd.DataFrame(results)
print(df)


