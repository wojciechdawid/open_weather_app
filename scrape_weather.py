import requests
import json

from config.api_config import API_KEYS

API_key = API_KEYS[0]

response_api = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat=44.34&lon=10.99&appid={API_key}')
data = response_api.text

parse_json = json.loads(data)



print(parse_json)
