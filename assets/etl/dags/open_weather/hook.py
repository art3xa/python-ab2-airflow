import requests
from airflow.hooks.base import BaseHook


class OpenWeatherMapHook(BaseHook):
    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id

    def get_weather(self, city: str):
        conn = self.get_connection(self.conn_id)
        api_key = conn.password
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
