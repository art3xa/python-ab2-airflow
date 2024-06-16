from typing import Any
from airflow.models.baseoperator import BaseOperator
from .hook import OpenWeatherMapHook


class OpenWeatherMapOperator(BaseOperator):
    def __init__(self, city: str, conn_id: str = 'openweathermap_conn_id', **kwargs) -> None:
        super().__init__(**kwargs)
        self.city = city
        self.conn_id = conn_id

    def execute(self, context: Any):
        hook = OpenWeatherMapHook(self.conn_id)
        weather_data = hook.get_weather(self.city)
        context['ti'].xcom_push(key='weather_data', value=weather_data)
        return weather_data
