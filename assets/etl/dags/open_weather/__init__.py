from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from .operator import OpenWeatherMapOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        dag_id='13_hw_task_5',
        default_args=default_args,
        schedule_interval='@daily',
        max_active_runs=3,
) as dag:
    create_table = PostgresOperator(
        task_id='create_table_task',
        sql='''
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(50) NOT NULL,
            temperature FLOAT NOT NULL,
            humidity INT NOT NULL,
            date DATE NOT NULL,
            UNIQUE (city, date)
        );
        ''',
        postgres_conn_id='postgres_weather_db',
    )

    get_weather = OpenWeatherMapOperator(
        task_id='get_weather',
        city='London',
        conn_id='openweathermap_conn_id',
    )

    insert_weather = PostgresOperator(
        task_id='insert_weather',
        postgres_conn_id='postgres_weather_db',
        sql='''
        INSERT INTO weather_data (city, temperature, humidity, date)
        VALUES ('{{ task_instance.xcom_pull(task_ids="get_weather")["name"] }}',
                {{ task_instance.xcom_pull(task_ids="get_weather")["main"]["temp"] }},
                {{ task_instance.xcom_pull(task_ids="get_weather")["main"]["humidity"] }},
                '{{ execution_date.strftime("%Y-%m-%d") }}')
        ON CONFLICT (city, date) DO UPDATE
        SET temperature = excluded.temperature, humidity = excluded.humidity;
        ''',
    )

    create_table >> get_weather >> insert_weather
