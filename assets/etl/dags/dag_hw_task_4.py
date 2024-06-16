from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from custom_operator import CustomOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(dag_id='12_hw_task_4', schedule_interval='@daily', default_args=default_args,
         catchup=False) as dag:
    start = DummyOperator(task_id='start')

    custom_task = CustomOperator(
        task_id='custom_task',
        custom_message='Hello from the custom operator!'
    )

    end = DummyOperator(task_id='end')

    start >> custom_task >> end
