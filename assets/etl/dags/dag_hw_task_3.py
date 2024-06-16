from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}


def print_hello():
    print("Hello, World!")


with DAG(dag_id='11_hw_task_3', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    start = DummyOperator(task_id='start')

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_hello,
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "This is a bash task!"',
    )

    end = DummyOperator(task_id='end')

    start >> python_task >> bash_task >> end
