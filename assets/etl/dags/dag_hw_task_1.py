from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(dag_id='09_hw_task_1', schedule_interval='@daily', default_args=default_args) as dag:
    start = DummyOperator(task_id='start', dag=dag)
    op_1 = DummyOperator(task_id='op-1', dag=dag)
    op_2 = DummyOperator(task_id='op-2', dag=dag)
    some_other_task = DummyOperator(task_id='some-other-task', dag=dag)
    op_3 = DummyOperator(task_id='op-3', dag=dag)
    op_4 = DummyOperator(task_id='op-4', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)

    start >> [op_1, op_2]
    [op_1, op_2] >> some_other_task
    some_other_task >> [op_3, op_4]
    [op_3, op_4] >> end
