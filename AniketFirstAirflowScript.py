from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=2)
}

def Print_function_python(**kwargs):
    return 'some data'
    # print("Hi Aniket Here! This is my first Airflow Program using Python")
    # print(kwargs)


with DAG(dag_id = 'AniketFirstAirflowScript', default_args = default_args, schedule_interval = "@daily", start_date=days_ago(15),
 catchup=True, max_active_runs=2) as dag:

    dummy_task = DummyOperator(
        task_id='task_1'
    )

    python_task = PythonOperator(
        task_id = 'PrintFn_Python',
        python_callable = Print_function_python
    )

    dummy_task >> python_task