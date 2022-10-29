## Import Libraries for Airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

#{"queue": "default", "master": "local[*]", "spark-home": "/home/aniketa/apache_spark/spark-3.1.2-bin-hadoop3.2/", "spark-binary": "spark-submit", "namespace": "default"}

default_args = {
    'start_date': days_ago(1),
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}


def PrintFn_Python(**kwargs):
    print("Hello")


spark_config = {
    'conf': {
        "spark.yarn.maxAppAttempts":"1",
        "spark.yarn.executor.memoryOverhead":"256"
    },
    'conn_id' : 'spark_local',
    #'application' : '/home/aniketa/apache_spark/spark-3.1.2-bin-hadoop3.2/examples/src/main/python/countMnM.py /home/aniketa/apache_spark/spark-3.1.2-bin-hadoop3.2/data/mnm_dataset.csv',
    'application' : '/home/aniketa/apache_spark/spark-3.1.2-bin-hadoop3.2/examples/src/main/python/countMnM.py',
    'driver_memory' : '1g',
    'executor_cores' : 1,
    'num_executors' : 1,
    'executor_memory' : '1g'
}


dag = DAG(dag_id = 'DemoPySparkJob', default_args = default_args, schedule_interval = "*/4 * * * *", catchup = False, max_active_runs=2)

python_task = PythonOperator(
    task_id = 'pyspark_airflow_fn',
    python_callable = PrintFn_Python,
    dag=dag
    )


spark_task = SparkSubmitOperator(
    task_id = 'pyspark_task',    
    dag = dag,
    **spark_config
    )

python_task.set_downstream(spark_task)


if __name__ == "__main__":
    dag.cli()