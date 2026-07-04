from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_1_func():
    print("Task 1: Extracting data (pretend)")

def task_2_func():
    print("Task 2: Transforming data (pretend)")

def task_3_func():
    print("Task 3: Loading data (pretend)")

with DAG(
    dag_id="dummy_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice"],
) as dag:

    task_1 = PythonOperator(task_id="extract", python_callable=task_1_func)
    task_2 = PythonOperator(task_id="transform", python_callable=task_2_func)
    task_3 = PythonOperator(task_id="load", python_callable=task_3_func)

    task_1 >> task_2 >> task_3