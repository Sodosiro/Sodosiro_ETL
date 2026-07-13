"""동작 확인용 예제 DAG."""
from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def hello() -> None:
    print("Hello from sodosiro-ETL Airflow!")


with DAG(
    dag_id="example_hello",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # 수동 트리거
    catchup=False,
    tags=["example"],
) as dag:
    say_hello = PythonOperator(task_id="say_hello", python_callable=hello)
    print_date = BashOperator(task_id="print_date", bash_command="date")

    say_hello >> print_date
