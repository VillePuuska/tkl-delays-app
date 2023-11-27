from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "trivial",
    default_args={},
    description="Most trivial possible DAG",
    start_date=datetime(2023, 11, 27),
    schedule=None
) as dag:
    t1 = BashOperator(
        task_id="echo",
        bash_command="echo Hello There",
    )