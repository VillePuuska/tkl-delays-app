from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime

with DAG(
    "latest",
    default_args={},
    description="Testing LatestOnlyOperator",
    start_date=datetime(2023, 12, 15),
    schedule='0 * * * *'
) as dag:
    t1 = BashOperator(
        task_id="always_run",
        bash_command="echo First task",
    )

    t2 = LatestOnlyOperator(
        task_id='run_latest',
    )

    t3 = BashOperator(
        task_id="always_run_2",
        bash_command="echo Last task",
    )

    t1 >> t2 >> t3