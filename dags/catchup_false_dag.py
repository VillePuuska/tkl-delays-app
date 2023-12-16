from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "catchup_False",
    default_args={},
    description="Testing catchup False",
    start_date=datetime(2023, 12, 16, 8, 20),
    schedule='* * * * *',
    catchup=False,
) as dag:
    t1 = BashOperator(
        task_id="always_run",
        bash_command="echo First task",
    )

    t2 = BashOperator(
        task_id="always_run_2",
        bash_command="echo Last task",
    )

    t1 >> t2