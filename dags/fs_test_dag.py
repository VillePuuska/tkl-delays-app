import datetime
import os

from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.operators.python import PythonOperator

def save_file(filename, contents):
    path = FSHook().get_path()
    with open(os.path.join(path, filename), 'w') as f:
        f.write(contents)

with DAG(
    dag_id="fs_test",
    description="Testing connection to local filesystem",
    start_date=datetime.datetime(2023, 12, 2),
    schedule=None,
) as dag:
    save_file_task = PythonOperator(
        task_id="save_file_task",
        python_callable=save_file,
        op_kwargs={'filename':'test_{{ ts_nodash }}.txt',
                   'contents':'testing saving a file\n' +
                   '{{ ts }}\n' +
                   '{{ run_id }}\n' +
                   '{{ ti }}'},
    )