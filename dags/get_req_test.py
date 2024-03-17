import datetime
import os

from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator

def get_and_save(**kwargs):
    response = HttpHook(method='GET', http_conn_id='journeys_activity').run()
    filepath = FSHook(fs_conn_id='fs_app').get_path()
    with open(os.path.join(filepath, f'{str(kwargs["ts_nodash"])}-api-call.txt'), 'w') as f:
        f.write(response.text)

with DAG(
    dag_id="test_get_and_save",
    description="Testing HTTP GET from JourneysAPI",
    start_date=datetime.datetime(2023, 12, 2),
    schedule=None,
) as dag:
    save_file_task = PythonOperator(
        task_id="get_and_save_task",
        python_callable=get_and_save,
        provide_context=True,
    )
