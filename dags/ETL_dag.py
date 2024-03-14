import datetime
import requests
import pandas as pd
import os
import json

from utils.flatten import body_to_df, body_to_df_buses

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.filesystem import FSHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def get_and_save(**kwargs):
    response = HttpHook(method='GET', http_conn_id='journeys_activity').run()
    filepath = FSHook(fs_conn_id='fs_app').get_path()
    with open(os.path.join(filepath, f'{str(kwargs["ts_nodash"])}-api-call.txt'), 'w') as f:
        f.write(response.text)

def flatten_and_upsert_function(**kwargs):
    filepath = FSHook(fs_conn_id='fs_app').get_path()
    with open(os.path.join(filepath, f'{str(kwargs["ts_nodash"])}-api-call.txt'), 'r') as f:
        contents = json.loads(f.read())
    body = contents['body']
    df = body_to_df(body, 2)
    hook = PostgresHook(postgres_conn_id='pg_app')
    hook.insert_rows('stops',
                     list(df.itertuples(index=False)),
                     ['Recorded_At',
                      'Line',
                      'Direction',
                      'Date',
                      'Lon',
                      'Lat',
                      'Delay',
                      'Departure_Time',
                      'Stop',
                      'Stop_Order'],
                      replace=True,
                      replace_index=['Line',
                                     'Direction',
                                     'Date',
                                     'Departure_Time',
                                     'Stop'],
                     )

def flatten_insert_save_buses_function(**kwargs):
    filepath = FSHook(fs_conn_id='fs_app').get_path()
    with open(os.path.join(filepath, f'{str(kwargs["ts_nodash"])}-api-call.txt'), 'r') as f:
        contents = json.loads(f.read())
    body = contents['body']
    df = body_to_df_buses(body, 2)
    df.to_csv(os.path.join(filepath, 'latest_bus_data.csv'))
    hook = PostgresHook(postgres_conn_id='pg_app')
    hook.insert_rows('buses',
                     list(df.itertuples(index=False)),
                     ['Recorded_At',
                      'Line',
                      'Direction',
                      'Date',
                      'Lon',
                      'Lat',
                      'Delay',
                      'Departure_Time'],
                      replace=True,
                      replace_index=['Recorded_At',
                                     'Line',
                                     'Direction',
                                     'Date',
                                     'Departure_Time'],
                     )

@task.virtualenv(
    task_id="make_folium_map",
    requirements=["folium==0.15.0", "pandas==2.1.2"],
)
def make_and_save_map(in_filename, out_filename):
    from folium import Map, Figure, Circle
    import pandas as pd

    df = pd.read_csv(in_filename)

    TILES = "cartodbdark_matter"
    WIDTH = 750
    HEIGHT = 700
    CENTER = [61.49398541579429, 23.76282953958757]
    ZOOM = 12
    f = Figure(width=WIDTH, height=HEIGHT)
    m = Map(location=CENTER, tiles=TILES, zoom_start=ZOOM).add_to(f)

    for _, row in df.iterrows():
        coords = [float(row.Lat), float(row.Lon)]
        Circle(coords, color='blue', popup=f"Line {row.Line}\nDelay {row.Delay}",
                fill=True, weight=0, fillOpacity=0.7, radius=100).add_to(m)

    m.save(out_filename)

with DAG(
    dag_id="etl",
    description="Entire ETL process",
    start_date=datetime.datetime(2023, 12, 16),
    schedule='* * * * *',
    catchup=False,
) as dag:
    get_data_task = PythonOperator(
        task_id='get_and_save_data_task',
        python_callable=get_and_save,
        provide_context=True,
    )

    flatten_and_upsert = PythonOperator(
        task_id='flatten_and_upsert_stop_data_task',
        python_callable=flatten_and_upsert_function,
        provide_context=True
    )

    flatten_and_insert_latest = PythonOperator(
        task_id='flatten_insert_and_save_bus_data_task',
        python_callable=flatten_insert_save_buses_function,
        provide_context=True
    )

    # archive_json = BashOperator(
    #     task_id='archive_saved_json',
    #     bash_command='mv $filepath/$filename $filepath/archive/$filename',
    #     env={'filepath':FSHook(fs_conn_id='fs_app').get_path(),
    #          'filename':'{{ ts_nodash }}-api-call.txt'}
    # )
    
    folium_task = make_and_save_map(
        in_filename=os.path.join(FSHook(fs_conn_id='fs_app').get_path(), 'latest_bus_data.csv'),
        out_filename=os.path.join(FSHook(fs_conn_id='fs_app').get_path(), 'map.html')
    )

    get_data_task >> [flatten_and_upsert, flatten_and_insert_latest]
    flatten_and_insert_latest >> folium_task
