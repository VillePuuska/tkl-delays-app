from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.hooks.filesystem import FSHook
from datetime import datetime
import os

def make_and_save_map(filename):
    from folium import Map, Figure
    TILES = "cartodbdark_matter"
    WIDTH = 750
    HEIGHT = 700
    CENTER = [61.49398541579429, 23.76282953958757]
    ZOOM = 12
    f = Figure(width=WIDTH, height=HEIGHT)
    m = Map(location=CENTER, tiles=TILES, zoom_start=ZOOM).add_to(f)
    m.save(filename)

with DAG(
    "folium",
    default_args={},
    description="Make and save a map of Tre",
    start_date=datetime(2023, 12, 16),
    schedule=None
) as dag:
    t1 = PythonVirtualenvOperator(
        task_id="make_map",
        requirements="folium==0.15.0",
        python_callable=make_and_save_map,
        op_kwargs={'filename':os.path.join(FSHook().get_path(), 'test_map.html')},
    )