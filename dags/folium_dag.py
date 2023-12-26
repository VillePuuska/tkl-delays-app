from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from datetime import datetime
import pandas as pd
import numpy as np
import os

def make_df(out_filename):
    df = pd.DataFrame({
        'Line':[1, 2, 3, 4],
        'Lon':[61.49398541579429, 61.49398541579429, 61.49398541579429, 61.49398541579429],
        'Lat':[23.76282953958757, 23.76382953958757, 23.76482953958757, 23.76582953958757],
        'Delay':[0, 10, 20, 30],
        })
    df.to_csv(out_filename, index=False)

with DAG(
    "folium",
    default_args={},
    description="Make and save a map of Tre",
    start_date=datetime(2023, 12, 16),
    schedule=None
) as dag:
    df_task = PythonOperator(
        task_id='make_df',
        python_callable=make_df,
        op_kwargs={'out_filename':os.path.join(FSHook().get_path(), '{{ ts_nodash }}-markers.csv')}
    )

    @task.virtualenv(
        task_id="make_map",
        requirements=["folium==0.15.0", "pandas==2.1.2"],
    )
    def make_and_save_map(in_filename, out_filename):
        from folium import Map, Figure, Circle
        import pandas as pd

        print(pd.read_csv(in_filename))

        TILES = "cartodbdark_matter"
        WIDTH = 750
        HEIGHT = 700
        CENTER = [61.49398541579429, 23.76282953958757]
        ZOOM = 12
        f = Figure(width=WIDTH, height=HEIGHT)
        m = Map(location=CENTER, tiles=TILES, zoom_start=ZOOM).add_to(f)
        Circle(CENTER, color='blue', popup=f"Test marker\n{CENTER}",
            fill=True, weight=0, fillOpacity=0.7, radius=100).add_to(m)
        Circle([CENTER[0]+0.01, CENTER[1]+0.01], color='blue', popup=f"Test marker #2\n{[CENTER[0]+0.01, CENTER[1]+0.01]}",
            fill=True, weight=0, fillOpacity=0.7, radius=100).add_to(m)
        m.save(out_filename)
    folium_task = make_and_save_map(
        in_filename=os.path.join(FSHook().get_path(), '{{ ts_nodash }}-markers.csv'),
        out_filename=os.path.join(FSHook().get_path(), 'test_map.html')
    )

    df_task >> folium_task