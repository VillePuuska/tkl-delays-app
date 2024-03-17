import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def select_to_df():
    hook = PostgresHook(postgres_conn_id='pg_app')    
    df = hook.get_pandas_df(sql="SELECT * FROM testing_table;")
    print(df)
    return df.to_string()

with DAG(
    dag_id="test_postgres",
    description="Testing connection to local postgres",
    start_date=datetime.datetime(2023, 11, 28),
    schedule=None,
) as dag:
    insert_task = PostgresOperator(
        postgres_conn_id='pg_app',
        task_id="insert_task",
        sql="INSERT INTO testing_table (col1) VALUES ('{{ ts }}');",
    )

    select_df_task = PythonOperator(
        task_id='select_star_to_df',
        python_callable=select_to_df,
    )

    insert_task >> select_df_task
