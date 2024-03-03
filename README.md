# tkl-delays-app

Small program for monitoring TKL delays.

Planned outline:
- data collected from Journeys API,
- data stored in a local Postgres db,
- ETL orchestrated with Airflow,
- Golang+HTMX for the minimal web app,
- Folium to plot latest data on a map.

Running in codespaces notes:
- webserver_config.py : set WTF_CSRF_ENABLED = False, otherwise logging to airflow web ui will fail with "The referrer does not match the host."

General notes to get running:
- airflow environment needs pandas, duckdb, and virtualenv installed
- env vars:
```
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/tkl-delays-app/dags
```
- set up the connections:
    - fs_default
    - http_default
    - postgres_default
- boot up postgres (docker compose up)
    - run create_tables.sql commands if running for the first time or want to reset data
- you can test all the connections with the dags other than `etl`

Connections defined with env variables for example setup:
```
export AIRFLOW_CONN_FS_APP='{
    "conn_type": "fs",
    "description": "",
    "login": "",
    "password": null,
    "host": "",
    "port": null,
    "schema": "",
    "extra": "{\"path\": \"/workspaces/tkl-delays-app/data\"}"
}'
export AIRFLOW_CONN_JOURNEYS_ACTIVITY='{
    "conn_type": "http",
    "description": "",
    "login": "",
    "password": null,
    "host": "http://data.itsfactory.fi/journeys/api/1/vehicle-activity",
    "port": null,
    "schema": "",
    "extra": "{\"User-Agent\": \"github.com/VillePuuska/tkl-delays-app\"}"
}'
export AIRFLOW_CONN_PG_APP='{
    "conn_type": "postgres",
    "description": "",
    "login": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": 5432,
    "schema": "airflow",
    "extra": "{}"
}'
```
**Note:** If you define connections with env variables, they will not show up in the Airflow UI.
