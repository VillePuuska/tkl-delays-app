# tkl-delays-app

Small program for monitoring TKL delays and collecting the data.

Planned outline:
- data collected from Journeys API,
- data stored in a (local) Postgres db,
- ETL orchestrated with Airflow,
- Folium to plot latest data on a map,
- possibly Golang+HTMX for a minimal web app.

General notes to get running:
- airflow environment needs pandas, duckdb, and folium installed
- env vars:
```
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/tkl-delays-app/dags
```
- set up the connections:
    - fs_app (filesystem connection, location for all saved files)
    - journeys_activity (http connection, JourneysAPI Vehicle Activity endpoint)
    - pg_app (Postgres connection, database details)
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
    "extra": "{\"User-Agent\": \"Airflow\"}"
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

Running in codespaces notes:
- webserver_config.py : set WTF_CSRF_ENABLED = False, otherwise logging to airflow web ui will fail with "The referrer does not match the host."
