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
