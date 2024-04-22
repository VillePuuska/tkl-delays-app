# tkl-delays-app

Small program for monitoring TKL delays and collecting the data.

Planned outline:
- data collected from Journeys API,
- data stored in a (local) Postgres db,
- ETL orchestrated with Airflow,
- Folium to plot latest data on a map,
- Streamlit for a webapp.

---

First time setup to get Airflow dev setup running:
- `chmod +x setup.sh`
- `. ./setup.sh` Note the first dot as the script needs to be run in the _current_ shell as it sets environment variables for Airflow. Alternatively you can run `source ./setup.sh`
- If NOT using codespaces, run `airflow standalone` and you're good to go.
- If using codespaces:
    - Run `airflow standalone` -> wait until Airflow is ready -> shut it down with `Ctrl-C`
    - Edit `~/airflow/webserver_config.py`: set `WTF_CSRF_ENABLED = False` Without this edit logging in to airflow web ui will fail with "The referrer does not match the host."
    - Run `airflow standalone` again and now your dev setup is running.

**NOTE**: When booting up Airflow in the future, you need to set the env variables again every time before running `airflow standalone`

---

Running the Streamlit app:
- You need a Python environment with `streamlit`, `duckdb`, and `pandas`
- Run `python -m streamlit run app/main.py` from the root of this repo, i.e. the directory `tkl-delays-app`.

**NOTE**: If you don't run from the root directory of this repo, the relative path to the file `data/map.html` will be wrong.

---

Tests and running them:
- `python -m pytest` from the directory `tkl-delays-app` to run pytests.
- Run DAGs starting with `test_` in Airflow to test the needed connections.

---

General notes to get running:
- create the directory `data`
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
