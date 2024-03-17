#/bin/sh

mkdir -p data

python3 -m venv venv
source venv/bin/activate
pip install pandas duckdb folium

docker compose pull

AIRFLOW_VERSION=2.8.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/tkl-delays-app/dags

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

echo "To complete setup, run airflow standalone once, then edit ~/airflow/webserver_config.py: set WTF_CSRF_ENABLED = False"
