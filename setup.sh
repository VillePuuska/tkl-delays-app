#!/bin/sh

mkdir -p data

# Pull and spin up database.
docker compose up -d

# Create python venv for airflow installation. Install dependencies and then airflow.
python3 -m venv venv
source venv/bin/activate
pip install pandas duckdb folium pytest

AIRFLOW_VERSION=2.8.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Export needed env variables.
# REMEMBER TO RUN THIS SCRIPT IN THE SAME SHELL YOU WILL START AIRFLOW FROM, NOT IN A SUBSHELL
# Done e.g. by calling '. ./setup.sh'
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

# Create tables. Separated here so we do not need to wait for the container to spin up before running this command.
cat ./create_tables.sql | docker exec -i tkl-delays-app-postgres-1 psql -U airflow -d airflow

echo -e "\n\n\n\nTo complete setup, run airflow standalone once, then edit ~/airflow/webserver_config.py: set WTF_CSRF_ENABLED = False"
echo "Also, note that the Postgres container is already running."
