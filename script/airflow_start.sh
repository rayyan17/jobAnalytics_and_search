#!/bin/bash
# Set Airflow Directories
export AIRFLOW_HOME="$(pwd)/data_pipeline/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/data_pipeline/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$(pwd)/data_pipeline/plugins"

# Start airflow
airflow initdb
airflow scheduler --daemon
airflow webserver --daemon -p 3001

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
  _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
  if [ $_RUNNING -eq 0 ]; then
    sleep 1
  else
    echo "Airflow web server is ready"
    break;
  fi
done
