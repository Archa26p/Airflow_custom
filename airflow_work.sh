#!/bin/sh
cd /cloudapps/airflow
. airflow_venv/bin/activate
/cloudapps/airflow/airflow_venv/bin/airflow worker --pid /cloudapps/airflow/airflow_venv/config/run/airflow/worker.pid

