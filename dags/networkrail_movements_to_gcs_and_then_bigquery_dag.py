import csv
import json

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchOperator
from airflow.utils import timezone
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html
from airflow.providers.postgres.hooks.postgres import PostgresHook


# setup project directories
DAG_FOLDER = '/opt/airflow/dags'
DATA_FOLDER = '/opt/airflow/data'
CONFIG_FOLDER = '/opt/airflow/env_conf'

# setup google-cloud connection and configuration
BUSINESS_DOMAIN = 'networkrail-airflow'
PROJECT_ID = 'networkrail-airflow-dbt'
GCS_BUCKET = 'networkrail_bucket'
BIGQUERY_DATASET = 'networkrail'
LOCATION = 'asia-southeast1'
GCS_SERVICE_ACCOUNT_FILE = f'{CONFIG_FOLDER}/networkrail-airflow-dbt-load-data-to-gcs.json'
BIGQUERY_SERVICE_ACCOUNT_FILE = f'{CONFIG_FOLDER}/networkrail-airflow-dbt-get-gcs-data-then-load-to-bq.json'