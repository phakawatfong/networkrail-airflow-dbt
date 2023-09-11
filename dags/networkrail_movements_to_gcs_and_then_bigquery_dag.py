import csv
import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils import timezone
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html
from airflow.providers.postgres.hooks.postgres import PostgresHook


# setup project directories
DAG_FOLDER = '/opt/airflow/dags'
DATA_FOLDER = '/opt/airflow/data'
CONFIG_FOLDER = '/opt/airflow/env_conf'

# setup google-cloud connection and configuration
DATA = 'movements'
BUSINESS_DOMAIN = 'networkrail-airflow'
PROJECT_ID = 'networkrail-airflow-dbt'
GCS_BUCKET = 'networkrail_bucket'
BIGQUERY_DATASET = 'networkrail'
LOCATION = 'asia-southeast1'
GCS_SERVICE_ACCOUNT_FILE = f'{CONFIG_FOLDER}/networkrail-airflow-dbt-load-data-to-gcs.json'
BIGQUERY_SERVICE_ACCOUNT_FILE = f'{CONFIG_FOLDER}/networkrail-airflow-dbt-get-gcs-data-then-load-to-bq.json'

header = [
    "event_type",
    "gbtt_timestamp",
    "original_loc_stanox",
    "planned_timestamp",
    "timetable_variation",
    "original_loc_timestamp",
    "current_train_id",
    "delay_monitoring_point",
    "next_report_run_time",
    "reporting_stanox",
    "actual_timestamp",
    "correction_ind",
    "event_source",
    "train_file_address",
    "platform",
    "division_code",
    "train_terminated",
    "train_id",
    "offroute_ind",
    "variation_status",
    "train_service_code",
    "toc_id",
    "loc_stanox",
    "auto_expected",
    "direction_ind",
    "route",
    "planned_event_type",
    "next_report_stanox",
    "line_ind",
]

def _extract_data_from_postgres(**context):

    # https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_modules/airflow/providers/postgres/hooks/postgres.html#PostgresHook.schema
    pg_hook = PostgresHook(
        postgres_conn_id='networkrail_postgres_conn',
        schema='networkrail'
    )

    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f" select * from {DATA}"

    cursor.execute(sql)
    results = cursor.fetchall()

    # write result to file
    with open(f"{DATA_FOLDER}/{DATA}.csv", "w") as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(header)
        for row in results:
            csvwriter.writerow(row)

    # Validate if data is alread extracted
    if os.path.isfile(f"{DAG_FOLDER}/{DATA}.csv") and os.stat(f"{DAG_FOLDER}/{DATA}.csv").st_size != 0:
        return "load_data_to_gcs"
    else:
        return "do_nothing"

## Define DAGS
# https://airflow.apache.org/docs/apache-airflow/1.10.12/tutorial.html
default_args = {
    'owner': 'kids pkf',
    'start_date': timezone.datetime(2023, 9, 11),
    'email': ['phakawat.fongchai.code@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='networkrail-airflow-dbt-kids-project',
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["networkrail"]
):
    
    start = EmptyOperator(task_id="start")

    extract_movements_from_postgres = BranchPythonOperator(
                task_id=f"extract_{DATA}_from_postgres",
                python_callable=_extract_data_from_postgres
            )

    do_nothing = EmptyOperator(task_id="do_nothing")

    
    # task dependencies
    start >> extract_movements_from_postgres
    extract_movements_from_postgres >> do_nothing