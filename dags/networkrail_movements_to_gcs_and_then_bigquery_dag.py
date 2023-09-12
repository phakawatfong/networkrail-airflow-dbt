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

    # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-variables
    ds = context["data_interval_start"].to_date_string()

    # https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_modules/airflow/providers/postgres/hooks/postgres.html#PostgresHook.schema
    pg_hook = PostgresHook(
        postgres_conn_id='networkrail_postgres_conn',
        schema='networkrail'
    )

    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f" select * from {DATA} where date(actual_timestamp) = '{ds}' "

    cursor.execute(sql)
    results = cursor.fetchall()

    # write result to file
    with open(f"{DATA_FOLDER}/{DATA}-{ds}.csv", "w") as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(header)
        for row in results:
            csvwriter.writerow(row)

    with open(f"{DATA_FOLDER}/{DATA}-{ds}.csv", 'r') as fp:
        lines = len(fp.readlines())
        print('Total Number of lines:', lines)

    # Validate if data is alread extracted
    check_if_file_exists = os.path.isfile(f"{DATA_FOLDER}/{DATA}-{ds}.csv")
    print(f"Check if '{DATA}-{ds}.csv' exists: {check_if_file_exists}")

    # if os.path.isfile(f"{DAG_FOLDER}/{DATA}-{ds}.csv") and os.stat(f"{DAG_FOLDER}/{DATA}-{ds}.csv").st_size != 0:
    if check_if_file_exists and lines > 1:
        return f"load_{DATA}_to_gcs"
    else:
        return "do_nothing"


def _load_data_to_gcs(**context):

    ds = context["data_interval_start"].to_date_string()

    # gs://networkrail_bucket
    source_file_name = f"{DATA_FOLDER}/{DATA}-{ds}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}-{ds}.csv"

    # https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html
    service_account_info_gcs = json.load(open(GCS_SERVICE_ACCOUNT_FILE))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)

    # https://cloud.google.com/storage/docs/uploading-objects#permissions-client-libraries
    storage_client = storage.Client(project=PROJECT_ID, credentials=credentials_gcs)
    bucket = storage_client.bucket(GCS_BUCKET)
    
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )




## Define DAGS
# https://airflow.apache.org/docs/apache-airflow/1.10.12/tutorial.html
default_args = {
    'owner': 'kids pkf',
    'start_date': timezone.datetime(2023, 5, 1),
    'email': ['phakawat.fongchai.code@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='networkrail-airflow-dbt-kids-project',
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    concurrency=10, 
    max_active_runs=4,
    tags=["networkrail"]
):
    
    start = EmptyOperator(task_id="start")

    extract_data_from_postgres = BranchPythonOperator(
                task_id=f"extract_{DATA}_from_postgres",
                python_callable=_extract_data_from_postgres,
            )

    do_nothing = EmptyOperator(task_id="do_nothing")

    load_data_to_google_cloud_storage = PythonOperator(
                task_id = f"load_{DATA}_to_gcs",
                python_callable=_load_data_to_gcs,
            )

    
    # task dependencies
    start >> extract_data_from_postgres >> load_data_to_google_cloud_storage
    extract_data_from_postgres >> do_nothing