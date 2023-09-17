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
BUSINESS_DOMAIN = 'networkrail_airflow'
PROJECT_ID = 'networkrail-airflow-dbt'
GCS_BUCKET = 'networkrail_bucket'
BIGQUERY_DATASET = 'networkrail'
LOCATION = 'asia-southeast1'
GCS_SERVICE_ACCOUNT_FILE = f'{CONFIG_FOLDER}/networkrail-airflow-dbt-load-data-to-gcs.json'
BIGQUERY_SERVICE_ACCOUNT_FILE = f'{CONFIG_FOLDER}/networkrail-airflow-dbt-get-gcs-data-then-load-to-bq.json'


# Define Header for .csv file
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

# https://cloud.google.com/bigquery/docs/schemas
# predefined Bigquery Schema for NetworkRail.
bigquery_schema = [
    bigquery.SchemaField("event_type", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("gbtt_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("original_loc_stanox", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("planned_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("timetable_variation", bigquery.enums.SqlTypeNames.INTEGER),
    bigquery.SchemaField("original_loc_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("current_train_id", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("delay_monitoring_point", bigquery.enums.SqlTypeNames.BOOLEAN),
    bigquery.SchemaField("next_report_run_time", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("reporting_stanox", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("actual_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("correction_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
    bigquery.SchemaField("event_source", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("train_file_address", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("division_code", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("train_terminated", bigquery.enums.SqlTypeNames.BOOLEAN),
    bigquery.SchemaField("train_id", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("offroute_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
    bigquery.SchemaField("variation_status", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("train_service_code", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("toc_id", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("loc_stanox", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("auto_expected", bigquery.enums.SqlTypeNames.BOOLEAN),
    bigquery.SchemaField("direction_ind", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("route", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("planned_event_type", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("next_report_stanox", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("line_ind", bigquery.enums.SqlTypeNames.STRING),
]

def _extract_data_from_postgres(**context):

    # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-variables
    ds = context["data_interval_start"].to_date_string()
    # ds = '2023-09-10'

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
    # check if our extracted csv file have records, then return to specific task.
    if check_if_file_exists and lines > 1:
        return f"load_{DATA}_to_gcs"
    elif lines == 1:
        return "delete_empty_file"
    else:
        return "do_nothing"
    

def _delete_empty_file(**context):

    ds = context["data_interval_start"].to_date_string()
    # ds = '2023-09-10'

    absolute_path = f"{DATA_FOLDER}"
    file_name = f"{DATA}-{ds}.csv"

    # ensure that file contains only header again
    with open(f"{absolute_path}/{file_name}", 'r') as fp:
        number_of_lines = len(fp.readlines())
        print('Total Number of lines:', number_of_lines)

        if number_of_lines == 1:
            try:
                os.remove(f"{absolute_path}/{file_name}")
                print("% s removed successfully" % file_name)
            except OSError as error:
                print(error)
                print(f"{file_name} can not be removed !!")


def _load_data_to_gcs(**context):

    ds = context["data_interval_start"].to_date_string()
    # ds = '2023-09-10'

    # https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html
    service_account_info_gcs = json.load(open(GCS_SERVICE_ACCOUNT_FILE))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)

    source_file_name = f"{DATA_FOLDER}/{DATA}-{ds}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}-{ds}.csv"

    # https://cloud.google.com/storage/docs/uploading-objects#permissions-client-libraries
    storage_client = storage.Client(project=PROJECT_ID, credentials=credentials_gcs)
    bucket = storage_client.bucket(GCS_BUCKET)
    
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


def _get_data_from_gcs_then_load_to_bg(**context):
    
    ds = context["data_interval_start"].to_date_string()
    # ds = '2023-09-10'

    service_account_info_bq = json.load(open(BIGQUERY_SERVICE_ACCOUNT_FILE))
    credentials_bq = service_account.Credentials.from_service_account_info(service_account_info_bq)

    bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials_bq)

    # https://github.com/googleapis/python-bigquery/blob/35627d145a41d57768f19d4392ef235928e00f72/samples/client_load_partitioned_table.py
    job_config = bigquery.LoadJobConfig(
                schema = bigquery_schema,
                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="actual_timestamp",  # Name of the column to use for partitioning.
                    ),
            )

    # gs://networkrail_bucket/networkrail-airflow/networkrail/<DATA>.csv
    partition = ds.replace("-", "")
    table_id = f"{PROJECT_ID}.{BUSINESS_DOMAIN}.{DATA}${partition}"
    uri = f"gs://{GCS_BUCKET}/{BUSINESS_DOMAIN}/{DATA}/{DATA}-{ds}.csv"

    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    ) # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bigquery_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


    with open(f"{DATA_FOLDER}/{DATA}-{ds}.csv", 'r') as fp:
        lines = len(fp.readlines())
        print('Total Number of lines:', lines)

    # validate to number of inserted records
    row_exclude_header = lines - 1
    if row_exclude_header == destination_table.num_rows:
        return "validate_inserted_rows"
    else:
        return "insert_failed"


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
    max_active_runs=10,
    tags=["networkrail"]
):
    
    start = EmptyOperator(task_id="start")

    extract_data_from_postgres = BranchPythonOperator(
                task_id = f"extract_{DATA}_from_postgres",
                python_callable = _extract_data_from_postgres,
            )

    delete_empty_extracted_file = PythonOperator(
                task_id = f"delete_empty_file",
                python_callable = _delete_empty_file,
            )

    do_nothing = EmptyOperator(task_id="do_nothing")

    load_data_to_google_cloud_storage = PythonOperator(
                task_id = f"load_{DATA}_to_gcs",
                python_callable = _load_data_to_gcs,
            )
    
    get_data_from_google_cloud_storage_then_load_to_bigquery = BranchPythonOperator(
                task_id = f"get_{DATA}_from_google_cloud_storage_then_load_to_bigquery",
                python_callable = _get_data_from_gcs_then_load_to_bg,
    )

    validate_inserted_records_between_csv_and_bigquery = EmptyOperator(task_id="validate_inserted_rows")
    row_diff_between_csv_and_bigquery = EmptyOperator(task_id="insert_failed")

    end = EmptyOperator(task_id="end", trigger_rule="one_success")
    
    # task dependencies
    start >> extract_data_from_postgres >> load_data_to_google_cloud_storage >> get_data_from_google_cloud_storage_then_load_to_bigquery >> validate_inserted_records_between_csv_and_bigquery >> end
    extract_data_from_postgres >> delete_empty_extracted_file >> end
    extract_data_from_postgres >> do_nothing >> end
    get_data_from_google_cloud_storage_then_load_to_bigquery >> row_diff_between_csv_and_bigquery