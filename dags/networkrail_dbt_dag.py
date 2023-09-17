from airflow.utils import timezone

from cosmos.providers.dbt import DbtDag

default_args = {
    "owner" : "kids pkf",
    "start_date" : timezone.datetime(2023, 5, 1),
    "email" : ['phakawat.fongchai.code@gmail.com'],
    "email_on_failure" : True,
    "email_on_retry" : False,
}


networkrail_dbt_dag = DbtDag(
    dag_id="networkrail_dbt_dag",
    schedule_interval="@hourly",
    default_args=default_args,
    conn_id="networkrail_dbt_bigquery_conn",
    catchup=False,
    dbt_project_name="networkrail",
    dbt_args={
        "schema": "networkrail_airflow" ## dataset name
    },
    dbt_root_path="/opt/airflow/dbt",
    max_active_runs=4,
    tags=["networkrail"],
)