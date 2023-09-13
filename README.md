# This is the data engineering project, I practices to create a pipeline of UK Data Networkrail in order to use data for Monitoring the UK-rail transportation performance.

### prequistitions
- python3
- Docker Desktop

### Setup configuration with these files to manage project dependencies
- .gitignore
- docker-compose.yaml
- Dockerfile
- requirements.txt

### for docker-compose.yaml, use this command to create folder to setup project structure for Airflow.

*** run these below commands on terminal

```
mkdir -p ./dags ./config ./logs ./plugins ./tests ./env_conf ./data ./dbt
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### use this command to startup the Airflow with Docker.

```
docker compose up --build
```

### Google-Cloud Platform service_account setup
** Google Cloud Storage
custom roles
-   storage.objects.create
-   storage.objects.delete

** Google Cloud Bigquery
custom roles
-   bigquery.jobs.create
-   bigquery.tables.create
-   bigquery.tables.get
-   bigquery.tables.update
-   bigquery.tables.updateData
-   storage.objects.get


### Run Backfill to catchup all data

```
airflow dags backfill -s <START_PERIOD> -e <END_PERIOD> <DAG_NAME>
```

I ran this below command, in order to perform backfill while run dags

```
airflow dags backfill -s 2023-05-24 -e 2023-09-10 networkrail-airflow-dbt-kids-project
```

add this option to rerun backfill dag

```
airflow dags backfill -s 2023-05-24 -e 2023-09-10 networkrail-airflow-dbt-kids-project --reset-dagruns
```

### Setup configuration with these files to manage dbt dependencies
- pyproject.toml

run this command to initiate poetry to create virsual environment for our project

```
poetry install
```

run this command to the poetry shell, to work under our configured dependencies

```
poetry shell
```

run this command to initiate dbt (note to change folder into the dbt folder)

```
cd dbt
poetry run dbt init 
```

## Configure DBT project

- profiles.yml

run this command to setup our dbt project (in this case replace <project_name> with networkrail)

```
cd <project_name> 
dbt debug --profiles-dir ..
dbt run --profiles-dir ..
```


run this command each time new data model applied.

```
dbt run --profiles-dir ..
```