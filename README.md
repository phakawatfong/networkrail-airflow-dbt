# Network Rail 

![project overview](https://github.com/phakawatfong/networkrail-airflow-dbt/blob/main/pictures/Project_overview.png)

I built a data pipeline to extract data from a Postgres database and put it in Google Cloud Storage as a Data Lake technology. Following that, an object is created in Google BigQuery for Data Warehouse technology, and comprehensive data modeling is done in DBT  to allow for additional visualization and analytics via Looker Studio. Apache Airflow is used to orchestrate the entire project, and Docker is used for containerization. 

### End-result
links to project dashboard: [https://lookerstudio.google.com/s/oOED5knwdTs]
![project_dashboard](https://github.com/phakawatfong/networkrail-airflow-dbt/blob/main/pictures/project_dashboard.png)


### Connections

The connections for each service will be configured in ***Airflow Connections*** 
under credentials authentication of ```google service_account```

![project_connection](https://github.com/phakawatfong/networkrail-airflow-dbt/blob/main/pictures/projects_connection.png)

Postgres - 'networkrail_postgres_conn'
DBT GCP - 'networkrail_dbt_bigquery_conn'

### Port for each services

Airflow
```
localhost:8081
```

DBT (data built tools)
```
localhost:8080
```

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

add this option when ***rerun*** backfill dag

```
airflow dags backfill -s 2023-05-24 -e 2023-09-10 networkrail-airflow-dbt-kids-project --reset-dagruns
```

### project pipeline to get data from Postgres and insert into Data Warehouse (Google Bigquery)

![project_pipeline](https://github.com/phakawatfong/networkrail-airflow-dbt/blob/main/pictures/networkrail-airflow-dbt-DAG.png)

## Configuration files for DBT project
*(to manage project dependencies, such as libraries)*
- pyproject.toml

*(to setup dbt project configure)*
- profiles.yml 

*(to setup data source for Data Modeling)*
- src.yml 

*(to model our project / Data zonning / materialization; VIEW or TABLE will be configured by this file)*
- dbt_projects.yml 

*(to document our model)
- models.yml

*(to configure project dependencies)*
- pakages.yml

*(to load csv files from dbt project to DataWarehouse)

ref: [https://docs.getdbt.com/docs/build/seeds]
- seeds.yml [Seeds are best suited to static data which changes infrequently.]
run this command to initiate poetry to create virsual environment for our project

```
poetry install
```

run this command to the poetry shell, to work under our configured dependencies (Run this command everytime you start working on a project)

```
poetry shell
```

run this command to initiate dbt (note to change folder into the dbt folder)

```
cd dbt
poetry run dbt init 
```


run this command to setup our dbt project (in this case replace <project_name> with **networkrail**)

```
cd <project_name> 
dbt debug --profiles-dir ..
dbt run --profiles-dir ..
```


run this command to prepare folder for Data Model (you need to in the */dbt/networkrail* folder first)
***in this case there will be 3 data zonning***
---
** _STAGING_ -> _PERSISTED_ -> _CURATED_ **

```
cd models/
mkdir ./staging ./persisted ./curated
```

run this command each time new data model applied.

```
dbt run --profiles-dir ..
```

In case, you want to specify which model that would be executed.

***dbt run --select <full_path_to_model> --profiles-dir ..***
```
dbt run --select models/staging/networkrail/stg_networkrail__movements.sql --profiles-dir ..
```

## Documentation of each field for movements table from Networkrail

[https://wiki.openraildata.com/index.php?title=Train_Movement]


to install dbt dependencies on our `packages.yml`

```
dbt run deps --profiles-dir ..
```

to see dbt documentation

```
dbt docs generate --profiles-dir ..
dbt docs serve --profiles-dir ..
```

to run Each Test cases

```
dbt test --profiles-dir ..
```

to import csv file from seeds directory

```
dbt seed --profiles-dir ..
```

### dbt pipeline to Orchestrate data-modeling

![dbt-pipeline](https://github.com/phakawatfong/networkrail-airflow-dbt/blob/main/pictures/dbt-DAG.png)

### dbt data-lineage

![dbt-lineage](https://github.com/phakawatfong/networkrail-airflow-dbt/blob/main/pictures/dbt-lineage.png)
## credits
[https://landing.skooldio.com/data-engineering-bootcamp]
