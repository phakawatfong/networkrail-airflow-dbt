# Network Rail 

[project overview](https://github.com/phakawatfong/networkrail-airflow-dbt/blob/main/pictures/Project_overview.png)

### Port for each services

Airflow
```
localhost:8081
```

DBT (data built tools)
```
localhost:8080
```

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

add this option when ***rerun*** backfill dag

```
airflow dags backfill -s 2023-05-24 -e 2023-09-10 networkrail-airflow-dbt-kids-project [--reset-dagruns]
```

### Setup configuration with these files to manage dbt dependencies
- pyproject.toml

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

## Configure DBT project

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
