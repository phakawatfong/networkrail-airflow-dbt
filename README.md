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

### Setup configuration with these files to manage dbt dependencies
- pyproject.toml

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