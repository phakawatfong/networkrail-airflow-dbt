# This is the data engineering project, I practices to create a pipeline of UK Data Networkrail in order to use data for Monitoring the UK-rail transportation performance.

# prequistitions
- python3
- Docker Desktop

# Setup configuration with these files
- .gitignore
- docker-compose.yaml
- Dockerfile


# for docker-compose.yaml, use this command to create folder to setup project structure for Airflow.

mkdir -p ./dags ./config ./logs ./plugins ./tests ./env_conf ./data ./dbt
echo -e "AIRFLOW_UID=$(id -u)" > .env

# use this command to startup the Airflow with Docker.

docker compose up --build