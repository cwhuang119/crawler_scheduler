## Crawler Scheduler 

### Crawler orchestration by Airflow


[Airflow install tutorial (Docker)](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

### Airflow install & setup
```bash
# download docker compose file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.4.1/docker-compose.yaml'
# create local folder to mount into docker container
mkdir -p ./dags ./logs ./plugins
# setup airflow ID
echo -e "AIRFLOW_UID=$(id -u)" > .env

# initialize db
docker-compose up airflow-init
# cleanup env (if needed)
docker-compose down --volumes --remove-orphans
# launch ariflow service
docker compose up
```

### Ariflow Variable setting
1. TW_STOCK_DB_CONNECT_STR: postgresql://username:password@host:port/db_name

