## Crawler Scheduler 

### Crawler orchestration by Airflow

```bash
docker build . -t crawler_scheuler:2.4.1-1
```

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
2. fillup past date by airflow dags backfill

#### FLOW CHART

![image](imgs/tw_stock_price_flow.png)

```bash
airflow dags backfill tw_stock_price_daily --start-date 20200101 --end-date 20221022 
```