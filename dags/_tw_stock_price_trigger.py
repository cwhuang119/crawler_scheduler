
import pendulum
from datetime import datetime, timedelta, date
from airflow import DAG
import airflow
from airflow.operators.python_operator import PythonOperator
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from airflow.models import Variable
from sqlalchemy import inspect
import time

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

last_date = date(2022,10,18)
start_date = date(2020,1,1)
date_list=[last_date-timedelta(days=x) for x in range((last_date-start_date).days)]
formated_data_list = [datetime.strftime(x,'%Y%m%d') for x in date_list]
with DAG(
    dag_id="tw_stock_price_range_trigger",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@hourly",
    tags=['tw_stock_price'],
) as dag:
    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        trigger_dag_id="tw_stock_price_daily_past_date",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"date": "2022-10-12"},
    )