from datetime import datetime, timedelta, date
from re import M
from airflow import DAG
import airflow
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Tom Huang',
    'start_date': airflow.utils.dates.days_ago(1),
    # 'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(hours=1)
}

def dummy_fn(date):
    print('test'+str(date))

def dummy_fn2(date):
    print('test2'+str(date))

for i in range(2):
    dag = DAG(f'celery_test_{i}',default_args={
        'owner': 'Tom Huang',
        'start_date': airflow.utils.dates.days_ago(1),
        'retries': 2,
        'retry_delay': timedelta(hours=1),
        'tags':[str(i)]
    })
    with dag:
        dummy_fn_task = PythonOperator(
            task_id='dummy_fn',
            python_callable=dummy_fn,
            op_kwargs = {"date": str(i)}
        )
        dummy_fn2_task = PythonOperator(
            task_id = 'dummy_fn2',
            python_callable=dummy_fn2,
            op_kwargs = {"date": str(i)}
        )
        dummy_fn_task >> dummy_fn2_task

        