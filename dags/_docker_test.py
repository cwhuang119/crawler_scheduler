
from datetime import datetime, timedelta
from airflow import DAG
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner':'Tom Huang',
    'start_date': airflow.utils.dates.days_ago(1),
    'retries':1,
    'retry_delay':timedelta(hours=1)
}

dag = DAG(
    'docker_test',
    default_args=default_args,
)

with dag:

    task_crawl = DockerOperator(
        task_id='docker_test_1',
        image='crawler:base',
        container_name='task___fin_crawler',
        api_version='auto',
        auto_remove=True,
        command="python crawler.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
        )

    task_crawl