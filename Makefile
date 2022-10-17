#https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
airflow-init-db:
	docker-compose up airflow-init
airflow-up:
	docker-compose up
airflow-down:
	docker-compose down