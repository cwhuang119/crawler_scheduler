FROM python:3.9-buster
RUN pip install fin-crawler
RUN pip install sqlalchemy
RUN adduser --disabled-password --gecos '' airflow
USER airflow
WORKDIR /workspace
ADD crawler.py /workspace/