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


# import requests
from fin_crawler import FinCrawler
default_args = {
    'owner': 'Tom Huang',
    'start_date': airflow.utils.dates.days_ago(1),
    # 'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(hours=1)
}



########## FUNCTIONS ##############
def seperate_common_other_data(data):
    
    common_data = []
    other_data = []
    for i in data:
        if len(i['stock_id'])>4:
            other_data.append(i)
        else:
            common_data.append(i)
    return common_data,other_data

def format_sql_insert_value(data):
    
    col_names = [
        "date",
        "stock_id",
        "trade_num",
        "trade_amount",
        "volume",
        "open",
        "high",
        "low",
        "close",
        "spread"
    ]

    insert_values = ''
    for i in data:
        value_str = ''
        for col_name in col_names:
            value = str(i[col_name]).replace('nan',"'Nan'")
            if col_name in ['stock_id','date']:
                value =f"'{value}'"
            value_str+=f"{str(value)},"

        value_str=value_str[:-1]
        value_str = f"({value_str}),"
        insert_values+=value_str
    insert_values=insert_values[:-1]
    insert_values+=';'
    return insert_values

def format_sql_statement(data,table_name='tw_stock'):
    sql_statement = f"INSERT INTO {table_name}(date,stock_id,trade_num,trade_amount,volume,open,high,low,close,spread) VALUES "
    insert_values = format_sql_insert_value(data)
    return sql_statement+insert_values

def get_engine():
    # setup engine
    TW_STOCK_DB_CONNECT_STR = Variable.get("TW_STOCK_DB_CONNECT_STR")
    engine = create_engine(TW_STOCK_DB_CONNECT_STR)
    return engine

########### END FUNCTIONS ########

def create_table_if_not_exist():

    engine = get_engine()

    # #check existing tables
    # inspector = inspect(engine)
    # schemas = inspector.get_schema_names()
    # table_names = []
    # for schema in schemas:
    #     for table_name in inspector.get_table_names(schema=schema):
    #         table_names.append(table_name)

    # # Create table
    sql_cmd = """
    CREATE TABLE IF NOT EXISTS TW_STOCK (
        id SERIAL PRIMARY KEY,
        date DATE,
        stock_id VARCHAR(255),
        trade_num FLOAT,
        trade_amount FLOAT,
        volume FLOAT,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        spread FLOAT
    );
    """
    # if 'tw_stock' not in table_names:
    with engine.connect() as con:
        statement = text(sql_cmd)
        con.execute(statement)

    sql_cmd = """
    CREATE TABLE IF NOT EXISTS TW_STOCK_O (
        id SERIAL PRIMARY KEY,
        date DATE,
        stock_id VARCHAR(255),
        trade_num FLOAT,
        trade_amount FLOAT,
        volume FLOAT,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        spread FLOAT
    );
    """
    # if 'tw_stock_o' not in table_names:
    with engine.connect() as con:
        statement = text(sql_cmd)
        con.execute(statement)

def crawl_data(ti,date):
    insert_db = False
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT date FROM tw_stock WHERE date='{date}'")).all()
    if len(rows)==0:
        print("Start crawl",date)
        data = FinCrawler.get('tw_stock_price_daily',{'date':date})
        if len(data)>0:
            insert_db = True
            ti.xcom_push(key = 'data', value = data)
        else:
            print('empty data')
    else:
        print('data already in db')
    ti.xcom_push(key = 'insert_db', value = insert_db)

def format_data(ti):
    if ti.xcom_pull(task_ids='crawl_data',key='insert_db'):
        # seperate common and other
        data = ti.xcom_pull(task_ids='crawl_data',key='data')
        common_data,other_data = seperate_common_other_data(data)
        ti.xcom_push(key = 'common_data', value = common_data)
        ti.xcom_push(key = 'other_data', value = other_data)

def insert_db_common(ti):
    if ti.xcom_pull(task_ids='crawl_data',key='insert_db'):
        TW_STOCK_DB_CONNECT_STR = Variable.get("TW_STOCK_DB_CONNECT_STR")
        engine = create_engine(TW_STOCK_DB_CONNECT_STR)
        common_data = ti.xcom_pull(task_ids='format_data',key='common_data')
        common_data_statement = format_sql_statement(common_data,'tw_stock')
        with engine.connect() as conn:
            conn.execute(text(common_data_statement))

def insert_db_other(ti):
    if ti.xcom_pull(task_ids='crawl_data',key='insert_db'):
        TW_STOCK_DB_CONNECT_STR = Variable.get("TW_STOCK_DB_CONNECT_STR")
        engine = create_engine(TW_STOCK_DB_CONNECT_STR)
        other_data = ti.xcom_pull(task_ids='format_data',key='other_data')
        other_data_statement = format_sql_statement(other_data,'tw_stock_o')
        with engine.connect() as conn:
            conn.execute(text(other_data_statement))

def send_notification():
    print('send notification')

with DAG('tw_stock_price_daily', default_args=default_args, schedule_interval='0 10 * * *') as dag:

    create_table_if_not_exist_task = PythonOperator(
        task_id='create_table_if_not_exist',
        python_callable=create_table_if_not_exist
    )

    crawl_data_task = PythonOperator(
        task_id='crawl_data',
        python_callable=crawl_data,
        op_kwargs = {"date": datetime.strftime(date.today(),'%Y%m%d')}
    )

    format_data_task = PythonOperator(
        task_id='format_data',
        python_callable=format_data
    )

    insert_db_common_task = PythonOperator(
        task_id='insert_db_common',
        python_callable=insert_db_common
    )

    insert_db_other_task = PythonOperator(
        task_id='insert_db_other',
        python_callable=insert_db_other
    )

    send_notification_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification
    )
    create_table_if_not_exist_task >> crawl_data_task
    crawl_data_task >> format_data_task
    format_data_task >> insert_db_common_task >> send_notification_task
    format_data_task >> insert_db_other_task >> send_notification_task
