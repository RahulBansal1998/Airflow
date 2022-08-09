"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator


from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 7),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("store", default_args=default_args,template_searchpath=['/usr/local/airflow/dags'],schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators

def transform_function():
    pass
    # print(os.getcwd())
    # df = pd.read_csv('/usr/local/airflow/dags/raw_store_transactions.csv')
    # df['STORE_LOCATION'] = df['STORE_LOCATION'].str[0:8]
    # col_list = ['MRP','CP','DISCOUNT','SP']
    # for i in col_list:
    #     df[i] = df[i].str[1:]
    
    # df.to_csv('raw_store_transactions_2.csv')
    # print(df.head())

python_task = PythonOperator(
    task_id="python_task",
    python_callable=transform_function,
    dag=dag
    # op_kwargs: Optional[Dict] = None,
    # op_args: Optional[List] = None,
    # templates_dict: Optional[Dict] = None
    # templates_exts: Optional[List] = None
)

# create_retail_table = PostgresOperator(
#     task_id="create_retail_table",
#     postgres_conn_id="postgres_default",
#     sql="""
#     CREATE TABLE IF NOT EXISTS clean_store_transactions(STORE_ID varchar(50), STORE_LOCATION varchar(50), PRODUCT_CATEGORY varchar(50), PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, DATE date);""",
#     dag=dag
# )

create_retail_table = MySqlOperator(task_id='create_mysql_table', mysql_conn_id="mysql_conn", sql="""CREATE TABLE IF NOT EXISTS 
    clean_store_transactions(STORE_ID varchar(50), STORE_LOCATION varchar(50), 
    PRODUCT_CATEGORY varchar(50), PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, DATE date);""",
    dag=dag)

load_retail_data_to_table = MySqlOperator(task_id='load_table', mysql_conn_id="mysql_conn", sql="""LOAD DATA INFILE '/store_files_mysql/clean_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;""",dag=dag)

get_results = MySqlOperator(task_id='get_results', mysql_conn_id="mysql_conn", sql="select_from_table.sql",dag=dag)

send_email = email_task = EmailOperator(
    task_id="email_task",
    to=['rahull.bansall16@gmail.com'],
    subject="Hello from Airflow!",
    html_content="<i>Message from Airflow</i>",
    # files=['file1.txt'],
    # mime_subtype: str = 'mixed',
    # mime_charset: str = 'utf-8',
    # conn_id: Optional[str] = None,
    dag=dag
)

python_task >> create_retail_table >> load_retail_data_to_table >> get_results >> send_email

# t2.set_upstream(t1)
# t3.set_upstream(t1)