from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
default_args = {
'owner': 'AGanshin',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2025, month=1, day=9).in_timezone('Europe/Moscow'),
'email': ['alex@alex.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)
}
#DAG1
dag1 = DAG('AGanshin001',
default_args=default_args,
description="seminar_6",
catchup=False,
schedule_interval='0 6 * * *')
task1 = BashOperator(
task_id='pyspark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/andrey/s6.py',
dag=dag1)
task2 = BashOperator(
task_id='spark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/andrey/s6s1.scala',
dag=dag1)
task2 >> task1
#DAG3
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import inspect,create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pandas.io import sql
import time
#pip install openpyxl
dag3 = DAG('AGanshin003',
default_args=default_args,
description="seminar_7",
catchup=False,
schedule_interval='0 8 * * *')
def hello(**kwargs):
  encoding="ISO-8859-1"
  print('Hello from {kw}'.format(kw=kwargs['my_keyword']))
  df=5+5
  print(df)
  df=pd.read_excel('/home/andrey/s4_2.xlsx')
  con=create_engine("mysql://Airflow:1@localhost:33061/spark")
  print(df)
  df['долг'] = df['Платеж по основному долгу'].cumsum()
  df['проценты'] = df['Платеж по процентам'].cumsum()
  df.to_sql('credit',con,schema='spark',if_exists='replace',index=False)
t2 = PythonOperator(
task_id='python3',
dag=dag3,
python_callable=hello,
op_kwargs={'my_keyword': 'Airflow 1234'}
)

