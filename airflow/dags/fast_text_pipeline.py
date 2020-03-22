#from datetime import timedelta
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import datetime,requests,collections,logging,json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='fast_text_pipeline',
    default_args=default_args,
    description='Trains fast text model',
    #schedule_interval=timedelta(days=1),
)