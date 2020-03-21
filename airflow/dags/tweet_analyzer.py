#from datetime import timedelta
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.twitter_plugin import TweetsToS3Operator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.operators.aws_plugin import S3DeleteObjectsOperator
import datetime,requests,collections,logging,json
from textblob import TextBlob

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'params': {
        "s3_conn_id": "j17devbucketdata",
        "s3_bucket": "j17devbucket"
    }
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

def generate_tweet_graphs():
	logging.info("Generating graphs")
	return True

def get_tweet_sentiment():
	logging.info("Performing sentiment analysis")
	return True

def write_results_to_db():
	logging.info("Writing results to db")
	return True

def clean_up_s3():
    logging.info("Cleaning up files on S3")
    return True

def push_timestamp(**kwargs):
    utcnow = datetime.datetime.utcnow()
    timestamp = utcnow.strftime("%Y%m%dT%H%M%SZ")
    return timestamp


with DAG(
    dag_id='tweet_analyzer', 
    default_args=default_args, 
    description='Pulls tweets about a given topic from twitter for analysis',
    #schedule_interval=timedelta(days=1),
    ) as dag:

    set_time_stamp = PythonOperator(
        task_id='set_timestamp',
        description='Sets the timestamp for the dag run and file names',
        python_callable=push_timestamp
    )

    tweets_to_s3 = TweetsToS3Operator(
        task_id='tweets_to_s3',
        description='Writes tweets about a certain topic to S3',
        topic='COVID-19',
        s3_conn_id='{{ params.s3_conn_id }}',
        s3_bucket='{{ params.s3_bucket }}',
        s3_key='tweet_data.{{ task_instance.xcom_pull(task_ids="set_timestamp") }}'
    )

    etl_tweets = S3FileTransformOperator(
        task_id='etl_tweet_jsons',
        description='cleans the tweet jsons pulled',
        source_s3_key='s3://j17devbucket/tweet_data.{{ task_instance.xcom_pull(task_ids="set_timestamp") }}',
        dest_s3_key='s3://j17devbucket/tweet_data_cleaned_{{ task_instance.xcom_pull(task_ids="set_timestamp") }}.csv',
        transform_script='etl_scripts/clean_tweets_pipeline.py',
        source_aws_conn_id='{{ params.s3_conn_id }}',
        dest_aws_conn_id='{{ params.s3_conn_id }}'
    )

    generate_graphs = PythonOperator(
        task_id='generate_graphs',
        description='Creates graphs based on tweets gathered',
        python_callable=generate_tweet_graphs
    )

    get_sentiment = PythonOperator(
        task_id='get_sentiment',
        description='Get sentiment of tweets',
        python_callable=get_tweet_sentiment
    )

    results_to_dynamoDB = PythonOperator(
        task_id='write_to_dynamoDB',
        description='Writes results to dynamoDB',
        python_callable=write_results_to_db
    )

    clean_up = PythonOperator(
        task_id='clean_up_s3',
        description='Clean up files on s3',
        python_callable=clean_up_s3
    )

    #set_time_stamp >> tweets_to_s3 >> etl_tweets >> [generate_graphs,get_sentiment] >> results_to_dynamoDB
    etl_tweets >> [generate_graphs,get_sentiment] >> results_to_dynamoDB