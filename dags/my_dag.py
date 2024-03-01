from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

def upload_files_to_s3():
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    s3_bucket = 'sample-airflow-bucket09'
    local_directory = './data'

    for file in os.listdir(local_directory):
        if file.endswith(".csv"):
            local_file_path = os.path.join(local_directory, file)
            s3_key = f'dags/{file}'
            s3_hook.load_file(filename=local_file_path, key=s3_key, bucket_name=s3_bucket)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # test
}

with DAG('upload_to_s3_dynamic', default_args=default_args, schedule_interval='@daily') as dag:
    upload_task = PythonOperator(
        task_id='upload_files_to_s3',
        python_callable=upload_files_to_s3
    )

upload_task
