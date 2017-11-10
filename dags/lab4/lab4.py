from __future__ import print_function
import airflow
import pytz
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators import FileToGoogleCloudStorageOperator
from airflow.operators import GcsToBqOperator
from airflow.operators.sensors import S3KeySensor

start_date = datetime(2017, 10, 24, 0, 0, 0, tzinfo=pytz.utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': None,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)

}

dag = DAG('lab4',
          description = 'Using S3KeySensor',
          schedule_interval = None,
          default_args = default_args)

s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    poke_interval = 10,
    timeout = 1000,
    bucket_key = 's3://{{ var.value.s3_bucket }}/shazam/shazam_*',
    wildcard_match = True,
    s3_conn_id = 's3_default',
    dag=dag)



