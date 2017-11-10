from __future__ import print_function
import airflow
import pytz
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators import FileToGoogleCloudStorageOperator
from airflow.operators import GcsToBqOperator
from airflow.operators import GoogleCloudStorageObjectSensor

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
          description = 'Using GCS Sensor',
          schedule_interval = None,
          default_args = default_args)

gcs_sensor = GoogleCloudStorageObjectSensor(
    task_id = 'gcs_sensor',
    bucket = '{{ var.value.project_bucket }}',
    poke_interval = 10,
    object = 'airflow-training/shazam/shazam_AR_20171029.txt',
    dag = dag
)