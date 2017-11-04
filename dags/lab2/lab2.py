from __future__ import print_function
import airflow
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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

dag = DAG('lab2',
          description = 'Using BashOperator and WGET to download files',
          schedule_interval = None,
          default_args = default_args)

download_file = BashOperator(
  task_id = 'dowload_file',
  bash_command = 'wget https://github.com/umg/data-science-summit-airflow/blob/master/data/shazam/shazam_AR_20171029.txt -O /tmp/shazam_AR_20171029.txt',
  dag = dag
)



                 