from __future__ import print_function
import airflow
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import BqWriteToTableOperator

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

dag = DAG('lab1',
          description = 'Testing the Airflow server set up by loading stream records from Spotify datamart',
          schedule_interval = None,
          default_args = default_args)


load_spotify_streams = BqWriteToTableOperator(
  task_id = 'load_spotify_streams',
  sql = """
    select * 
    from `{{ var.value.spotify_streams_src }}`
    where _partitiontime = timestamp_sub(
      timestamp('{{ yesterday_ds }}'),
      interval 72 hour)
    limit 1000
    """,
  destination_table = '{{ var.value.spotify_streams_dst }}',
  dag = dag
)
                 