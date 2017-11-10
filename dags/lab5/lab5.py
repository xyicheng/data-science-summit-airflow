from __future__ import print_function
import airflow
import logging
import sys
import pytz

from os import path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BqLastUpdateOperator,  BqIncrementalLoadDataOperator
from airflow.models import Variable

# setting start date to some value in the past
# we will not be using it, so it's only for consistency of
# DAG configuration
start_date = datetime(2017, 11, 8, 19, 30, 0, tzinfo=pytz.utc)

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


# declare DAG
dag = DAG('incremental_pattern',
          description='Load data incrementally',
          schedule_interval = None,
          default_args=default_args)


last_update_timestamp = BqLastUpdateOperator(
    task_id = 'last_update_timestamp',
    dataset_table = '{{ var.value.transactions_dst }}',
    dag = dag

)

incremental_load = BqIncrementalLoadDataOperator(
    task_id = 'incremental_load',
    sql = 'sql/transactions_daily_partition.sql',
    partition_list_sql = 'sql/transactions_partition_list.sql',
    source_table = '{{ var.value.transactions_src }}',
    destination_table = '{{ var.value.transactions_dst }}',
    source_partition_column = 'transaction_date',
    destination_partition_column = 'transaction_date',
    last_update_column = 'load_datetime', 
    last_update_value = '{{ ti.xcom_pull("last_update_timestamp") }}',
    execution_time = '{{ ts }}',
    dag = dag
)

last_update_timestamp >> incremental_load