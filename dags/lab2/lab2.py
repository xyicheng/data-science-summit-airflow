from __future__ import print_function
import airflow
import pytz
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

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

""" 
# Step 1.
download_file = BashOperator(
  task_id = 'download_file',
  bash_command = 'wget https://raw.githubusercontent.com/umg/data-science-summit-airflow/master/data/shazam/shazam_AR_20171029.txt -O /tmp/shazam_AR_20171029.txt',
  dag = dag
)
"""

"""
# Step 2
download_file = BashOperator(
  task_id = 'download_file',
  bash_command = 'wget $URL/shazam_AR_20171029.txt -O /tmp/shazam_AR_20171029.txt',
  env = {'URL': '{{ var.value.shazam_files_url }}'},
  dag = dag
)
"""

"""
# Step 3
download_file = BashOperator(
  task_id = 'download_file',
  bash_command = 'wget $URL/shazam_AR_20171029.txt -O /tmp/shazam_AR_20171029.txt; echo $?' ,
  env = {'URL': '{{ var.value.shazam_files_url }}'},
  xcom_push = True,
  dag = dag
)
"""

"""
# Step 4
download_file = BashOperator(
  task_id = 'download_file',
  bash_command = 'wget $URL/shazam_AR_$EXEC_DATE.txt -O /tmp/shazam_AR_$EXEC_DATE.txt; echo $?',
  env={'URL': '{{ var.value.shazam_files_url }}',
    'EXEC_DATE': '{{ ds_nodash }}'},
  xcom_push = True, 
  dag = dag
)
"""

# Step 5
shazam_country_list = Variable.get('shazam_country_list').split(',')

for country in shazam_country_list:
  download_file = BashOperator(
  task_id = 'download_file_{}'.format(
    country
  ),
  bash_command = 'wget $URL/shazam_{}_$EXEC_DATE.txt -O /tmp/shazam_{}_$EXEC_DATE.txt; echo $?'.format(
    country, 
    country
  ),
  env={'URL': '{{ var.value.shazam_files_url }}',
    'EXEC_DATE': '{{ ds_nodash }}'},
  xcom_push = True, 
  dag = dag
)



                 