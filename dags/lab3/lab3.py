from __future__ import print_function
import airflow
import pytz
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

def verifyWgetReturnCode(*args, **kwargs):

    #get country code from kwargs
    country = kwargs['country']
    logging.info('Verifying WGET return code for country: %s', country)

    #create task instance from kwargs
    task_instance = kwargs['ti']

    #pull wget return code from XCom
    upstream_task_id = 'download_file_{}'.format(
      country
    )
    wget_code = task_instance.xcom_pull(
      upstream_task_id
    )
    logging.info('WGET return code from task %s is %s',
      upstream_task_id,
      wget_code
    )

    # Return True if return code was 0 (no errors)
    if(wget_code == '0'):
        return True
    else:
        return False

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

dag = DAG('lab3',
          description = 'Using PythonOperator for file processing',
          schedule_interval = None,
          default_args = default_args)


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

  verify_download = ShortCircuitOperator(
    task_id = 'verify_download_{}'.format(
      country
    ),
    python_callable = verifyWgetReturnCode,
    provide_context = True,
    op_kwargs = [('country', country)],
    dag=dag
  )

  download_file >> verify_download




                 