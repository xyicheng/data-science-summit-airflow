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

def transformData(country, ds_nodash, *args, **kwargs):
  logging.info('Transforming data file')
  logging.info('Country: %s', country)
  logging.info('Date: %s', ds_nodash)

  # read file into a dataframe
  df = pd.read_csv('/tmp/shazam_{}_{}.txt'.format(
    country,
    ds_nodash
  ), sep='\t',  error_bad_lines=False)
  logging.info('Dimensions: %s', df.shape)

  #rename columns, according to the schema
  df.columns = ['row_num', 'country', 'partner_report_date', 'track', 'artist', 'isrcs']

  #add `load_datetime` column with current timestamp
  df['load_datetime'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

  #change `partner_report_date` column to YYYY-MM-DD format
  df['partner_report_date'] = df['partner_report_date'].apply(
    lambda d: datetime.strptime(str(d), '%Y%m%d')
                      .strftime('%Y-%m-%d')
    )

  #write to a combined file
  with open('/tmp/shazam_combined_{}.txt'.format(ds_nodash), 'a') as f:
    df.to_csv(f, header=False, sep='\t')

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

upload_to_gcs = FileToGoogleCloudStorageOperator(
    task_id = 'upload_to_gcs',
    dst = 'airflow-training/shazam/shazam_combined_{{ ds_nodash }}.txt',
    bucket = '{{ var.value.project_bucket }}',
    conn_id = 'google_cloud_default',
    trigger_rule='all_done', 
    src = '/tmp/shazam_combined_{}.txt'.format(
      '{{ ds_nodash }}'
    ),
    dag = dag
)

ingest_to_bq = GcsToBqOperator(
    task_id = 'ingest_to_bq',
    gcs_uris = ['gs://{{ var.value.project_bucket }}/airflow-training/shazam/shazam_combined_{{ ds_nodash }}.txt'],
    destination_table = '{{ var.value.shazam_bq_table }}${{ ds_nodash }}',
    schema = [  {'name': 'row_num', 'type': 'string', 'mode': 'nullable'},
      {'name': 'id', 'type': 'string', 'mode': 'nullable'},
      {'name': 'country', 'type': 'string', 'mode': 'nullable'},
      {'name': 'partner_report_date', 'type': 'date', 'mode': 'nullable'},
      {'name': 'track', 'type': 'string', 'mode': 'nullable'},
      {'name': 'artist', 'type': 'string', 'mode': 'nullable'},
      {'name': 'isrcs', 'type': 'string', 'mode': 'nullable'},
      {'name': 'load_datetime', 'type': 'timestamp', 'mode': 'nullable'} ],
    source_format = 'CSV',
    field_delimiter = '\t',
    bigquery_conn_id = 'bigquery_default',
    write_disposition = 'WRITE_TRUNCATE',
    dag = dag
)

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

  transform = PythonOperator(
    task_id = 'transform_data_{}'.format(
      country
    ),
    python_callable = transformData,
    provide_context = True,
    op_kwargs = [('country', country)],
    dag=dag
  )

  


  download_file >> verify_download >> transform >> upload_to_gcs

upload_to_gcs >> ingest_to_bq
                 