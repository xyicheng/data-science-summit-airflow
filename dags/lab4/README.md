# Lab 5. Using S3 Sensor to load files into BigQuery as soon as they are uploaded to S3

### Step 1. Create AWS profile

1. Configure AWS access with provided keys

```
$ aws configure --profile=airflow_training

```

### Step 2. Create S3 Sensor

1. Create new `lab4.py` DAG file

2. Add the following code:

```
from __future__ import print_function
import airflow
import pytz
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators import FileToGoogleCloudStorageOperator
from airflow.operators import GcsToBqOperator

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
          description = 'Using S3 Sensor',
          schedule_interval = None,
          default_args = default_args)
```

3. Add import statement for `S3KeySensor`

```
from airflow.operators.sensors import S3KeySensor

```

4. Add `s3_sensor` task to the DAG file

```
s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    poke_interval = 10,
    timeout = 1000,
    bucket_key = 's3://{{ var.value.s3_bucket }}/shazam/shazam_*',
    wildcard_match = True,
    s3_conn_id = 's3_default',
    dag=dag)
```

5. Go to `Admin -> Variables`  and create new global variable with name `s3_bucket` and value `s3://umg-airflow_training`

6. Go to `Admin -> Connections` and create a new connection called `s3_default` and following properties:

* `Conn Id`: `s3_default`
* `Conn Type`: `S3`
* `Extra`: `{"aws_access_key_id":"", "aws_secret_access_key": ""}`

Leave the rest of fields blank
 
7. 



