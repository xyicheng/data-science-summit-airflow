# Lab 4. Building GCS Sensor

>Note: this is a variation of the DAG we create in Labs 2 and 3

After completion of this lab you will be able to do the following:

* Create a sensor that monitors the file on GCS

* Modify the existing DAG created in Lab 3 to load files as soon as they appear on on GCS

### Step 1. Create a GCS sensor in `gcs_plugin.py` 

1. Add the import statement for `BaseSensor` in `gcs_plugin.py` 

```
from airflow.operators.sensors import BaseSensorOperator
```

2. Add the following code to `gcs_plugin.py`

```
class GoogleCloudStorageObjectSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.
    """
    template_fields = ('bucket', 'object')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
            self,
            bucket,
            object,
            google_cloud_conn_id='google_cloud_storage_default',
            delegate_to=None,
            *args,
            **kwargs):
        super(GoogleCloudStorageObjectSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        logging.info('Sensor checks existence of : %s, %s', self.bucket, self.object)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return hook.exists(self.bucket, self.object)
```

3. Add `GoogleCloudStorageObjectSensor` to the list of operators in the `gcs_plugin.py`

```
class GcsPlugin(AirflowPlugin):
    name = "Google Cloud Storage Plugin"
    operators = [FileToGoogleCloudStorageOperator,
      GoogleCloudStorageObjectSensor]
```

4. Create a new DAG file `lab4.py`

```
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
```

5. SCP `gcs_plugin.py` file to `/opt/airflow/plugins` 

6. SCP `lab4.py` file to `/opt/airflow/dags`

7. Restart Airflow Docker containers by running the following command from under `airflow` user on the server instance

```
sudo docker-compose -f airflow-1.8.1.yml restart
```

8. Refresh Web UI until `lab4` DAG appears on the home page

9. Click `Run` on `lab4` DAG

10. Go inside the DAG on the Tree View and observe that `gcs_sensor` task keeps running waiting for `shazam_AR_20171029.txt` file to appear in the bucket

11. Click on `gcs_sensor` and go to `View Logs`. Do it a few times after 10 sec wait. Observe that the log entry is being generated for every poke for the file in the bucket.

12. Copy `shazam_AR_20171029.txt` file into the GCS bucket

13. Refresh Web UI to observe that `gcs_sensor` task stops executing

### Step 2. Assignment

Using all the knowledge you learned so far, finish the DAG to do the following:

1. Import the `shazam_AR_20171029.txt` file into a BigQuery table

2. Think how you can modify GCS sensor to check for any file in the GCS bucket. Implement the required changes.

3. Implement changes to delete file from GCS bucket after processing.

3. Research how use `TriggerDagRunOperator` to start next execution after first file is processed. Implement the required changes.

4. Think how to insert files with different timestamps into corresponding partitions in BigQuery table. Implement the required changes.



