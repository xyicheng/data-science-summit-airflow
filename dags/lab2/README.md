# Lab 2. Using BashOperator and WGET to download files

After completion of this lab you will be able to do the following:

* Use BashOperator to run UNIX shell commands from within Airflow
* Configure global variables to pass environment context to the BashOperator
* Pass execution date to BashOperator for downloading file for current date
* Return value from BashOperator as an XCom variable
* Create DAG dynamically by looping through list of files that need to be downloaded and creating a task for each file

This lab will be done progressively from most simple BashOperator functionality, to increasingly more complex. Each time the changes are done in your DAG file, you should SCP it to your Airflow server `/opt/airflow/dags` folder.

### Step 1. Create DAG

1. Open your preferred Python editor and create a new DAG. Give it id: `lab2`.

```
from __future__ import print_function
import airflow
import pytz
from datetime import datetime, timedelta
from airflow import DAG

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
```

2. Import `BashOperator` into the DAG. Add the following line:
```
...
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
...

```