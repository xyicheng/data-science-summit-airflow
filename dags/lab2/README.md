# Lab 2. Using BashOperator and WGET to download files

After completion of this lab you will be able to do the following:

* Use BashOperator to run UNIX shell commands from within Airflow
* Configure global variables to pass environment context to the BashOperator
* Pass execution date to BashOperator for downloading file for current date
* Return value from BashOperator as an XCom variable
* Create DAG dynamically by looping through list of files that need to be downloaded and creating a task for each file
* Getting inside docker container to check for downloaded files

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

2. Import `BashOperator` into the DAG at the top of the file:

```
...
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
...

```

3. Create `download_file` task at the bottom of the file:

```
download_file = BashOperator(
  task_id = 'dowload_file',
  bash_command = 'wget https://github.com/umg/data-science-summit-airflow/blob/master/data/shazam/shazam_AR_20171029.txt -O /tmp/shazam_AR_20171029.txt',
  dag = dag
)
```
4. SCP DAG file `lab2.py` to `opt/airflow/dags` folder on the server and refresh Web UI home page a few time until the DAG appears in the list. 

5. Click Run (looks like Play icon) and click on the DAG name to go inside. Refresh a few times until the `download_file` task appears dark green if it succeeds or red if it fails. 

6. Click on the task box and go to `Log`. You should see the following line:
```
HTTP request sent, awaiting response... 200 OK
```

7. From your SSH session on the Airflow server run the following command:
```
$ sudo docker ps | grep worker
```
It should return an output similar to this:
```
337c390e2863        sstumgdocker/docker-airflow-mongotools   "/entrypoint.sh wo..."   7 days ago          Up 6 days           5555/tcp, 8080/tcp, 8793/tcp                 airflow_worker_1
```
The first value is a container id of the worker

8. On the Airflow server, launch the following command to get inside the worker's Docker container
```
$ sudo docker exec -it <container id> bash
```

9. List the content of the `/tmp` folder to look for downloaded file:

```
$ ls /tmp
```

10. Exit from worker's Docker container
```
$ exit
```





