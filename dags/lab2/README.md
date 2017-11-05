# Lab 2. Using BashOperator and WGET to download files

After completion of this lab you will be able to do the following:

* Use BashOperator to run UNIX shell commands from within Airflow
* Configure global variables to pass environment context to the BashOperator
* Return value from BashOperator as an XCom variable
* Pass execution date to BashOperator for downloading file for current date
* Do a DAG backfill on a particular execution date
* Create DAG dynamically by looping through list of files that need to be downloaded and creating a task for each file
* Getting inside docker container to check for downloaded files

This lab will be done progressively from most simple BashOperator functionality, to increasingly more complex. Each time the changes are done in your DAG file, you should SCP it to your Airflow server `/opt/airflow/dags` folder.

### Step 1. Create DAG

1. Open your preferred Python editor and create a new DAG. Give it id: `lab2`.

```
from __future__ import print_function
import airflow
import pytz
import logging
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
  task_id = 'download_file',
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

### Step 2. Passing download URL as a parameter from a global variable

1. From Airflow Web UI, to to `Admin -> Variables`

2. Create a variable `shazam_files_url` and assing the value of `https://github.com/umg/data-science-summit-airflow/blob/master/data/shazam`

3. Make the following change in your DAG:

```
download_file = BashOperator(
  task_id = 'download_file',
  bash_command = 'wget $URL/shazam_AR_20171029.txt -O /tmp/shazam_AR_20171029.txt',
  env={'URL': '{{ var.value.shazam_files_url }}'},
  dag = dag
)
```

4. Stop for a moment and reflect on what we just have done. There are multiple very important concepts at play here:

* We created a global variable `shazam_file_url`
* We used _templated parameter_ `env` of `BashOperator` to create an _environment variable_ `URL` and assigned  it's value from the _template notation_ `{{ var.value.<name of the variable> }}`
* We passed value of the environment variable `URL` into our Bash shell command using regular UNIX notation for environment variables as `$<environment variable name>`

5. SCP the changed DAG to the `/opt/airflow/dags` folder on the server. From DAG view in Web UI go to `Code` to make sure your changes have been picked up by Docker. 

6. Run the DAG and check the task log to make sure it's still executing properly.

> Note: There are two ways you can check the execution of the DAG: 
> 1. Click `Run` from Web UI home page to create a new execution of the DAG
> 2. Click on `download_file` task of the existing execution and click `Clear`. Then refresh UI a few times to see how the task is being executed again. 

> The second approach is preferrable so the Tree View in DAG UI doesn't get cluttered with too many DAG executions. 

7. Click on `download_file` task box and go to `Rendered`. Check the value of rendered template.

### Step 3. Returning Bash command output as an XCom variable

1. Modify the `download_file` task to add the `xcom_push = True` parameter:

```
download_file = BashOperator(
  task_id = 'download_file',
  bash_command = 'wget $URL/shazam_AR_20171029.txt -O /tmp/shazam_AR_20171029.txt; echo $?',
  env={'URL': '{{ var.value.shazam_files_url }}'},
  xcom_push = True, 
  dag = dag
)
```

2. Reflect on the modifications we have just done:
* We added `xcom_push` parameter to return output from the BashOperator command as an XCom variable.
* We added `echo $?` command at the end of `bash_command` parameter, to return WGET's exit code. This code later can be parsed by a downstream task. 

>Note: The WGET exit codes for your reference:

>0. No problems occurred
>1. Generic error code
>2. Parse error — for instance, when parsing command-line options, the .wgetrc or .netrc…
>3. File I/O error
>4. Network failure
>5. SSL verification failure
>6. Username/password authentication failure
>7. Protocol errors
>8. Server issued an error response

3. SCP the changed DAG to the server

4. Clear the `download_file` task and refresh UI for the task to be executed again. 

5. Click on `download_file` task box in Tree View and go to `View Log -> XCom`. Check that return value is `0`. 


### Step 4. Passing execution date to dynamically generate the name of the file

1. Modify the `download_file` task to the following:

```
download_file = BashOperator(
  task_id = 'download_file',
  bash_command = 'wget $URL/shazam_AR_$EXEC_DATE.txt -O /tmp/shazam_AR_$EXEC_DATE.txt; echo $?',
  env={'URL': '{{ var.value.shazam_files_url }}',
    'EXEC_DATE': '{{ ds_nodash }}'},
  xcom_push = True, 
  dag = dag
)
```

2. Reflect on the change we have just made:

* We added another environment variable `EXEC_DATE` to the templated parameter `env`.
* We passed the value of _default variable_ `ds_nodash` via template notation `{{ default variable }}` to the environment variable `EXEC_DATE`, which represents the execution date of the DAG in `YYYYMMDD` format.
* We used `$<variable name>` UNIX notation to obtain the value of the _environment variable_ to the Bash command at the run time. 

> Note: the full list of _default variables_ can be obtained [here](https://airflow.apache.org/code.html#default-variables)

3. SCP the changed DAG to the server

4. Since our files are from `20171029`, we need to execute our DAG on that date so the files get downloaded successfully. For this we need to do `backfill` command in Airflow server.

5. From SSH session on Airflow server do the following:

```
$ sudo docker ps | grep worker
```
It should return an output similar to this:
```
337c390e2863        sstumgdocker/docker-airflow-mongotools   "/entrypoint.sh wo..."   7 days ago          Up 6 days           5555/tcp, 8080/tcp, 8793/tcp                 airflow_worker_1
```
The first value is a container id of the worker

6. On the Airflow server, launch the following command to get inside the worker's Docker container
```
$ sudo docker exec -it <container id> bash
```

7. Launch the following command inside the worker's Docker container:

```
$ airflow backfill 'lab2' -s '2017-10-29' -e '2017-10-29'
```

>Note: the parameters of the `backfill` command are:

```
$ airflow backfill '<dag_id>' -s '<start date>' -e '<end date>'
```

> Backfill can be done for multiple dates at once, but we need it just for one day, so the `start date` and `end date` parameters are equal. 

8. Go back to Tree View in the UI and notice a new DAG execution being added. Refresh UI a few times until DAG execution completes. 

9. Click on `download_file` task box and go to `Rendered`. Check that all the templated values got rendered correctly. It should look like this:
```
{'URL': u'https://github.com/umg/data-science-summit-airflow/blob/master/data/shazam', 'EXEC_DATE': u'20171029'}
```

>Note: Now if you will try to restart the DAG executions that we did previously which don't fall on 2017-10-29, they will not fail, despite there are no files under the URL that match their execution dates. But the WGET exit code will be `8 - Server issued an error response`. You can try to restart these DAG executions, click on the `download_file` task box and go to `View Log -> XCom` to verify this. 

### Step 5. Generating DAG dynamically for downloading multiple files

1. Go to `Admin -> Variables` in Web UI and create a global variable with name `shazam_country_list` and value of `AR,US,CA`. Don't add any spaces between commas.

2. Add the following line in the import statements in DAG code:
```
from airflow.models import Variable
```

3. Modify the DAG code as the following:

```
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

```

4. Reflect on the changes we just have made:

* We used `Variable.get` method to obtain the CSV value of the `shazam_country_list` global variable and created a Python list from it by using `split` method. 
* We added a loop on this country list so the `download_file` task gets repeated as many times as there are values in the list. 
* We added postfix of country code value for the `task_id` parameter of the `download_file` task. This is very important so every task gets it's own unique id. 
* We added the country code to the source and destination file names in WGET command. We used `format` method in Python to substitute `{}` placeholders with values of country codes. 

5. SCP the changed DAG to the server.

6. Refresh Tree View in UI a few times until you see three tasks in the DAG and not one. They should be named as `download_file_AR`, `download_file_US` and `download_file_CA`. 

7. Go inside worker's Docker container and do backfill for the DAG. (See Step 4. 4-7) on `20171029`.

8. Refresh Tree View in UI a few times so all three tasks get executed. 

9. Do `$ ls /tmp` inside the worker's Docker container to make sure that all three files got downloaded successfully.

10. You may also check logs and rendered templates. 

>Note: This is a very powerfull technique in Airflow that puts it above competitors. Once one pipeline gets created, it may be repeated for multiple files or inputs dynamically, and be treated as it's own unique branch in DAG with perfect visibility of it's execution. 


