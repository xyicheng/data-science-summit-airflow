# Lab 3. Using PythonOperator to validate file downloads, cleanse the data, merge into a consolidated file and upload it to GCS.

>Note: this is a continuation of the DAG we started to create in Lab 2. 

After completion of this lab you will be able to do the following:

* Use PythonOperator as an Airflow wrapper around your custom Python functions
* Use ShortCircuitOperator, a variation of the PythonOperator to hault downstream tasks, if some condition is not met in an upstream tasks
* Pull XCom variable value from the previous task 
* Understand the _context_ and how to obtain _default variables_ from it
* Set dependencies between tasks
* Test your custom Python functions from within Airflow
* Use `FileToGoogleCloudStorageOperator` to upload file to GCS
* "Hack" community contributed operators to make them work for your use cases

### Step 1. Validate WGET return codes

1. Create a new file called `lab3.py` in you prefered Python IDE. 

2. Copy the content of the `lab2.py` that we created during previous lab exercise, but rename dag id to `lab3` and add new description:

```
dag = DAG('lab3',
          description = 'Using PythonOperator for file processing',
```

3. Backfill the DAG on `2017-10-29`, using same procedure as in the previous lab.

4. Verify that execution is successful, by checking downloaded files on the worker, logs and WGET return codes.

5. Add import statement for PythonOperator and ShortCircuitOperator at the top of the file: 

```
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
```
6. Add `verify_download` task  to the `lab3.py` DAG file inside the country list loop:

```
verify_download = ShortCircuitOperator(
    task_id='verify_download_{}'.format(
      country
    ),
    python_callable=verifyWgetReturnCode,
    provide_context=True,
    op_kwargs=[('country', country)]
    dag=dag
)
    
```
7. Reflect on the changes we just have made:
* We created an instance of ShortCircuitOperator task with dynamic id inside the country list loop
* We provided name of custom Python function we'll be building in a moment via `python_callable` parameter
* We set `provide_context` parameter to `True` which allows passing default variables and custom parameters via `kwargs` into the body of the custom Python function. This will allow us to collect XCom variables from the upstream `download_file` task.
* We set custom parameter `country` via `op_kwargs` parameter that will pass the country code through `kwargs` to the custom Python function

8. Add a Python function at the top of the DAG file and outside of the country list loop, which verifies WGET return codes from the previous task. Give it same name, as we specified in `python_callable` parameter of the ShortCircuitOperator

```
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
    if(wget_code == 0):
        return True
    else:
        return False
```

9. Reflect on the logic of our Python function, especially on how we use `kwargs` to exchange information between task instances and the Python function. 

Here are a few points to pay attention to:
* The only way to pass a parameter from PythonOperator or ShortCircuitOperator to the Python callable function is via `op_kwargs` . As much as it is tempting to do something like `python_callable = verifyWgetReturnCode(country)` this will not work. 
* To pull XCom variable value from the previous (ustream) task, you need to instantiate an instane of _current_ task. This current task instance comes from `ti` _default variable_, which becomes available in `kwargs`, only if parameter `provide_context` is set to `True`. 

10. Set dependency between `download_file` and `verify_download` tasks using Python bitshift operator. This should be set inside the country list loop after the both tasks are declared:

```
download_file >> verify_download
```

>Note: using bitshift notation for setting task dependencies is the same as doing `download_file.set_downstream(verify_download)` or `verify_download.set_upstream(download_file)`

11. Test your code by running the `airflow test` command from inside of worker's Docker container:

```
$ airflow test 'lab3' 'verify_download' '2017-10-29'
```

>Note: Because our code deals with XCom variables, in order for the `airflow test` command to work, the upstream tasks need to be previously executed with `airflow backfill` to have a state. Otherwise our code that pulls XCom variables from upstream tasks will be failing, because the upstream tasks will have nothing returned and will not have a state in Airflow database. 

If everything is correct, the output should look similar to this:
```
[2017-11-05 15:24:14,534] {models.py:1352} INFO - Executing <Task(ShortCircuitOperator): verify_download_US> on 2017-10-29 00:00:00
[2017-11-05 15:24:14,553] {lab3.py:15} INFO - Verifying WGET return code for country: US
[2017-11-05 15:24:14,563] {lab3.py:29} INFO - WGET return code from task download_file_US is 0
[2017-11-05 15:24:14,564] {python_operator.py:84} INFO - Done. Returned value was: True
[2017-11-05 15:24:14,564] {python_operator.py:161} INFO - Condition result is True
[2017-11-05 15:24:14,564] {python_operator.py:164} INFO - Proceeding with downstream tasks...
```

12. Run `airflow backfill` inside worker's Docker container on `2017-10-29`. 

>Note: if you clear the previously ran `download_file` tasks they will run again after you do backfill. But if you leave them completed, then the backfill will just run the newly added tasks. This is convenient for progressive development. 

>Note: This sequence of doing `airflow test` first to debug your code and then run `airflow backfill` to execute the just developed tasks and fill the Airflow database with state, is very common in the Airflow development cycle. 

13. Check the success of DAG execution, explore task logs etc. 

### Step 2. Data transformations with Pandas 

1. Add import statement for Pandas package to the DAG file:
```
import pandas as pd
```

2. Add new `transform` PythonOperator task in the DAG file:

```
transform = PythonOperator(
    task_id = 'transform_data_{}'.format(
      country
    ),
    python_callable = transformData,
    provide_context = True,
    op_kwargs = [('country', country)],
    dag=dag
  )
```

The settings are very similar to the ShortCircuitOperator task we did before. This task will be calling `transformData` Python function.

3. Create `transformData` Python function that uses Pandas dataframes to transform content of the file to a format that can be easily imported by BigQuery:

```
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

```

>Note: Please note the other way custom parameters and default variables can be passed from the context into a Python function. In `verifyWgetReturnCode` function, we obtained value of the `country` parameter from `kwargs`, but here we passed `country` and `ds_nodash` explicitely as arguments of the Python function. `country` is a custom parameter that we pass via `op_kwargs` parameter, but `ds_nodash` is a default variable so it doesn't need to be explicitely stated in the operator, because it gets passed with the context. Any default variables can be passed in this fashion as long as  `provide_context` is set to `True`.

4. SCP changed file and backfill the DAG on `2019-10-29`

5. Check /tmp/shazam_combined_20171029.txt file to be properly transformed and having records from all 3 files

### Step 3. Using `FileToGoogleCloudStorageOperator`to copy the generated combined file to GCS

In this step we are going to try using community-contributed `FileToGoogleCloudStorageOperator` to move combined daily Shazam file to GCS before to facilitate import to BigQuery. 

1. Create a bucket in GCS in your project, for example: `gs://<your project name>`

2. Create a global variable in Web UI `Admin -> Variables` with the name `project_bucket` and value `<your project name>`. No `gs://` prefix needed

3. Add an import statement to the top of the DAG file:
```
from airflow.contrib.operators import file_to_gcs
```

4. Add `upload_to_gcs` task to your DAG, before the country loop

```
 upload_to_gcs = file_to_gcs.FileToGoogleCloudStorageOperator(
    task_id = 'upload_to_gcs',
    dst = 'airflow-training/shazam/shazam_combined_{{ ds_nodash }}.txt',
    bucket = '{{ var.value.project_bucket }}',
    conn_id = 'google_cloud_default',
    src = '/tmp/shazam_combined_{{ ds_nodash }}.txt',
    dag = dag
)
```

> Note: the reason we are placing this task in front of the country list loop, is because we want to connect all the specific country branches into a single `upload_to_gcs` task. 

5. In the Web UI, go to `Admin -> Connections` and create a `google_cloud_storage_default` connection with following properties:
* `Conn Id`: `google_cloud_storage_default`
* `Conn Type`: `Google Cloud Default`
* `Project Id`: `<your project name>`
* `Keyfile Path`: `/opt/app/<your project name>-key.json`
* `Scopes`: `https://www.googleapis.com/auth/devstorage.read_write`

6. Add dependency to `upload_to_gcs` task within the country list loop:

```
  download_file >> verify_download >> transform >> upload_to_gcs
```
7. Run the `upload_to_gcs` task from Web UI and observe it fail. If everything has been done correctly up to this point, the failure would be due to this error:

```
IOError: [Errno 2] No such file or directory: '/tmp/shazam_combined_{{ ds_nodash }}.txt'
```

>Note: this has been set on purpose, to demonstrate the common frustration of dealing with community-contributed operators. The reason for this error is that `src` parameter of the `FileToGoogleCloudStorageOperator` is not templated. So how do we pass execution date to generate the name of the file dynamically? 

>There are couple of ways you can go from there:

>a) Keep trying to find a workaround with existing contributed operator (might take days, if not weeks)

>b) Fork the operator repository, and modify it to make it work. Do a pull request to contribute it back to the community

>c) Create a custom plugin and "steal" most of the contrib operator's code and maintain it within your own repo

>We will focus on the option c) in the next step

### Step 4. Stealing `FileToGoogleCloudStorageOperator` into your custom plugin and hacking it to make it work

1. Go to log and find location of the `FileToGoogleCloudStorageOperator` file. In our case it should be `/sstumg-incubator-airflow/airflow/contrib/operators/file_to_gcs.py` inside the Docker container. 

2. Copy the file to `/opt/app` directory sould we could grab it from the instance. If you have permissions issues to copy file into `opt/app`, exit the worker's bash into the instance SSH session and do `$ sudo chmod 777 /opt/app` command.

3. Create a new file `gcs_plugin.py` in your dev environment. Copy/paste there the code from `file_to_gcs.py`

4. Add the import statement for `AirflowPlugin`

```
from airflow.plugins_manager import AirflowPlugin
```

5. Add the `GcsPlugin` class statement at the end of the plugin file

```
class GcsPlugin(AirflowPlugin):
    name = "Google Cloud Storage Plugin"
    operators = [FileToGoogleCloudStorageOperator]
```

6. Add the following code in the `FileToGoogleCloudStorageOperator` class, right after the class declaration:

```
class FileToGoogleCloudStorageOperator(BaseOperator):
    """
    Uploads a file to Google Cloud Storage
    """

    template_fields = ('src', 'dst', 'bucket')
```

This turns the fields in the list to templated fields

7. In the DAG file replace import statement 
`from airflow.contrib.operators import file_to_gcs` with `from airflow.operators import FileToGoogleCloudStorageOperator`

8. Also in the DAG file remove `file_to_gcs` from `file_to_gcs.FileToGoogleCloudStorageOperator` leaving only `FileToGoogleCloudStorageOperator` in the operator declaration

9. SCP `gcs_plugin.py` file to `/opt/app/plugins` folder on the server's instance

10. Restart Airflow Docker containers by running the following command from under `airflow` user on the server instance
```
sudo docker-compose -f airflow-1.8.1.yml restart
```

11. SCP the DAG file to the server and refresh Web UI

12. Run the `upload_to_gcs` task from the Web UI

13. Check your GCS bucket for the uploaded file




