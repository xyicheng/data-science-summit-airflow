# Lab 3. Using PythonOperator to validate file downloads, cleanse the data, merge into a consolidated file and upload it to GCS.

>Note: this is a continuation of the DAG we started to create in Lab 2. 

After completion of this lab you will be able to do the following:

* Use PythonOperator as an Airflow wrapper around your custom Python functions
* Use ShortCircuitOperator, a variation of the PythonOperator to hault downstream tasks, if some condition is not met in an upstream tasks
* Pull XCom variable value from the previous task 
* Understand the _context_ and how to obtain _default variables_ from it
* Push multiple XCom variables as an output from a single task
* Set dependencies between tasks
* Test your custom Python functions from within Airflow

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
* The only way to pass a parameter from PythonOperator or ShortCircuitOperator to the Python callable function is via `op_kwargs` . As much as it is tempting to do something like `python_callable = verifyWgetReturnCode("{}").format(country)` this will not work. 
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





