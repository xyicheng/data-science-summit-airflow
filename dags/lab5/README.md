# Lab 5. Exploring incremetal update pattern

### Step 1. Adding `BqIncrementalLoadDataOperator` to `bq_plugin.py`

1. Add `BqIncrementalLoadDataOperator` to the `bq_plugin.py`

```
class BqIncrementalLoadDataOperator(BaseOperator):
    """
    Incrementally loads data from one table to another, repartitioning if necessary

    """
    ui_color = '#33FFEC'
    template_fields = ('sql',
                       'partition_list_sql',
                       'source_table',
                       'destination_table',
                       'last_update_value',
                       'execution_time'
    )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 partition_list_sql,
                 source_table,
                 destination_table,
                 source_partition_column,
                 destination_partition_column,
                 last_update_column, 
                 last_update_value,
                 execution_time,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 *args, 
                 **kwargs):
        self.sql = sql
        self.partition_list_sql = partition_list_sql,
        self.source_table = source_table
        self.destination_table = destination_table
        self.source_partition_column = source_partition_column
        self.destination_partition_column = destination_partition_column
        self.last_update_column = last_update_column
        self.last_update_value = last_update_value
        self.execution_time = execution_time
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql
        super(BqIncrementalLoadDataOperator, self).__init__(*args, **kwargs)
    

    def execute(self, context):
        
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        dst_table = dst_table_array[len(dst_table_array) - 1]
        dst_dataset = dst_table_array[len(dst_table_array) - 2]

        # check if we need to back-off failed load 
        # we do this if we find any value with higher last update value 
        # than the one passed as a parameter

        
        backoff_partitions_sql = """
            select distinct {} 
            from `{}` 
            where {} > timestamp('{}')
            and load_datetime <= timestamp_add(timestamp('{}'), interval 24 hour)
            """.format(
                self.destination_partition_column,
                self.destination_table,
                self.last_update_column,
                self.last_update_value,
                self.execution_time)

        logging.info('Checking for partitions to back off')
        logging.info('Executing SQL: ' + backoff_partitions_sql)
        job_id = hook.execute_query(backoff_partitions_sql, use_legacy_sql=False)
        backoff_partitions =  hook.fetch(job_id)

        if len(backoff_partitions) > 0:
            logging.info('Backing off previously loaded partitions')
            for partition in backoff_partitions:
                #move partition into a temp table so we could do DML on it
                temp_table_name = 'temp_{}_{}'.format(
                        dst_table,
                        partition[self.destination_partition_column].replace('-', '')
                )

                create_temp_table_sql = """
                select *
                from `{}`
                where _partitiontime = timestamp('{}')
                """.format(
                    self.destination_table,
                    partition[self.destination_partition_column]
                )

                logging.info('Copying partition to an unpartitioned temp table')
                logging.info('Executing SQL: ' + create_temp_table_sql)

                hook.write_to_table(sql = create_temp_table_sql,
                    destination_dataset = dst_dataset,
                    destination_table = temp_table_name,
                    write_disposition='WRITE_TRUNCATE'
                )

                # delete all the records with timestamp later than last loaded
                delete_records_sql = """
                delete from `{}.{}` 
                where load_datetime > timestamp('{}')
                and load_datetime <= timestamp_add(timestamp('{}'), interval 24 hour)
                """.format(
                    dst_dataset,
                    temp_table_name,
                    self.last_update_value,
                    self.execution_time
                )

                logging.info('Deleting all the records with timestamp later than last loaded')
                logging.info('Executing SQL: ' + delete_records_sql)

                job_id = hook.execute_query(delete_records_sql, use_legacy_sql=False)
                result =  hook.fetch(job_id)

                #reinsert updated partition into main table
                replace_partition_sql = """
                select * from `{}.{}` 
                """.format(
                    dst_dataset,
                    temp_table_name
                )
                logging.info('Reinsert updated partition into main table')
                logging.info('Executing SQL: ' + replace_partition_sql)

                hook.write_to_table(sql = replace_partition_sql,
                    destination_dataset = dst_dataset,
                    destination_table = '{}${}'.format(
                                dst_table,
                                partition[self.destination_partition_column].replace('-', '')),
                    write_disposition='WRITE_TRUNCATE'
                )

                #delete temp table
                logging.info('Deleting temp table')
                hook.delete_table(dst_dataset, temp_table_name)

        else:
            logging.info('No need to back off')
            
        #get list of partitions to load
        logging.info('Getting list of partitions to load')
        job_id = hook.execute_query(self.partition_list_sql, use_legacy_sql=False)
        partition_list =  hook.fetch(job_id)

        # load partitions
        logging.info('Loading partitions')
        partition_array = []
        for partition in partition_list:
            load_partition_sql = self.sql.replace(
                '#{}#'.format(self.destination_partition_column), 
                partition[self.destination_partition_column])
            logging.info('Executing SQL: ' + load_partition_sql)
            hook.write_to_table(sql = load_partition_sql,
                        destination_dataset = dst_dataset,
                        destination_table = '{}${}'.format(
                            dst_table,
                            partition[self.destination_partition_column].replace('-', '')),
                        write_disposition='WRITE_APPEND')

            partition_array.append(partition[self.destination_partition_column])

        return partition_array    
```

2. Go through the code to understand every stage of the functionality of the operator.

3. Add `BqLastUpdateOperator`  to the `bq_plugin.py`

```
class BqLastUpdateOperator(BaseOperator):
    """

    Gets last loaded timestamp from a BigQuery table

    """
    ui_color = '#b4e6f0'
    template_fields = ('dataset_table',
                       'last_execution_time')

    @apply_defaults
    def __init__(self,
                 dataset_table,
                 timestamp_field='load_datetime',
                 last_execution_time=None,
                 bigquery_conn_id='bigquery_default',
                 *args, **kwargs):
        self.dataset_table = dataset_table
        self.timestamp_field = timestamp_field
        self.last_execution_time = last_execution_time
        self.bigquery_conn_id = bigquery_conn_id
        super(BqLastUpdateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        self.sql = 'select max({}) as {} from `{}`'.format(
            self.timestamp_field,
            self.timestamp_field,
            self.dataset_table
        )

        if self.last_execution_time:
            self.sql += " where _partitiontime >= timestamp('{}')".format(
                self.last_execution_time
            )

        logging.info('Executing SQL: %s', self.sql)

        job_id = hook.execute_query(self.sql, use_legacy_sql=False)
        result =  hook.fetchone(job_id)

        if result[self.timestamp_field]:
            timestamp = datetime.utcfromtimestamp(result[self.timestamp_field]).strftime('%Y-%m-%d %H:%M:%S')
        else:
            timestamp = '1970-01-01'

        logging.info('Last Update Timestamp: %s', timestamp)

        return timestamp
```

4. Add the new operators to the `BqPlugin` class in the `bq_plugin.py`

```
class BqPlugin(AirflowPlugin):
    name = "BigQuery Plugin"
    operators = [BqWriteToTableOperator,
                 GcsToBqOperator,
                 BqIncrementalLoadDataOperator,
                 BqLastUpdateOperator]
```

5. SCP `bq_plugin.py` to `/opt/airflow/plugins` on the server

6. Restart Docker containers

