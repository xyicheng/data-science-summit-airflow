import logging
import time
from datetime import datetime, timedelta
import uuid
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators import BaseOperator
from bq_hook import BigQueryHook


class BqWriteToTableOperator(BaseOperator):
    """

    Creates a daily partition in BigQuery table,
    based on provided execution time and SQL.
    With option to create a shard instead of partition

    """
    ui_color = '#33F3FF'
    template_fields = ('sql',
                       'destination_table',
                       'partition')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_table,
                 partition = None,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 shard = False,
                 write_disposition = 'WRITE_TRUNCATE',
                 *args, **kwargs):
        self.sql = sql
        self.destination_table = destination_table
        self.partition = partition
        self.bigquery_conn_id=bigquery_conn_id
        self.use_legacy_sql=use_legacy_sql
        self.shard = shard
        self.write_disposition = write_disposition
        super(BqWriteToTableOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info('Writing data to %s from SQL: %s',
                     self.destination_table,
                     self.sql)

        #prepare parameters for passing to the BQ hook for execution

        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        table_name = dst_table_array[len(dst_table_array) - 1]
        dataset_name = dst_table_array[len(dst_table_array) - 2]
        #logging.info('partition: %s', partition)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        hook.write_to_table(sql = self.sql,
                            destination_dataset = dataset_name,
                            destination_table = '{}{}{}'.format(table_name,
                                                                '_' if self.shard else '$',
                                                                self.partition.replace('-', '')) if self.partition else table_name,
                            write_disposition=self.write_disposition)

class GcsToBqOperator(BaseOperator):

    ui_color = '#88F8FF'
    template_fields = ('gcs_uris',
        'destination_table')

    @apply_defaults
    def __init__(
        self,
        gcs_uris,
        destination_table,
        schema,
        source_format = 'CSV',
        field_delimiter = ',',
        bigquery_conn_id = 'bigquery_default',
        write_disposition = 'WRITE_TRUNCATE',
        *args, **kwargs):

        self.gcs_uris = gcs_uris
        self.destination_table = destination_table
        self.schema = schema
        self.source_format = source_format
        self.field_delimiter = field_delimiter
        self.bigquery_conn_id = bigquery_conn_id
        self.write_disposition = write_disposition
        super(GcsToBqOperator, self).__init__(*args, **kwargs)


    def execute(self, context):
        logging.info('Importing data from %s to %s',
                     self.gcs_uris,
                     self.destination_table)

        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        table_name = dst_table_array[len(dst_table_array) - 1]
        dataset_name = dst_table_array[len(dst_table_array) - 2]

        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        hook.import_from_gcs( 
                        gcs_uris = self.gcs_uris,
                        dataset = dataset_name,
                        table = table_name,
                        schema = self.schema,
                        source_format = self.source_format,
                        field_delimiter = self.field_delimiter,
                        write_disposition = self.write_disposition)
    

    
    

class BqPlugin(AirflowPlugin):
    name = "BigQuery Plugin"
    operators = [BqWriteToTableOperator,
                 GcsToBqOperator]
