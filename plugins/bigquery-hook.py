import logging
import time
from datetime import datetime, timedelta
import uuid
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators import BaseOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from bigquery import get_client
from bigquery.errors import BigQueryTimeoutException


#silence some annoying warnings
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

#set logging level for the plugin
logging.getLogger(__name__).setLevel(logging.INFO)

class BigQueryHook(GoogleCloudBaseHook, DbApiHook):

    """
    Interact with BigQuery. This hook uses the Google Cloud Platform
    connection.

    """
    conn_name_attr = 'bigquery_conn_id'

    def __init__(self,
                 bigquery_conn_id='bigquery_default'):
        super(BigQueryHook, self).__init__(
            conn_id=bigquery_conn_id)


    def client(self):

        """
        Returns a BigQuery PEP 249 connection object.

        """
        project = self._get_field('project')
        json_key_file = self._get_field('key_path')

        logging.info('project: %s', project)
        logging.info('json_key_file: %s', json_key_file)
        return get_client(project_id=project,
                          json_key_file=json_key_file,
                          readonly=False)

    def insert(self, dataset, table, rows, id):
        inserted = self.client().push_rows(dataset, table, rows, id)

    def execute_query(self,
                      sql,
                      use_legacy_sql=False):


        job_id, _results=self.client().query(query=sql,
                                        use_legacy_sql=use_legacy_sql)

        return job_id


    def fetch(self, job_id):
        complete = False
        sec = 0
        while not complete:
            complete, row_count = self.client().check_job(job_id)
            time.sleep(1)
            sec += 1

        results = self.client().get_query_rows(job_id)

        if complete:
            logging.info("Query completed in {} sec".format(sec))
        else:
            logging.info("Query failed")

        logging.info('results: %s', results)

        return results

    def fetchone(self, job_id):

        return self.fetch(job_id)[0]


    def write_to_table(self,
                       sql,
                       destination_dataset,
                       destination_table,
                       use_legacy_sql = False,
                       write_disposition='WRITE_TRUNCATE'
                       ):

        job = self.client().write_to_table(query=sql,
                                    dataset=destination_dataset,
                                    table=destination_table,
                                    use_legacy_sql=use_legacy_sql,
                                    write_disposition=write_disposition,
                                    maximum_billing_tier=5
                                   )
        try:
            job_resource = self.client().wait_for_job(job, timeout=600)
            logging.info("Job completed: {}".format(job_resource))

        except BigQueryTimeoutException:
            logging.info("Query Timeout")
    
    def export_to_gcs(self,
                      dataset, 
                      table,
                      gcs_uri):

    

        job = self.client().export_data_to_uris( [gcs_uri],
                                   dataset,
                                   table,
                                   destination_format='NEWLINE_DELIMITED_JSON')
        try:
            job_resource = self.client().wait_for_job(job, timeout=600)
            logging.info('Export job: %s', job_resource)
        except BigQueryTimeoutException:
            logging.info('Timeout occured while exporting table %s.%s to %s',
                dataset,
                table,
                gcs_uri)

    def delete_table(self, dataset, table):
        deleted = self.client().delete_table(dataset, table)