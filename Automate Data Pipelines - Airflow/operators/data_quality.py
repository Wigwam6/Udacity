from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator executing')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        ### for expected values implementaion: tables = list(context['params'].items())
        
        # Retrieve list of tables from params dictionary
        tables = context['params'].get('tables', [])
        
        if not tables:
            raise ValueError("No tables provided for data quality check")
        
        ### for implementaion of expected values loop: for table, expected_result in tables:
       
        # Checking tables if they contain values, if not, error is raised
        for table in tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
            
        ###if num_records != expected_result:
        ###    raise ValueError(f"Data quality check failed. {table} contained {num_records} rows, expected {expected_result} rows")
            