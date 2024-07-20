from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 sql_query="",
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Loading data from staging to fact table: {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        try:
            redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
            self.log.info(f"Successfully loaded data into fact table: {self.table}")
        except Exception as e:
            self.log.error(f"Error loading data into fact table: {self.table}")
            raise 
