from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 sql_query="",
                 table="",
                 mode="append",
                 redshift_conn_id = "",
                 # parameters=None, uncomment for parametrized sql queries
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_query = sql_query
        self.table = table
        self.mode = mode
        self.redshift_conn_id = redshift_conn_id
        # self.parameters = parameters if parameters is not None else {}

    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading from staging to dimension table")

        # There are only 2 valid modes
        # if mode = "truncate-insert", first delete, then load, default is append/insert only
        if self.mode not in ["append", "truncate-insert"]:
            raise ValueError("Invalid mode. Use 'append' or 'truncate-insert'.")
        if self.mode == "truncate-insert":
            self.log.info("Truncating dimension table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("Inserting data into dimension table")
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
        # redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query), self.parameters) Delete above run to use this !!!
