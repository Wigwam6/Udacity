from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Custom operator to stage data from S3 to Redshift.

    :param aws_credentials_id: Airflow connection ID for AWS credentials
    :param table: Target Redshift table
    :param s3_bucket: Source S3 bucket
    :param s3_key: Source S3 key (supports templating)
    :param redshift_conn_id: Airflow connection ID for Redshift
    :param log_json_file: JSON format file path in S3 (default: 'auto')
    """
    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 redshift_conn_id="",
                 log_json_file="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.log_json_file = log_json_file

        # Debugging statements
        self.log.info(f"aws_credentials_id: {aws_credentials_id}")
        self.log.info(f"table: {table}")
        self.log.info(f"s3_bucket: {s3_bucket}")
        self.log.info(f"s3_key: {s3_key}")
        self.log.info(f"redshift_conn_id: {redshift_conn_id}")
        self.log.info(f"log_json_file: {log_json_file}")
        self.log.info(f"args: {args}")
        self.log.info(f"kwargs: {kwargs}")

    def execute(self, context):
        self.log.info('Establishing connection using AwsHook')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Uncomment the lines below to clear the staging table before loading data
        # self.log.info("Clearing data from staging Redshift table")
        # redshift.run("DELETE FROM {}".format(self.table))

        # Render the S3 key with the execution date
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        if self.log_json_file and self.log_json_file != 'auto':
            log_json_path = f"s3://{self.s3_bucket}/{self.log_json_file}"
            self.log.info(f"Copying data from {s3_path} to Redshift table {self.table} using JSON log file {log_json_path}")
            sql_query = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                log_json_path
            )
        else:
            self.log.info(f"Copying data from {s3_path} to Redshift table {self.table} using 'auto' JSON format")
            sql_query = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                'auto'
            )

        try:
            self.log.info(f"Executing COPY command: {sql_query}")
            redshift.run(sql_query)
            self.log.info(f"Successfully loaded data into staging table {self.table}")
        except Exception as e:
            self.log.error(f"Error loading data into staging table {self.table}: {str(e)}")
            raise e





