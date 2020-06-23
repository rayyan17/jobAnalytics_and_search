from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}' 
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 s3_bucket="",
                 s3_key="",
                 format_type="",
                 *args, **kwargs):

        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format_type = format_type
        
    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from Redshift table")
        redshift.run("TRUNCATE {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = CopyToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.format_type
        )

        redshift.run(formatted_sql)
