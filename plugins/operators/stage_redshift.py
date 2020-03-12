from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json {}
        region '{}'
    """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 table="",
                 json_format="'auto'",
                 partition_by_date=False,
                 *args, **kwargs):
       """ 
            Constructor method where the parameters are initialized
            params:
                conn_id = represent the identifier of the connector to the database.
                
                aws_credentials_id = represent the identifier of the AWS connector.
                
                s3_bucket = represent the root folder of the bucket that we want to extract the data from.
                
                s3_key = represent the specifiction in case we want to partition the 
                load of data from s3 or want to get only a specific folter.
                
                region = represent the region where the s3 bucket is located. 
                
                table = represent the staging table where the data extracted from s3 will be stored.
                
                json_format = represent the format of the json that will be mapped from s3, if we use auto
                which happen to be the default it will automatically try to parse otherwise we have to provide a json parser.
                
                partition_by_date = if this parameter is set to True it will use the execution date of the dag 
                to try to access your s3 bucket by date in each iteration.
                
                
        """        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.region=region
        self.table=table
        self.json_format=json_format
        self.partition_by_date=partition_by_date

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.conn_id)
        
        rendered_key = self.s3_key.format(**context)
        if self.partition_by_date==True:
            s3_bucket_path="s3://{}/{}".format(self.s3_bucket,rendered_key)
        else:
            s3_bucket_path="s3://{}".format(self.s3_bucket)
        
        formatted_copy=StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_bucket_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format,
            self.region
        )
        redshift.run(formatted_copy)
        self.log.info('StageToRedshiftOperator Executed')





