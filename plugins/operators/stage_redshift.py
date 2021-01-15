import logging
import os 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import CreateQueries, SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 s3_file_location="",
                 target_table="",
                 load_only_today_json=True,
                 json_mapping_file = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_file_location = s3_file_location
        self.load_only_today_json = load_only_today_json
        self.target_table = target_table
        self.json_mapping_file = json_mapping_file

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(CreateQueries.CREATE_TABLES_SQL)
        
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        
        if self.load_only_today_json is True:
            date_tuple = context['ds'].split('-')
            full_json_path = os.path.join(self.s3_file_location, date_tuple[0], date_tuple[1], f"{context['ds']}-events.json" )
        else:
            full_json_path = self.s3_file_location
            
        if self.json_mapping_file == "":
            json_mapping_file = "auto"
        else:
            json_mapping_file = self.json_mapping_file
            
        redshift.run(SqlQueries.S3_REDSHIFT_COPY_SQL.format(self.target_table, full_json_path, credentials.access_key, credentials.secret_key, json_mapping_file))
        





