import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadTableOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 truncate_insert = 0,
                 sql_statement = "",
                 *args, **kwargs):

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.truncate_insert = truncate_insert
        self.destination_table = destination_table
        self.sql_statement = sql_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_insert == 1:
            redshift.run(SqlQueries.TRUNCATE_TABLE.format(self.destination_table))
           
        redshift.run(self.sql_statement)
