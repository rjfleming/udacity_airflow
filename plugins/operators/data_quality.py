import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 test_cases = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for testcase in self.test_cases:
            result = redshift.get_records(testcase[0])
            if (result is None) or (result[0][0] != testcase[1]):
                raise ValueError("Number of rows found does not match expected result")
        
        