from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            
        for check in self.dq_checks:
            
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift.get_records(sql)[0]
            error_count = 0

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

            if error_count > 0:
                self.log.info('Data Quality Test Failed')
                raise ValueError()

            if error_count == 0:
               self.log.info('Data Quality Check Passed')