from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_checks = [],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for test_obj in self.dq_checks:
            self.log.info(f"Perfroming Test on Query: {test_obj['check_sql']}")
            records = redshift_hook.get_records(test_obj['check_sql'])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {test_obj['check_sql']}: returned no results")
            num_records = records[0][0]
            if eval(test_obj['test_expr'].format(num_records)):
                raise ValueError(f"Data quality check failed. on statement: {test_obj['test_expr']} with num_records: {num_records}")
                
        self.log.info(f"Data quality Tests Passed")
