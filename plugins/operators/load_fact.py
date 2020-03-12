from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_insert="INSERT INTO {} "

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql="",
                 destination_table="",
                 *args, **kwargs):
       """ 
            Constructor method where the parameters are initialized
            params:
                conn_id = represent the identifier of the connector to the database.
                
                sql = represent the query that will extract the data that will be load in the fact table.
                
                destination_table = represent the table where the data coming from the sql query will be stored.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.sql=sql
        self.destination_table=destination_table


    def execute(self, context):
        """ 
            This method append new data to the fact tables given the input sql in the constructor. 
        """                
        redshift=PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info(f"transforming and load from stage to {self.destination_table}")
        sql_insert_select_script=LoadFactOperator.sql_insert.format(self.destination_table)+self.sql
        
        redshift.run(sql_insert_select_script)
        
        self.log.info('LoadFactOperator has been executed')
