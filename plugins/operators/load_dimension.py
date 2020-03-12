from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_insert="INSERT INTO {} "
    
    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql="",
                 destination_table="",
                 required_clearance=False,
                 *args, **kwargs):
       """ 
            Constructor method where the parameters are initialized.
            params:
                conn_id = represent the identifier of the connector to the database.
                
                sql = represent the query that will extract the data that will be load in the dimensional table.
                
                destination_table = represent the table where the data coming from the sql query will be stored.
                
                required_clearance = indicates if it is necessary to truncate the table before inserting the new data.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.sql=sql
        self.destination_table=destination_table
        self.required_clearance=required_clearance

    def execute(self, context):
        """ 
            This method truncate the data of the dimensional table if it is indicated 
            and load new data to it given the input sql in the constructor. 
        """        
        redshift=PostgresHook(postgres_conn_id=self.conn_id)
        if self.required_clearance:
            self.log.info("Truncate of the destination table")
            redshift.run(f"TRUNCATE {self.destination_table}")
        
        self.log.info(f"transforming and load from stage to {self.destination_table}")
        sql_insert_select_script=LoadDimensionOperator.sql_insert.format(self.destination_table)+self.sql
        
        redshift.run(sql_insert_select_script)
        
        self.log.info('LoadDimensionOperator has been executed')
