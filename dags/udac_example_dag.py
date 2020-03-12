from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                PythonOperator)
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'Catchup':False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )
    

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend/log_data",
    s3_key="{execution_date.year}/{execution_date.month}/{ds}-events.json",
    partition_by_date=True,
    table="staging_events",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend/song-data",
    partition_by_date=False,
    table="staging_songs",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    destination_table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    sql=SqlQueries.user_table_insert,
    required_clearance=True,
    destination_table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    sql=SqlQueries.song_table_insert,
    required_clearance=True,
    destination_table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    required_clearance=True,
    destination_table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    required_clearance=True,
    destination_table="time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift",
    sql_test_cases={ 
        SqlQueries.hasrow_check.format("staging_events"):True,
        SqlQueries.hasrow_check.format("staging_songs"):True,
        SqlQueries.hasrow_check.format("songs"):True,
        SqlQueries.hasrow_check.format("artists"):True,
        SqlQueries.column_notnull_check.format("songs","title"):True,
        SqlQueries.column_notnull_check.format("artists","name"):True
    }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]
[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator