from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadTableOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'email_on_retry': False,
    'depends_on_past': False,
    'max_active_runs': 1,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          schedule_interval = "@daily",
          description='Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id = "redshift",
    s3_file_location = "s3://udacityrichardf/log-data",
    load_only_today_json = True,
    target_table = "staging_events",
    json_mapping_file = "s3://udacityrichardf/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id = "redshift",
    load_only_today_json = False,
    s3_file_location = "s3://udacityrichardf/song_data",
    target_table = "staging_songs",
    dag=dag
)

load_songplays_table = LoadTableOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    truncate_insert = 0,
    destination_table = "songplays",
    sql_statement = SqlQueries.SONGPLAY_TABLE_INSERT,
    dag=dag
)

load_user_dimension_table = LoadTableOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = "redshift",
    truncate_insert = 1,
    destination_table = "users",
    sql_statement = SqlQueries.USER_TABLE_INSERT,
    dag=dag
)

load_song_dimension_table = LoadTableOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = "redshift",
    truncate_insert = 1,
    destination_table = "songs",
    sql_statement = SqlQueries.SONG_TABLE_INSERT,
    dag=dag
)

load_artist_dimension_table = LoadTableOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = "redshift",
    truncate_insert = 1,
    destination_table = "artists",
    sql_statement = SqlQueries.ARTIST_TABLE_INSERT,
    dag=dag
)

load_time_dimension_table = LoadTableOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = "redshift",
    truncate_insert = 1,
    destination_table = "time",
    sql_statement = SqlQueries.TIME_TABLE_INSERT,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id = "redshift",
    test_cases = 
    [("SELECT count(*) from songs where title is Null", 0),
     ("SELECT count(*) from songs where year > 3000", 0)],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator