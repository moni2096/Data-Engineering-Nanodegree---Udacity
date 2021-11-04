from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,  
    'data_quality_checks':[
                    {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
                    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},
                    {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},
                    {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0},
                    {'check_sql': "SELECT COUNT(*) FROM songplays WHERE userid is null", 'expected_result':0}
                   ]
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path="auto",
    dag=dag
)


load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    redshift_conn_id = "redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert,
    truncate_table=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert,
    truncate_table=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert,
    truncate_table=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,
    truncate_table=True,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    redshift_conn_id="redshift",
    dq_checks=default_args['data_quality_checks'],
    dag=dag
)

end_operator = DummyOperator(task_id="End_execution",  dag=dag)

# Load the staging table
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Load the songplays fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Load the dimension table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Run the quality checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# End execution
run_quality_checks >> end_operator