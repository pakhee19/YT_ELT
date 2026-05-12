from airflow import DAG
from datawarehouse.dwh import core_table, staging_table
from api.video_stats import get_playlistID, get_video_ids, extract_video_data, save_to_json
import pendulum
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_api_data_quality_check

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['pakheem19@gmail.com'],
    'start_date': datetime(2024, 6, 1, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"
#Dag 1 produce_json
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to fetch YouTube data and save to JSON',
    schedule='0 14 * * *',
    catchup=False
) as dag_produce:
    playlist_id = get_playlistID()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json_task=save_to_json(video_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id='trigger_update_db',
        trigger_dag_id='update_db',
    )
    playlist_id >> video_ids >> video_data >> save_to_json_task >> trigger_update_db

#Dag 2 update_db

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON data and update the database',
    schedule=None,
    catchup=False
) as dag_update:
    
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='data_quality',
    )

    update_staging >> update_core >> trigger_data_quality

#Dag for data quality check
with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check data quality on both layers in the db',
    schedule=None,
    catchup=False
) as dag_quality:
    
    soda_validate_staging = yt_api_data_quality_check(staging_schema)
    soda_validate_core = yt_api_data_quality_check(core_schema)

    soda_validate_staging >> soda_validate_core