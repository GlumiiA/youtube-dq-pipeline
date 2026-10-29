from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

#Define the local timezone
local_tz = pendulum.timezone("Europe/Riga")

#Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email":"hello@raivis.it",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2026, 5, 27, tzinfo=local_tz),
    # "end_date": datetime(2024, 6, 30, tzinfo=local_tz)
}

# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: produce_json
with DAG(
     dag_id="produce_json",
     default_args=default_args,
     description="A DAG to extract video stats from YouTube API and save it as JSON",
     schedule="0 14 * * *", # Run daily at 14:00 (2 PM) local time
     catchup=False
) as dag_produce:
    
    # Define the tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # Define dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task >> trigger_update_db


# DAG 2: update_db
with DAG(
     dag_id="update_db",
     default_args=default_args,
     description="DAG to process JSON file and insert data into the staging and core schemas",
     schedule=None,
     catchup=False
) as dag_update:
    
    # Define the tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # Define dependencies
    update_staging >> update_core >> trigger_data_quality


# DAG 3: data_quality
with DAG(
     dag_id="data_quality",
     default_args=default_args,
     description="DAG to check the data quality on both layers in the db",
     schedule=None,
     catchup=False
) as dag_quality:
    
    # Define the tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core