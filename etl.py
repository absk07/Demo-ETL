import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'Airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=50)
}
   
dag = DAG(
    dag_id="my_demo",
    default_args=default_args,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    tags=['demo', 'etl']
)

start_pipeline = DummyOperator(
    task_id="start_pipeline",
    dag=dag
)

extract_data = GCSToBigQueryOperator(
    task_id="stage_data_from_gcs_to_bq",
    bucket="<GCS_BUCKET_NAME>",
    source_objects=["<CSV_FILE_NAME>"],
    source_format="CSV",
    destination_project_dataset_table="<PROJECT_ID.DATASET_NAME.TABLE_NAME>",
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,
    field_delimiter=",",
    skip_leading_rows=1,
    dag=dag,
)

transform_and_load_data = BigQueryInsertJobOperator(
    task_id='transform_and_load_data_to_bq',
    configuration={
        'query': {
            'query': '''
                CREATE OR REPLACE TABLE `<PROJECT_ID.DATASET_NAME.TABLE_NAME>` AS
                SELECT
                    id,
                    season,
                    city,
                    date,
                    match_type,
                    player_of_match,
                    venue,
                    team1,
                    team2,
                    toss_winner,
                    toss_decision,
                    UPPER(TRIM(winner)) AS winner,
                    result,
                    result_margin,
                    target_runs,
                    target_overs,
                    super_over,
                    method,
                    umpire1,
                    umpire2
                FROM
                    `<PROJECT_ID.DATASET_NAME.TABLE_NAME>`
            ''',
            'useLegacySql': False,
        }
    },
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id="end_pipeline",
    dag=dag
)

start_pipeline >> extract_data >> transform_and_load_data >> end_pipeline