from os.path import join
from pathlib import Path
from datetime import datetime, date
from airflow.models import DAG
from sqlalchemy import TIMESTAMP
from operators.twitter_operator import TwitterOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6)
}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

BASE_FOLDER = join(
    str(Path("/mnt/d/bootcamp-covid")),
    "datalake/{stage}/twitter_covid/{partition}"
)
PARTITION_FOLDER = "extract_data={{ ds }}"

countries = ["ES", 'EC', 'CL', 'MX', 'AR', 'BR']

with DAG(
    dag_id="Twitter_dag",
    default_args=ARGS,
    #schedule_interval="0 9 * * *", #padrao CRON minutos horas dias meses diasdasemana
    max_active_runs=1
    ) as dag:
    
    
    twitter_operator = TwitterOperator(
        task_id="twitter_bootcamp-covid",
        query="covid",
        file_path=join(
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "CovidTweets_{{ ds_nodash }}.json"
        ),
        start_time=datetime.strftime(datetime.now() - timedelta(days=5), TIMESTAMP_FORMAT),
        end_time=datetime.strftime(datetime.now() - timedelta(days=0), TIMESTAMP_FORMAT),
        country="BR"
    )

    # twitter_transform = SparkSubmitOperator(
    #     task_id = "transform_twitter_aluraonline",
    #     application = join(
    #         "/mnt/d/alura/datapipeline",
    #         "spark/transformation.py"
    #     ),
    #     name = "twitter_transformation",
    #     application_args = [
    #         "--src",
    #         BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
    #         "--dest",
    #         BASE_FOLDER.format(stage="silver", partition=""),
    #         "--process-date",
    #         "{{ ds }}"
    #     ]
    # )

    twitter_operator #>> twitter_transform
