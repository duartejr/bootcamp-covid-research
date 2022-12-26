from os.path import join
from pathlib import Path
from datetime import datetime, date
from airflow.models import DAG
from sqlalchemy import TIMESTAMP
from operators.twitter_operator import TwitterOperator
from airflow.operators.python_operator import PythonOperator
import subprocess
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

ARGS = {"owner"          : "airflow",
        "depends_on_past": False,
        "start_date"     : days_ago(1)}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

BASE_FOLDER = join(str(Path("/mnt/d/bootcamp-covid")),
                   "datalake/{stage}/twitter/query_covid/{country}/{partition}")
PARTITION_FOLDER = "extract_date={{ ds }}"

countries = ["ES", "EC", "CL", "MX", "AR"]

def transform_twitter(**kwargs):
    src = kwargs["src"]
    dest = kwargs["dest"]
    process_date = kwargs["process_date"]
    table_name = kwargs["table_name"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covid/datapipeline/twitter_process/tweet_transformation.py",
                    src, dest, process_date, table_name])

def sentimental_analysis(**kwargs):
    src = kwargs["src"]
    dest = kwargs["dest"]
    process_date = kwargs["process_date"]
    table_name = kwargs["table_name"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covid/datapipeline/twitter_process/tweet_sentimental_analysis.py",
                    src, dest, process_date, table_name])
    

with DAG(dag_id          = "Twitter_dag",
         default_args    = ARGS,
         #schedule_interval="0 9 * * *", #padrao CRON minutos horas dias meses diasdasemana
         max_active_runs = 1
         ) as dag:
    
    for country in countries:
        twitter_operator = TwitterOperator(
            task_id    = "twitter_covid_" + country,
            query      = "covid",
            file_path  = join(BASE_FOLDER.format(stage     = "bronze",
                                                 country   = country,
                                                 partition = PARTITION_FOLDER),
                              "CovidTweets_{{ ds_nodash }}.json"),
            start_time = datetime.strftime(datetime.now() - timedelta(days=2), 
                                           TIMESTAMP_FORMAT),
            end_time   = datetime.strftime(datetime.now() - timedelta(days=1), 
                                           TIMESTAMP_FORMAT),
            country    = country,
        )

        twitter_transform = PythonOperator(
            task_id         = "twitter_transform_" + country,
            python_callable = transform_twitter,
            op_args         = [],
            op_kwargs       = {"src" : BASE_FOLDER.format(stage      = "bronze",
                                                          country    = country,
                                                          partition  = PARTITION_FOLDER),
                               "dest" : BASE_FOLDER.format(stage     = "silver",
                                                           country   = country,
                                                           partition = ""),
                               "process_date" : "{{ ds }}",
                               "table_name": "tweet_processed"}
        )
        
        twitter_sentiments_analysis = PythonOperator(
            task_id         = "twitter_sentiment_" + country,
            python_callable = sentimental_analysis,
            op_args         = [],
            op_kwargs       = {"src": BASE_FOLDER.format(stage      = "silver",
                                                         country    = country,
                                                         partition  = "tweet_processed"),
                               "dest": BASE_FOLDER.format(stage     = "silver",
                                                          country   = country,
                                                          partition = ""),
                               "process_date": "{{ ds }}",
                               "table_name": "tweet_sentiments"}
        )

        twitter_operator >> twitter_transform >> twitter_sentiments_analysis
