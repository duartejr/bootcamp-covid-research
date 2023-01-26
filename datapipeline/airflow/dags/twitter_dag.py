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
import time
from airflow.models import Variable

dir_twitter_scripts = Variable.get("dir_twitter_scripts")
dir_sql_scripts = Variable.get("dir_sql_scripts")
dir_datalake = Variable.get("dir_datalake")


ARGS = {"owner"          : "airflow",
        "depends_on_past": False,
        "start_date"     : days_ago(1)}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
START_DATE = datetime.today() - timedelta(1)
BASE_FOLDER = join(dir_datalake,
                   "{stage}/twitter/query_covid/{country}/{partition}")
PARTITION_FOLDER = "extract_date={}".format(datetime.strftime(START_DATE , '%Y-%m-%d'))
countries = ["CL", "AR", "MX", "EC", "ES"]


def transform_twitter(**kwargs):
    src = kwargs["src"]
    dest = kwargs["dest"]
    process_date = kwargs["process_date"]
    table_name = kwargs["table_name"]
    subprocess.run(["python",
                    f"{dir_twitter_scripts}/tweet_transformation.py",
                    src, dest, process_date, table_name])

def sentimental_analysis(**kwargs):
    src = kwargs["src"]
    dest = kwargs["dest"]
    process_date = kwargs["process_date"]
    subprocess.run(["python",
                    f"{dir_twitter_scripts}/tweet_sentimental_analysis.py",
                    src, dest, process_date])

def export_csv(**kwargs):
    src = kwargs["src"]
    dest = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    f"{dir_twitter_scripts}/tweet_export_csv.py",
                    src, dest, countries])

def export_sql(**kwargs):
    src       = kwargs["src"]
    table     = kwargs["table"]
    
    if "extract_date" in kwargs.keys():
        extract_date = kwargs["extract_date"]
    else:
        extract_date = None

    if forecast_date:
        subprocess.run(["python",
                        f"{dir_sql_scripts}/insert_into_forecast_table.py",
                        src, table, extract_date])
    else:
        subprocess.run(["python",
                        f"{dir_sql_scripts}/insert_into_forecast_table.py",
                        src, table]) 

with DAG(dag_id          = "Twitter_dag",
         default_args    = ARGS,
         schedule_interval='0 1 * * *',
         max_active_runs = 1
         ) as dag:
    
    twitter_export_csv = PythonOperator(
            task_id         = "twitter_export_csv",
            python_callable = export_csv,
            op_args         = [],
            op_kwargs       = {"src": f"{dir_datalake}/silver/twitter/sentiment_analysis",
                               "dest":  f"{dir_datalake}/gold/twitter/sentiment_analysis",
                               "countries": ",".join(countries)}
        )

    twitter_export_sql = PythonOperator(
            task_id         = "twitter_export_sql",
            python_callable = export_sql,
            op_args         = [],
            op_kwargs       = {"src":   f"{dir_datalake}/gold/twitter/sentiment_analysis",
                               "extract_date": datetime.strftime(START_DATE, '%Y-%m-%d'),
                               "table": "SENTIMENTOS"}
        )


    for country in countries:
        twitter_operator = TwitterOperator(
            task_id    = "twitter_covid_" + country,
            query      = "covid",
            file_path  = join(BASE_FOLDER.format(stage     = "bronze",
                                                 country   = country,
                                                 partition = PARTITION_FOLDER),
                              "CovidTweets_{}.json".format(datetime.strftime(START_DATE, "%Y%m%d"))),
            start_time = datetime.strftime(START_DATE, "%Y-%m-%dT00:00:00.00Z"),
            end_time   = datetime.strftime(START_DATE, "%Y-%m-%dT23:59:59.00Z"),
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
                               "process_date" : datetime.strftime(START_DATE , '%Y-%m-%d'),
                               "table_name": "tweet_processed"}
        )
        
        twitter_sentiments_analysis = PythonOperator(
            task_id         = "twitter_sentiment_" + country,
            python_callable = sentimental_analysis,
            op_args         = [],
            op_kwargs       = {"src": BASE_FOLDER.format(stage      = "silver",
                                                         country    = country,
                                                         partition  = "tweet_processed"),
                               "dest":  f"{dir_datalake}/silver/twitter/sentiment_analysis/{country}",
                               "process_date": datetime.strftime(START_DATE , '%Y-%m-%d')}
        )

        twitter_operator >> twitter_transform >> twitter_sentiments_analysis >> twitter_export_csv >> twitter_export_sql
