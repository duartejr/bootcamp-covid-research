import subprocess
from os.path import join
from pathlib import Path
from datetime import timedelta
from airflow.models import DAG
from sqlalchemy import TIMESTAMP
from datetime import datetime as dt
from airflow.utils.dates import days_ago
from operators.covid_operator import CovidOperator
from airflow.operators.python_operator import PythonOperator

ARGS = {"owner"           : "airflow",
        "depends_on_past" : False,
        "start_date"      : days_ago(1)}
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'
BASE_FOLDER = join(str(Path("/mnt/d/bootcamp-covid")),
                       "datalake/{stage}/covid_data/{country}/{partition}")
EXTRACT_DATE = dt.now() - timedelta(days=1)
PARTITION_FOLDER = f"extract_date={dt.strftime(EXTRACT_DATE, '%Y-%m-%d')}"
COUNTRIES = ["Spain", "Ecuador", "Chile", "Mexico", "Argentina"]

def transform_covid_data(**kwargs):
    src = kwargs["src"]
    dest = kwargs["dest"]
    extract_date = kwargs["extract_date"]
    country = kwargs["country"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covid/datapipeline/covid_data_process/covid_data_transformation.py",
                    src, dest, extract_date, country])
    

with DAG(dag_id          = "Covid_dag",
         default_args    = ARGS,
         max_active_runs = 1) as dag:
    
    covid_operator = CovidOperator(task_id   = "get_covid_data",
                                   file_path = join(BASE_FOLDER.format(stage     = "bronze",
                                                                       country   = "",
                                                                       partition = PARTITION_FOLDER),
                                                    f"CovidData_{dt.strftime(EXTRACT_DATE, '%Y%m%d')}.csv"),
                                   date      = dt.strftime(EXTRACT_DATE, '%m-%d-%Y'))
    
    covid_transform = PythonOperator(
            task_id = "covid_data_transform",
            python_callable = transform_covid_data,
            op_args = [],
            op_kwargs = {"src" : BASE_FOLDER.format(stage      = "bronze",
                                                          country    = country,
                                                          partition  = PARTITION_FOLDER),
                         "dest" : BASE_FOLDER.format(stage     = "silver",
                                                        country   = country,
                                                        partition = ""),
                         "process_date" : "{{ ds }}",
                         "table_name": "tweet_processed"}
    )

