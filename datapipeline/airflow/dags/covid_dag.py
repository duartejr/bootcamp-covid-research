import subprocess
from os.path import join
from pathlib import Path
from datetime import timedelta
from airflow.models import DAG
from datetime import datetime as dt
from airflow.utils.dates import days_ago
from operators.covid_operator import CovidOperator
from airflow.operators.python_operator import PythonOperator

ARGS             = {"owner"           : "airflow",
                    "depends_on_past" : False,
                    "start_date"      : days_ago(1)}
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'
BASE_FOLDER      = join(str(Path("/mnt/d/bootcamp-covid")),
                                 "datalake/{stage}/covid_data/{partition}")
EXTRACT_DATE     = dt.now() - timedelta(days=1)
PARTITION_FOLDER = f"extract_date={dt.strftime(EXTRACT_DATE, '%Y-%m-%d')}"
COUNTRIES        = "Spain,Ecuador,Chile,Mexico,Argentina"
COUNTRIES_ABRV   = "ES,EC,CH,MX,AR"


def transform_covid_data(**kwargs):
    src            = kwargs["src"]
    dest           = kwargs["dest"]
    extract_date   = kwargs["extract_date"]
    countries      = kwargs["countries"]
    countries_abrv = kwargs["countries_abrv"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covid/datapipeline/covid_data_process/covid_data_transformation.py",
                    src, dest, extract_date, countries, countries_abrv])


def calc_covid_fields(**kwargs):
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covi/datapipeline/covid_data_process/covid_data_calc_fields.py",
                    src, dest, countries])


def forecast_covid_cases(**kwargs):
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covi/datapipeline/covid_data_process/covid_cases_forecast.py",
                    src, dest, countries])
    

with DAG(dag_id          = "Covid_dag",
         default_args    = ARGS,
         max_active_runs = 1) as dag:
    
    covid_operator = CovidOperator(
        task_id    = "get_covid_data",
        file_path  = join(BASE_FOLDER.format(stage     = "bronze",
                                             partition = PARTITION_FOLDER),
                          f"CovidData_{dt.strftime(EXTRACT_DATE, '%Y%m%d')}.csv"),
        date       = dt.strftime(EXTRACT_DATE, '%m-%d-%Y')
    )
    
    covid_time_series   = PythonOperator(
        task_id         = "covid_data_transform",
        python_callable = transform_covid_data,
        op_args         = [],
        op_kwargs       = {"src" : BASE_FOLDER.format(stage      = "bronze",
                                                      partition  = PARTITION_FOLDER),
                           "dest" : BASE_FOLDER.format(stage     = "silver",
                                                       partition = "time_series"),
                           "extract_date" : dt.strftime(EXTRACT_DATE, '%Y-%m-%d'),
                           "countries": COUNTRIES,
                           "countries_abrv": COUNTRIES_ABRV}
    )
    
    covid_calc_fields   = PythonOperator(
        task_id         = "covid_calc_fields",
        python_callable = calc_covid_fields,
        op_args         = [],
        op_kwargs       = {"src": BASE_FOLDER.format(stage      = "silver",
                                                     partition  = "time_series"),
                           "dest": BASE_FOLDER.format(stage     = "silver",
                                                      partition = "series_with_calc_fields"),
                           "countries": COUNTRIES_ABRV}
    )
    
    covid_cases_forecast = PythonOperator(
        task_id          = "covid_cases_forecast",
        python_callable  = forecast_covid_cases,
        op_args          = [],
        op_kwargs        = {"src": BASE_FOLDER.format(stage      = "silver",
                                                      partition  = "series_with_calc_fields"),
                            "dest": BASE_FOLDER.format(stage     = "silver",
                                                       partition = "forecast"),
                            "countries": COUNTRIES_ABRV}
    )
    
    covid_operator >> covid_time_series >> covid_calc_fields >> covid_cases_forecast
