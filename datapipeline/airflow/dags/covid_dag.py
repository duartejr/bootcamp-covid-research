import subprocess
from os.path import join
from pathlib import Path
from datetime import timedelta
from airflow.models import DAG
from datetime import datetime as dt
from airflow.utils.dates import days_ago
from operators.covid_operator import CovidOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

dir_covid_scripts = Variable.get("dir_covid_scripts")
dir_sql_scripts = Variable.get("dir_sql_scripts")
dir_datalake = Variable.get("dir_datalake")

ARGS             = {"owner"           : "airflow",
                    "depends_on_past" : False,
                    "start_date"      : days_ago(1)}
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'
BASE_FOLDER      = join(dir_datalake, "{stage}/covid_data/{partition}")
EXTRACT_DATE     = dt.today()# - timedelta(days=1)
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
                    f"{dir_covid_scripts}/covid_data_transformation.py",
                    src, dest, extract_date, countries, countries_abrv])


def calc_covid_fields(**kwargs):
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    f"{dir_covid_scripts}/covid_data_calc_fields.py",
                    src, dest, countries])


def forecast_covid_cases(**kwargs):
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    f"{dir_covid_scripts}/covid_cases_forecast.py",
                    src, dest, countries, ""])
    

def export_csv(**kwargs):
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    table     = kwargs["table"]
    subprocess.run(["python",
                    f"{dir_covid_scripts}/covid_export_csv.py",
                    src, dest, countries, table])

def export_fcst_sql(**kwargs):
    src       = kwargs["src"]
    table     = kwargs["table"]
    
    if "extract_date" in kwargs.keys():
        extract_date = kwargs["extract_date"]
    else:
        extract_date = None

    if extract_date:
        subprocess.run(["python",
                        f"{dir_sql_scripts}/insert_into_forecast_table.py",
                        src, table, extract_date])
    else:
        subprocess.run(["python",
                        f"{dir_sql_scripts}/insert_into_forecast_table.py",
                        src, table]) 
    

with DAG(dag_id          = "Covid_dag",
         default_args    = ARGS,
         schedule_interval='0 21 * * *',
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
    
    covid_export_fcst_csv = PythonOperator(
        task_id          = "covid_export_fcst_csv",
        python_callable  = export_csv,
        op_args          = [],
        op_kwargs        = {"src": BASE_FOLDER.format(stage      = "silver",
                                                      partition  = "forecast"),
                            "dest": BASE_FOLDER.format(stage     = "gold",
                                                       partition = "forecast/arima"),
                            "countries": COUNTRIES_ABRV,
                            "table": "forecast"}
    )

    covid_export_jhons_csv = PythonOperator(
        task_id          = "covid_export_jhons_csv",
        python_callable  = export_csv,
        op_args          = [],
        op_kwargs        = {"src": BASE_FOLDER.format(stage      = "silver",
                                                      partition  = "series_with_calc_fields"),
                            "dest": BASE_FOLDER.format(stage     = "gold",
                                                       partition = "jhons_hopkins"),
                            "countries": COUNTRIES_ABRV,
                            "table": "jhons_hopkins"}
    )

    covid_export_forecast_sql = PythonOperator(
        task_id          = "covid_export_forecast_sql",
        python_callable  = export_fcst_sql,
        op_args          = [], 
        op_kwargs        = {"src": BASE_FOLDER.format(stage      = "gold",
                                                      partition  = "forecast/arima"),
                            "table": "PREVISAO_CASOS",
                            "extract_date": dt.strftime(EXTRACT_DATE, '%Y-%m-%d'),}
    )

    covid_export_jhons_sql = PythonOperator(
        task_id          = "covid_export_jhons_sql",
        python_callable  = export_fcst_sql,
        op_args          = [], 
        op_kwargs        = {"src": BASE_FOLDER.format(stage      = "gold",
                                                      partition  = "jhons_hopkins"),
                            "table": "COVID_JHONS_HOPKINS",
                            "extract_date": dt.strftime(EXTRACT_DATE, '%Y-%m-%d')}
    )
    
    covid_operator >> covid_time_series >> covid_calc_fields 
    covid_calc_fields >> covid_export_jhons_csv >> covid_export_jhons_sql
    covid_calc_fields >> covid_cases_forecast >> covid_export_fcst_csv
    covid_export_fcst_csv >> covid_export_forecast_sql