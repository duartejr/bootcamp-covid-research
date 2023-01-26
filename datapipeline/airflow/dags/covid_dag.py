# covid_dag - Dag to do ETL in Covid data from Jhons Hopkins.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.

import subprocess
from os.path import join
from datetime import datetime as dt

# Import airflow functions 
from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from operators.covid_operator import CovidOperator # Import operator to download covid data

# Control variables
DIR_COVID_SCRIPTS = Variable.get("dir_covid_scripts")
DIR_SQL_SCRIPTS   = Variable.get("dir_sql_scripts")
DIR_DATALAKE      = Variable.get("dir_datalake")
ARGS              = {"owner"           : "airflow",
                     "depends_on_past" : False,
                     "start_date"      : days_ago(1)}
TIMESTAMP_FORMAT  = '%Y-%m-%dT%H:%M:%S.00Z'
BASE_FOLDER       = join(DIR_DATALAKE, "{stage}/covid_data/{partition}")
EXTRACT_DATE      = dt.today()
PARTITION_FOLDER  = f"extract_date={dt.strftime(EXTRACT_DATE, '%Y-%m-%d')}"
COUNTRIES         = "Spain,Ecuador,Chile,Mexico,Argentina"
COUNTRIES_ABRV    = "ES,EC,CH,MX,AR"


def transform_covid_data(**kwargs):
    """
    This function calls the script "covid_data_transformation.py" which
    transforms the raw data into time series data separated by country.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    extract_date (str) : The date on which the data was collected.
    countries (str) : A list of country names separeted by commas.
    countries_abrv (str) : A list of abbreviations of country names, separated
                           by commas. For example: "AR,ES" representing Argentina
                           and Spain.
    
    """
    src            = kwargs["src"]
    dest           = kwargs["dest"]
    extract_date   = kwargs["extract_date"]
    countries      = kwargs["countries"]
    countries_abrv = kwargs["countries_abrv"]
    subprocess.run(["python",
                    f"{DIR_COVID_SCRIPTS}/covid_data_transformation.py",
                    src, dest, extract_date, countries, countries_abrv])


def calc_covid_fields(**kwargs):
    """
    This function calls the script "covid_data_calc_fields.py" which 
    calculates fields such as new cases, new_deaths, and active_cases for each
    country.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    countries (str) : A list of country names separeted by commas.
    """
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    f"{DIR_COVID_SCRIPTS}/covid_data_calc_fields.py",
                    src, dest, countries])


def forecast_covid_cases(**kwargs):
    """
    This function calls the script "covid_cases_forecast.py" which generates
    forecasts for the number of COVID cases for each country.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    countries (str) : A list of country names separeted by commas.
    """
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    f"{DIR_COVID_SCRIPTS}/covid_cases_forecast.py",
                    src, dest, countries, ""])
    

def export_csv(**kwargs):
    """
    This function calls the script "covid_export_csv.py" which saves the data
    in CSV format in the destination folder.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    countries (str) : A list of country names separeted by commas.
    table (str): The name of the folder where the final data will be saved.
    """
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    table     = kwargs["table"]
    subprocess.run(["python",
                    f"{DIR_COVID_SCRIPTS}/covid_export_csv.py",
                    src, dest, countries, table])


def export_fcst_sql(**kwargs):
    """
    This function calls the script "insert_into_datawarehouse.py" which exports
    the data to a data warehouse.
    
    Attributes:
    src (str) : The directory where the original data is located.
    table (str): The name of the table where the final data will be storaged.
    extract_date (str) : The date on which the data was collected. The default 
                         is None.
    """
    src       = kwargs["src"]
    table     = kwargs["table"]
    
    if "extract_date" in kwargs.keys():
        extract_date = kwargs["extract_date"]
    else:
        extract_date = None # If no extract_Date is provided the default is None

    if extract_date:
        # If a extract date is provided, this call will add a new row to the
        # table in the data warehouse, rather then overwriting existing data.
        subprocess.run(["python",
                        f"{DIR_SQL_SCRIPTS}/insert_into_datawarehouse.py",
                        src, table, extract_date])
    else:
        # Otherwise, if no extract data is provided, al the data in the table
        # will be replaced with the new data.
        subprocess.run(["python",
                        f"{DIR_SQL_SCRIPTS}/insert_into_datawarehouse.py",
                        src, table]) 
    
# Implementation of a DAG to control the ETL process of COVID data.
with DAG(dag_id            = "Covid_dag", # DAG name for the Airflow control panel
         default_args      = ARGS,        # Setting the default args
         schedule_interval ='0 21 * * *', # Schedule the DAG to run every day at 21 hours
         max_active_runs   = 1) as dag:   
    
    # Task to download the COVID data provided by Jhons Hopkins.
    covid_operator = CovidOperator(
        task_id    = "get_covid_data",
        file_path  = join(BASE_FOLDER.format(stage     = "bronze",
                                             partition = PARTITION_FOLDER),
                          f"CovidData_{dt.strftime(EXTRACT_DATE, '%Y%m%d')}.csv"),
        date       = dt.strftime(EXTRACT_DATE, '%m-%d-%Y')
    )
    
    # Task to convert the raw data into time series to each country.
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
    
    # Task to calc fields like: new case, new deaths, new recoved.
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
    
    # Taks to forecast COVID cases.
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
    
    # Task to export the forecasts into CSV format.
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

    # Task to export the COVID data into CSV format.
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

    # Task to export the forecasts into data warehouse.
    covid_export_forecast_sql = PythonOperator(
        task_id          = "covid_export_forecast_sql",
        python_callable  = export_fcst_sql,
        op_args          = [], 
        op_kwargs        = {"src": BASE_FOLDER.format(stage      = "gold",
                                                      partition  = "forecast/arima"),
                            "table": "PREVISAO_CASOS",
                            "extract_date": dt.strftime(EXTRACT_DATE, '%Y-%m-%d'),}
    )

    # Task to export the COVID cases into data warehouse.
    covid_export_jhons_sql = PythonOperator(
        task_id          = "covid_export_jhons_sql",
        python_callable  = export_fcst_sql,
        op_args          = [], 
        op_kwargs        = {"src": BASE_FOLDER.format(stage      = "gold",
                                                      partition  = "jhons_hopkins"),
                            "table": "COVID_JHONS_HOPKINS",
                            "extract_date": dt.strftime(EXTRACT_DATE, '%Y-%m-%d')}
    )
    
    # Controls the excecution order of the tasks
    covid_operator >> covid_time_series >> covid_calc_fields 
    covid_calc_fields >> covid_export_jhons_csv >> covid_export_jhons_sql
    covid_calc_fields >> covid_cases_forecast >> covid_export_fcst_csv
    covid_export_fcst_csv >> covid_export_forecast_sql