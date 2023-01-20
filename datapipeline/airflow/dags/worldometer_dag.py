from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import subprocess
from airflow.utils.dates import days_ago
import os

EXTRACT_DATE     = datetime.today()

def get_worldometer_data(**kwargs):
    src            = "-"
    dest           = kwargs["dest"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covid/datapipeline/worldometer_process/get_worldometer_data.py",
                    src, dest, 'get'])

def worldometer_export_csv(**kwargs):
    src            = kwargs["src"]
    dest           = kwargs["dest"]
    subprocess.run(["python",
                    "/mnt/d/bootcamp-covid/datapipeline/worldometer_process/get_worldometer_data.py",
                    src, dest, 'export'])

def export_sql(**kwargs):
    src       = kwargs["src"]
    table     = kwargs["table"]

    if "extract_date" in kwargs.keys():
        extract_date = kwargs["extract_date"]
    else:
        extract_date = None

    if forecast_date:
        subprocess.run(["python",
                        "/mnt/d/bootcamp-covid/datapipeline/load_datawarehouse/insert_into_forecast_table.py",
                        src, table, extract_date])
    else:
        subprocess.run(["python",
                        "/mnt/d/bootcamp-covid/datapipeline/load_datawarehouse/insert_into_forecast_table.py",
                        src, table]) 

with DAG(dag_id = 'dag_worldometer', 
	     catchup=False,
	     start_date = datetime.today()) as dag:

	get_worldometer_data = PythonOperator(
		task_id = "get_worldometer_data",
		python_callable = get_worldometer_data,
		op_args= [],
		op_kwargs = {"src": '-',
		           "dest": '/mnt/d/bootcamp-covid/datalake/bronze/worldometer',
		          })

	export_worldometer_csv = PythonOperator(
		task_id = "export_worldometer_data",
		python_callable = worldometer_export_csv,
		op_args = [],
		op_kwargs = {"src": '/mnt/d/bootcamp-covid/datalake/bronze/worldometer', 
		             "dest": '/mnt/d/bootcamp-covid/datalake/gold/covid_data/worldometer'})

	export_worldometer_sql = PythonOperator(
		task_id = "export_worldometer_sql",
		python_callable = export_sql,
		op_args = [],
		op_kwargs = {"src": "/mnt/d/bootcamp-covid/datalake/gold/covid_data/worldometer",
		             "table": "COVID_WORLDOMETER",
		             "extract_date": datetime.strftime(EXTRACT_DATE, '%Y-%m-%d')}
		)

	get_worldometer_data >> export_worldometer_csv >> export_worldometer_sql
