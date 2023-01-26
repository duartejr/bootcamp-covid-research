# worldometer_dag - Dag to do ETL in Twitter data.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import subprocess
from datetime import datetime

# Import airflow functions 
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# Control variables
DIR_WORLDOMETER_SCRIPTS = Variable.get("dir_worldometer_scripts")
DIR_SQL_SCRIPTS         = Variable.get("dir_sql_scripts")
DIR_DATALAKE            = Variable.get("dir_datalake")
EXTRACT_DATE            = datetime.today()


def get_worldometer_data(**kwargs):
    """
    This function calls the script "get_worldometer_data.py" which performs a
    web scraping on the Worldometer website to collect the data available for the
    current date.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    """
    src            = "-"
    dest           = kwargs["dest"]
    subprocess.run(["python",
                    f"{DIR_WORLDOMETER_SCRIPTS}/get_worldometer_data.py",
                    src, dest, 'get'])


def worldometer_export_csv(**kwargs):
    """
    This function calls the script "get_worldometer_data.py" to saves the data
    in CSV format in the destination folder.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    """
    src            = kwargs["src"]
    dest           = kwargs["dest"]
    subprocess.run(["python",
                    f"{DIR_WORLDOMETER_SCRIPTS}/get_worldometer_data.py",
                    src, dest, 'export'])


def export_sql(**kwargs):
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

# Implementation of a DAG to control the ETL process of Worldometer data.
with DAG(dag_id            = 'dag_worldometer', 
         catchup           = False,
         schedule_interval = '0 21 * * *', # Scheduled to run every day at 9PM
         start_date        = datetime.today()
         ) as dag:

    # Task to download the Worldometer data
    get_worldometer_data = PythonOperator(
        task_id         = "get_worldometer_data",
        python_callable = get_worldometer_data,
        op_args         = [],
        op_kwargs       = {"src": '-',
                           "dest": f'{DIR_DATALAKE}/bronze/worldometer',
                          })

    # Task to export the data into CSV format
    export_worldometer_csv = PythonOperator(
        task_id         = "export_worldometer_data",
        python_callable = worldometer_export_csv,
        op_args         = [],
        op_kwargs       = {"src": f'{DIR_DATALAKE}/bronze/worldometer', 
                           "dest": f'{DIR_DATALAKE}/gold/covid_data/worldometer'
                           })

    # Task to export the data into the datawarehouse
    export_worldometer_sql = PythonOperator(
        task_id         = "export_worldometer_sql",
        python_callable = export_sql,
        op_args         = [],
        op_kwargs       = {"src": f"{DIR_DATALAKE}/gold/covid_data/worldometer",
                           "table": "COVID_WORLDOMETER"
                          }
        )
    
    # Exceution order of the tasks
    get_worldometer_data >> export_worldometer_csv >> export_worldometer_sql
