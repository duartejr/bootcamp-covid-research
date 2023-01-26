from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import subprocess
from airflow.models import Variable

dir_worldometer_scripts = Variable.get("dir_worldometer_scripts")
dir_sql_scripts = Variable.get("dir_sql_scripts")
dir_datalake = Variable.get("dir_datalake")
EXTRACT_DATE = datetime.today()

def get_worldometer_data(**kwargs):
    src            = "-"
    dest           = kwargs["dest"]
    subprocess.run(["python",
                    f"{dir_worldometer_scripts}/get_worldometer_data.py",
                    src, dest, 'get'])

def worldometer_export_csv(**kwargs):
    src            = kwargs["src"]
    dest           = kwargs["dest"]
    subprocess.run(["python",
                    f"{dir_worldometer_scripts}/get_worldometer_data.py",
                    src, dest, 'export'])

def export_sql(**kwargs):
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

with DAG(dag_id = 'dag_worldometer', 
         catchup=False,
         schedule_interval='0 21 * * *',
         start_date = datetime.today()) as dag:

    get_worldometer_data = PythonOperator(
        task_id = "get_worldometer_data",
        python_callable = get_worldometer_data,
        op_args= [],
        op_kwargs = {"src": '-',
                   "dest": f'{dir_datalake}/bronze/worldometer',
                  })

    export_worldometer_csv = PythonOperator(
        task_id = "export_worldometer_data",
        python_callable = worldometer_export_csv,
        op_args = [],
        op_kwargs = {"src": f'{dir_datalake}/bronze/worldometer', 
                     "dest": f'{dir_datalake}/gold/covid_data/worldometer'})

    export_worldometer_sql = PythonOperator(
        task_id = "export_worldometer_sql",
        python_callable = export_sql,
        op_args = [],
        op_kwargs = {"src": f"{dir_datalake}/gold/covid_data/worldometer",
                     "table": "COVID_WORLDOMETER"
                     }
        )
    
    get_worldometer_data >> export_worldometer_csv >> export_worldometer_sql
