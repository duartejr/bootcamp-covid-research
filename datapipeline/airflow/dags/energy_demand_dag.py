# energy_demand_dag - Dag to do ETL in energy demand data.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import pandas as pd
from datetime import datetime

# Import airflow functions 
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# Functions to connect to MySQL server
import mysql.connector as mysql
from mysql.connector import Error

# Control variables
DATABASE_USER = Variable.get("database_user")
DATABASE_PSWD = Variable.get("database_pwd")
DIR_DATALAKE  = Variable.get("dir_datalake")

def fill_table(src : str, table : str):
    """
    This function exports the data directly to the MySQL Server.

    Args:
        src (str): The location of the data to export to the MySQL Server.
        table (str): The table on the MySQL serve where the data will be stored.
    """
    
    demand_data = pd.read_csv(src) # Read the data.
    
    try: # Attempts to estabilish a connection with the MySQL server.
        # Configure the connection
        conn = mysql.connect(host     = 'localhost', 
                             database = 'datawarehouse',
                             user     = DATABASE_USER, 
                             password = DATABASE_PSWD)
        
        if conn.is_connected(): # When the connection is valid.
            cursor = conn.cursor()
            cursor.execute("SELECT database();") # Select the database(datawarehouse) 
            record = cursor.fetchone()
            
            # loop through the data frame, iterate in each row
            for i, row in demand_data.iterrows(): 
                # Query do insert ddata into the database. Here %S means string values,
                sql = f"INSERT INTO datawarehouse.{table}(`calendario_data`, `paises_id`, `demanda_TWh`) VALUES (%s, %s, %s)"
                cursor.execute(sql, tuple(row)) # Execute the previous query.
                # The connection is not auto committed by default, so we must commit to save our changes
                conn.commit()
    except Error as e: # If the connection is unsuccessfl, perform this action.
        print("Error while connecting to MySQL", e)

# Implementation of a DAG to insert the energy demand data into the datawarehouse.
with DAG(dag_id     = 'dag_energy_sql',
         catchup    = False,
         start_date = datetime.today()
         ) as dag:
    # This DAG is not scheduled as the original data is not updated peridically,
    # manual execution of the DAG is needed to process the data.

    # Task to insert the energy demand into the data warehouse.
    fill_energy_table_db = PythonOperator(
        task_id         = "load_energy_table_db",
        python_callable = fill_table,
        op_args         = [f'{DIR_DATALAKE}/gold/ember/energy_demand/energy_demand.csv',
                           'DEMANDA_ENERGIA'])

    fill_energy_table_db
