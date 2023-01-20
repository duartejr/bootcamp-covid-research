from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import os

def carrega_tabela(src, table):
	
	empdata = pd.read_csv(src)
	try:
		conn = mysql.connect(host='localhost', 
			                 database='datawarehouse',
			                 user='root', 
			                 password='root')
		if conn.is_connected():
			cursor = conn.cursor()
			cursor.execute("select database();")
			record = cursor.fetchone()
			#loop through the data frame
			for i,row in empdata.iterrows(): 
				#here %S means string values 
				sql = f"INSERT INTO datawarehouse.{table}(`calendario_data`, `paises_id`, `demanda_TWh`) VALUES (%s, %s, %s)"
				cursor.execute(sql, tuple(row))
				# the connection is not auto committed by default, so we must commit to save our changes
				conn.commit()
	except Error as e:
		print("Error while connecting to MySQL", e)


with DAG(dag_id = 'dag_energy_sql',
	catchup=False,
	start_date = datetime.today()) as dag:

	carrega_tabela_energia_db = PythonOperator(
		task_id = "carrega_tabela_energia_db",
		python_callable = carrega_tabela,
		op_args= ['/mnt/d/bootcamp-covid/datalake/gold/ember/energy_demand/energy_demand.csv',
				  'DEMANDA_ENERGIA'])

	carrega_tabela_energia_db


# if __name__ == "__main__":
# 	carrega_tabela()