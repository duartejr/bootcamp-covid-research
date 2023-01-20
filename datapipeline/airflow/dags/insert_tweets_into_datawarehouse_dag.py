from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import os
from glob import glob
#

map_countries = {'AR':1, 'CH':2, 'EC':3, 'ES':4, 'MX':5}

def carrega_tabela(src, table):
	
	for country in map_countries:
		src2 = f"{src}/{country}"
		files = glob(f'{src2}/*.csv')
		for file in files:
			empdata = pd.read_csv(file)
			empdata = empdata[['date', 'texto', 'neg', 'neu', 'pos', 'compound']]
			empdata['country'] = map_countries[country]

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
						sql = f"INSERT INTO datawarehouse.{table}(`calendario_data`, `tweet`, `negativo`, `positivo`, `neutro`, `composto`, `paises_id`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
						cursor.execute(sql, tuple(row))
						# the connection is not auto committed by default, so we must commit to save our changes
						conn.commit()
			except Error as e:
				print("Error while connecting to MySQL", e)


with DAG(dag_id = 'dag_twitter_sql', 
	schedule_interval="30 * * * *", 
	catchup=False,
	start_date = datetime.today()) as dag:

	carrega_tabela_twitter_db = PythonOperator(
		task_id = "carrega_tabela_twitter_db",
		python_callable = carrega_tabela,
		op_args= ['/mnt/d/bootcamp-covid/datalake/gold/twitter/sentimental_analysis',
				  'SENTIMENTOS'])

	carrega_tabela_twitter_db


# if __name__ == "__main__":
# 	carrega_tabela()