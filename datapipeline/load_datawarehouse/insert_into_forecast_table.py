from datetime import datetime
import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import os
import sys
from airflow.models import Variable

database_user = Variable.get("database_user")
database_pwd = Variable.get("database_pwd")
map_countries = {'AR':1, 'CH':2, 'EC':3, 'ES':4, 'MX':5}

def insert_into(conn, cursor, sql_query, df):
	for i, row in df.iterrows():
		cursor.execute(sql_query, tuple(row))
		conn.commit()

def fill_table(src, table, sql_query, forecast_date=None, base_folder='arima'):
	conn = mysql.connect(host = "localhost",
		                 database = "datawarehouse",
		                 user = database_user,
		                 password = database_pwd)

	if table == 'SENTIMENTOS':
		map_countries = {'AR':1, 'CL':2, 'EC':3, 'ES':4, 'MX':5}
	else:
		map_countries = {'AR':1, 'CH':2, 'EC':3, 'ES':4, 'MX':5}
		
	try:
		if conn.is_connected():
			cursor = conn.cursor()
			cursor.execute("SELECT database();")
			record = cursor.fetchone()
			if not forecast_date:
				sql = f"DELETE FROM datawarehouse.{table};"
				cursor.execute(sql)
				conn.commit()

			if table == "COVID_WORLDOMETER":
				src_worldometer = f'{src}/{base_folder}_data.csv'
				df = pd.read_csv(src_worldometer)
				insert_into(conn, cursor, sql_query, df)

			else:
				for country in map_countries:
					src_country = f'{src}/{base_folder}_{country}.csv'
					df = pd.read_csv(src_country).dropna()

					if forecast_date:
						column = 'forecast_date' if 'forecast_date' in df.columns else 'date'
						df = df[df[column] == forecast_date]

					for i, row in df.iterrows():
						cursor.execute(sql_query, tuple(row))
						conn.commit()

	except Error as e:
		print("Error while connecting to MySQL", e)


def execute(src, table, forecast_date=None):
	if table == 'PREVISAO_CASOS':
		sql_query = f"INSERT INTO datawarehouse.{table}(`paises_id`, `calendario_data`, `data_prevista`, `casos_previstos`) VALUES (%s, %s, %s, %s)"
		base_folder = 'arima'
	if table == "COVID_JHONS_HOPKINS":
		sql_query = f"INSERT INTO datawarehouse.{table}(`paises_id`, `calendario_data`, `confirmados`, `mortes`, `ativos`, `novos_casos`, `novas_mortes`, `recuperados`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
		base_folder = 'covid_jhons'
	if table == "SENTIMENTOS":
		sql_query = f"INSERT INTO datawarehouse.{table}(`calendario_data`, `paises_id`, `tweet`, `negativo`, `positivo`, `neutro`, `composto`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
		base_folder = 'tweet_sentiments'
	if table == "COVID_WORLDOMETER":
		columns = '(`paises_id`, `calendario_data`, `populacao`, `total_casos`, `novos_casos`, `total_mortes`, `novas_mortes`, `total_recuperados`, `novos_recuperados`, `casos_ativos`, `casos_criticos`, `total_casos_1Mpop`, `total_mortes_1Mpop`, `total_testes`, `total_testes_1Mpop`)'
		sql_query = f"INSERT INTO datawarehouse.{table}{columns} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
		base_folder = 'worldometer'
	
	fill_table(src, table, sql_query, forecast_date, base_folder)


if __name__ == "__main__":
	src = sys.argv[1]
	table = sys.argv[2]
	if len(sys.argv) > 3:
		forecast_date = sys.argv[3]
	else:
		forecast_date = None

	execute(src, table, forecast_date)