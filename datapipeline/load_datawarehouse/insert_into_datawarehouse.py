# insert_into_datawarehouse - Script to insert data into the data warehouse.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
import pandas as pd
from airflow.models import Variable
import mysql.connector as mysql
from mysql.connector import Error

# Control variables
DATABASE_USER = Variable.get("database_user")
DATABASE_PSWD = Variable.get("database_pwd")

def insert_into(conn, cursor, sql_query: str, df: pd.DataFrame):
    """
    It executes a SQL query to insert data into the MySQL server.

    Args:
        conn (connector mysql): The MySQL connector.
        cursor (cursor mysql): The cursor into the MySQL server.
        sql_query (str): The query to insert data into the MySQL server.
        df (pd.DataFrame): Dataframe with the data to inserto into the data warehouse.
    """
    for i, row in df.iterrows():
        cursor.execute(sql_query, tuple(row))
        conn.commit()

def fill_table(src: str, table: str, sql_query: str, process_date=None, base_folder='arima'):
    """
    Fill the table in the data warehouse with the provided data.

    Args:
        src (str): Source directory of the data.
        table (str): Table name where the data will be inserted.
        sql_query (str): SQL query to insert data into the data warehouse.
        process_date (str, optional): The date when the data was collect or processed. Defaults to None.
        base_folder (str, optional): The folder where the data is stored. Defaults to 'arima'.
    """
    # Stabilish the connection with da MySQL server.
    conn = mysql.connect(host     = "localhost",
                         database = "datawarehouse",
                         user     = DATABASE_USER,
                         password = DATABASE_PSWD)

    # Creates a dictionaty with the code and the abbreviation of each country.
    if table == 'SENTIMENTOS':
        # When the data is provided by Twitter API use this
        map_countries = {'AR':1, 'CL':2, 'EC':3, 'ES':4, 'MX':5}
    else:
        # In other case use this.
        map_countries = {'AR':1, 'CH':2, 'EC':3, 'ES':4, 'MX':5}
    
    # Establish a connection with the MySQL server and execute the query.
    try:
        if conn.is_connected(): # If the connection is established.
            cursor = conn.cursor()
            cursor.execute("SELECT database();") # Select the database(datawarehouse)
            record = cursor.fetchone()
            
            # If no process_date is specified, all existing data in the table
            # will be deleted before inserting the new data.
            if not process_date:
                sql = f"DELETE FROM datawarehouse.{table};"
                cursor.execute(sql)
                conn.commit()

            # Insert new data into the COVID_WORLDOMETER table
            if table == "COVID_WORLDOMETER":
                src_worldometer = f'{src}/{base_folder}_data.csv' # File with the data.
                df = pd.read_csv(src_worldometer) # Dataset with the data
                insert_into(conn, cursor, sql_query, df) # Execute the insertion

            else: # It is execute when the table is not the COVID_WOLDOMETER.
                for country in map_countries: # Loop to interate each country.
                    src_country = f'{src}/{base_folder}_{country}.csv' # File with the data.
                    df = pd.read_csv(src_country).dropna()
                    
                    # Select the data in the dataframe according with the process date.
                    if process_date:
                        column = 'forecast_date' if 'forecast_date' in df.columns else 'date'
                        df = df[df[column] == process_date]

                    # Loop to insert data into the data warehouse.
                    for i, row in df.iterrows():
                        cursor.execute(sql_query, tuple(row))
                        conn.commit()

    except Error as e: # When the connection is not stablished excecute this.
        print("Error while connecting to MySQL", e)

def execute(src: str, table: str, process_date=None):
    """
    This execute all the process to load and insert the data into thde data warehouse.

    Args:
        src (str): Source folder with the data to insert into the data warehouse.
        table (str): Tabla of the data warehouse where the data will be stored.
        process_date (str, optional): The date when the data was collect or processed. Defaults to None.
    """
    if table == 'PREVISAO_CASOS': # Create query to insert data into the table PREVISAO_CASOS
        sql_query = f"INSERT INTO datawarehouse.{table}(`paises_id`, "+\
                    "`calendario_data`, `data_prevista`, `casos_previstos`) "+\
                    "VALUES (%s, %s, %s, %s)"
        base_folder = 'arima'
    
    elif table == "COVID_JHONS_HOPKINS": # Create query to insert data into the table COVID_JHONS_HOPKINS
        sql_query = f"INSERT INTO datawarehouse.{table}(`paises_id`, "+\
                    "`calendario_data`, `confirmados`, `mortes`, `ativos`, "+\
                    "`novos_casos`, `novas_mortes`, `recuperados`) VALUES "+\
                    "(%s, %s, %s, %s, %s, %s, %s, %s)"
        base_folder = 'covid_jhons'
    
    elif table == "SENTIMENTOS": # Create query to insert data into the table SENTIMENTOS
        sql_query = f"INSERT INTO datawarehouse.{table}(`calendario_data`, "+\
                    "`paises_id`, `tweet`, `negativo`, `positivo`, `neutro`, "+\
                    "`composto`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        base_folder = 'tweet_sentiments'
    
    elif table == "COVID_WORLDOMETER": # Create query to insert data into the table COVID_WORLDOMETER
        columns = "(`paises_id`, `calendario_data`, `populacao`, `total_casos`, "+\
                  "`novos_casos`, `total_mortes`, `novas_mortes`, `total_recuperados`, "+\
                  "`novos_recuperados`, `casos_ativos`, `casos_criticos`, "+\
                  "`total_casos_1Mpop`, `total_mortes_1Mpop`, `total_testes`, "+\
                  "`total_testes_1Mpop`, `mortalidade`)"
        sql_query = f"INSERT INTO datawarehouse.{table}{columns} VALUES "+\
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        base_folder = 'worldometer'
    
    # Run the function to insert the data into the datawarehouse
    fill_table(src, table, sql_query, forecast_date, base_folder)


if __name__ == "__main__":
    src = sys.argv[1] # Gets the source folder
    table = sys.argv[2] # Gets the table name
    
    # Check if the process_date is provided
    if len(sys.argv) > 3:
        process_date = sys.argv[3]
    else:
        process_date = None

    execute(src, table, process_date) # Calls the execute function