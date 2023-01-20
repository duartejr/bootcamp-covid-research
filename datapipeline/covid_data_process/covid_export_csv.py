import sys
import shutil
import pandas as pd
from os.path import join, exists
from pyspark.sql import functions as f
from pyspark.sql import SparkSession, Row
from statsmodels.tsa.arima.model import ARIMAResults
from pyspark.sql.types import StructType, StructField, DateType, FloatType

map_countries = {'AR':1, 'CH':2, 'EC':3, 'ES':4, 'MX':5}

def save(df, dest):
    df = df.toPandas()
    df['country'] = df['country'].replace(map_countries)
    df.to_csv(dest, index=False)
    
def read_data(spark, src):
    df = spark.read.parquet(src)
    return df

def order_columns_forecast(df):
    df = df.orderBy('forecast_date', 'forecasted_date')
    df = df.select('country', 'forecast_date', 'forecasted_date', 'forecast')
    return df

def order_columns_jhons(df):
    df = df.orderBy('date')
    df = df.select('country', 'date', 'Confirmed', 'Deaths',
                   'Active', 'New_Confirmed', 'New_Deaths',
                   'New_Recovered')
    return df

def execute(spark, src, dest, countries, table):
    
    for i, country in enumerate(countries):
        src_data = join(src, f'{country}.parquet')
        if table == 'forecast':
            dest_data = join(dest, f'arima_{country}.csv')
        else:
            dest_data = join(dest, f'covid_jhons_{country}.csv')
        df = read_data(spark, src_data)
        df = df.withColumn("country", f.lit(country))
        if table == 'forecast':
            df = order_columns_forecast(df)
        else:
            df = order_columns_jhons(df)
        save(df, dest_data)
        
        if i == 0:
            df_tot = df
        else:
            df_tot = df_tot.union(df)
    
    if table == 'forecast':
        dest_data_tot = join(dest, 'arima_all.csv')
    else:
        dest_data_tot = join(dest, 'jhons_hopkins_all.csv')

    print(df_tot.toPandas().head())
    save(df_tot, dest_data_tot)

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    countries = sys.argv[3].split(',')
    table = sys.argv[4]
    
    spark = SparkSession\
            .builder\
            .appName("covid_export_csv")\
            .getOrCreate()

    execute(spark, src, dest, countries, table)
    
    spark.stop()