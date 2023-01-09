import sys
import shutil
import pandas as pd
from os.path import join, exists
from pyspark.sql import functions as f
from pyspark.sql import SparkSession, Row
from statsmodels.tsa.arima.model import ARIMAResults
from pyspark.sql.types import StructType, StructField, DateType, FloatType

def save(df, dest):
    df.toPandas().to_csv(dest, index=False)
    
def read_data(spark, src):
    df = spark.read.parquet(src)
    return df

def order_columns(df):
    df = df.orderBy('forecast_date', 'forecasted_date')
    df = df.select('country', 'forecast_date', 'forecasted_date', 'forecast')
    return df


def execute(spark, src, dest, countries):
    
    for i, country in enumerate(countries):
        src_data = join(src, f'{country}.parquet')
        dest_data = join(dest, f'arima_{country}.csv')
        df = read_data(spark, src_data)
        df = df.withColumn("country", f.lit(country))
        df = order_columns(df)
        save(df, dest_data)
        if i == 0:
            df_tot = df
        else:
            df_tot = df_tot.join(df)
    dest_data_tot = join(dest, 'arima_all.csv')
    save(df_tot, dest_data_tot)

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    countries = sys.argv[3].split(',')
    
    spark = SparkSession\
            .builder\
            .appName("covid_export_csv")\
            .getOrCreate()

    execute(spark, src, dest, countries)
    
    spark.stop()