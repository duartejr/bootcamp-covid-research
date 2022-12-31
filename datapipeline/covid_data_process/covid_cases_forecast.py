import sys
import shutil
import pandas as pd
from os.path import join, exists
from pyspark.sql import SparkSession
from statsmodels.tsa.arima.model import ARIMAResults


def forecast(obs_data, src_model, fcst_horizon=7):
    model = ARIMAResults.load(src_model)
    model = model.apply(obs_data)
    return model.forecast(fcst_horizon)

def save(df, dest):
    if exists(dest):
        shutil.rmtree(dest)
    df.coalesce(1).write.parquet(dest)

def read_data(spark, src):
    df = spark.read.parquet(src)
    return df

def execute(spark, src, dest, country, fcst_horizon=7):
    src = join(src, f'{country}.parquet')
    dest = join(dest, f'{country}.parquet')
    src_model = f'/mnt/d/bootcamp-covid/model/arima_{country}.pkl'
    df = read_data(spark, src)
    fcst = forecast(df, src_model, fcst_horizon=fcst_horizon)
    return fcst

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    countries = sys.argv[3]
    
    spark = SparkSession\
                .builder\
                .appName("covid_calc_fields")\
                .getOrCreate()
    
    if ',' in countries:
        for country in countries.split(','):
            execute(spark, src, dest, country)
    else:
        execute(spark, src, dest, countries)
    
    spark.stop()

