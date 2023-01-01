import sys
import shutil
import pandas as pd
from datetime import datetime
from os.path import join, exists
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as f
from statsmodels.tsa.arima.model import ARIMAResults
from pyspark.sql.types import StructType, StructField, DateType, FloatType

def forecast(obs_data, src_model, fcst_horizon=7):
    model = ARIMAResults.load(src_model)
    obs_data = obs_data.toPandas()
    if len(obs_data) > 150:
        obs_data = obs_data.iloc[len(obs_data)-150:]
    obs_data = obs_data.set_index("Date")
    model = model.apply(obs_data)
    model = model.apply(obs_data)
    fcst = model.forecast(fcst_horizon)
    fcst = fcst.values[:]
    total_fcst = fcst.sum()
    fcst = [obs_data.index[-1]] + list(fcst) + [total_fcst]
    horizons = [f'd{i}' for i in range(1, fcst_horizon+1)]
    fcst = pd.DataFrame(fcst).T
    fcst.columns = ['forecast_date'] + horizons + ['total']
    return fcst

def save(spark, df, dest):
    if exists(dest):
        df_old = spark.read.parquet(dest)
        df = df.union(df_old).toPandas()
        shutil.rmtree(dest)
        df = spark.createDataFrame(df)
    df.coalesce(1).write.parquet(dest)
    

def read_data(spark, src):
    df = spark.read.parquet(src)
    return df

def select_data(df, col):
    return df.select([f.col("Date"), f.col(col)])

def pd_to_spark(spark, df):
    content = df.values.tolist()[0]
    data = [
        Row(date=content[0],
            value1=float(content[1]),
            value2=float(content[2]),
            value3=float(content[3]),
            value4=float(content[4]),
            value5=float(content[5]),
            value6=float(content[6]),
            value7=float(content[7]),
            value8=float(content[8]))
        ]
    schema = StructType([
        StructField("forecast_date", DateType(), True),
        StructField("d1", FloatType(), True),
        StructField("d2", FloatType(), True),
        StructField("d3", FloatType(), True),
        StructField("d4", FloatType(), True),
        StructField("d5", FloatType(), True),
        StructField("d6", FloatType(), True),
        StructField("d7", FloatType(), True),
        StructField("total", FloatType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    return df
    

def execute(spark, src, dest, country, fcst_horizon=7, col="New cases"):
    src = join(src, f'{country}.parquet')
    dest = join(dest, f'{country}.parquet')
    src_model = f'/mnt/d/bootcamp-covid/model/arima_{country}.pkl'
    df = read_data(spark, src)
    df = select_data(df, col)
    fcst = forecast(df, src_model, fcst_horizon=fcst_horizon)
    fcst = pd_to_spark(spark, fcst)
    save(spark, fcst, dest)
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

