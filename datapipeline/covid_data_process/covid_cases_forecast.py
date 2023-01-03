import sys
import shutil
import numpy
import pandas as pd
from datetime import timedelta
from os.path import join, exists
from pyspark.sql import functions as f
from pyspark.sql import SparkSession, Row
from statsmodels.tsa.arima.model import ARIMAResults
from pyspark.sql.types import StructType, StructField, DateType, FloatType


def forecast(obs_data, src_model, fcst_horizon=7, format_BI=True, fcst_date=None):
    model = ARIMAResults.load(src_model)
    obs_data = obs_data.toPandas()
    obs_data = obs_data.set_index("date")
    
    if fcst_date:
        obs_data = obs_data.loc[:fcst_date]

    if len(obs_data) > 150:
        obs_data = obs_data.iloc[len(obs_data)-150:]
    
    model = model.apply(obs_data)
    fcst = model.forecast(fcst_horizon)
    if format_BI:
        fcst = fcst.to_frame().reset_index()
        fcst['forecast_date'] = obs_data.index[-1]
        fcst.columns = ['forecasted_date', 'forecast', 'forecast_date']
        fcst = fcst[['forecast_date', 'forecasted_date', 'forecast']]
        if type(fcst.forecasted_date[0]) == numpy.int64:
            forecasted_dates = [fcst.forecast_date[i] + timedelta(days=i+1) for i in range(len(fcst))]
            fcst['forecasted_date'] = forecasted_dates
    else:
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
        df = df.dropDuplicates()
    df.coalesce(1).write.parquet(dest)
    
def read_data(spark, src):
    df = spark.read.parquet(src)
    return df

def select_data(df, col):
    return df.select([f.col("date"), f.col(col)])

def pd_to_spark(spark, df, format_BI=True):
    if format_BI:
        df = spark.createDataFrame(df)
        return df
    else:
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
    
def execute(spark, src, dest, country, fcst_horizon=7, col="New_Confirmed",
            fcst_date=None, format_BI=True):
    src = join(src, f'{country}.parquet')
    dest = join(dest, f'{country}.parquet')
    if exists(src):
        src_model = f'/mnt/d/bootcamp-covid/model/arima_{country}.pkl'
        df = read_data(spark, src)
        df = select_data(df, col)
        fcst = forecast(df, src_model, fcst_horizon=fcst_horizon,
                        fcst_date=fcst_date,
                        format_BI=format_BI)
        fcst = pd_to_spark(spark, fcst)
        print(fcst)
        save(spark, fcst, dest)
        print('dest_forecast:', dest)


if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    countries = sys.argv[3]
    
    spark = SparkSession\
                .builder\
                .appName("covid_cases_forecast")\
                .getOrCreate()
    
    if ',' in countries:
        for country in countries.split(','):
            execute(spark, src, dest, country)
    else:
        execute(spark, src, dest, countries)
    
    spark.stop()
