import sys
import shutil
from os.path import join, exists
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime as dt
from datetime import timedelta

def select_country(df, country):
    return df.filter(df.Country_Region == country)

def select_columns(spark, df):
    df.createOrReplaceTempView("tempView")
    df = spark.sql("SELECT Country_Region, date, "+\
                   "CAST(Confirmed AS DOUBLE) Confirmed, "+\
                   "CAST(Deaths AS DOUBLE) Deaths, "+\
                   "CAST(Recovered AS DOUBLE) Recovered, "+\
                   "CAST(Incident_Rate AS DOUBLE) Incident_Rate, "+\
                   "CAST(Case_Fatality_Ratio AS DOUBLE) Case_Fatality_Ratio "+\
                   "FROM tempView")
    return df

def group_data(spark, df):
    df = spark.sql("SELECT date, Country_Region, "+\
                   "SUM(Confirmed) AS Confirmed, "+\
                   "SUM(Deaths) AS Deaths, "+\
                   "SUM(Recovered) AS Recovered, "+\
                   "SUM(Active) AS Active, "+\
                   "AVG(Incident_Rate) AS Incident_Rate, "+\
                   "AVG(Case_Fatality_Ratio) AS Case_Fatality_Ratio "+\
                   "FROM tempView GROUP BY date, Country_Region")
    return df                                           

def save_time_series(spark, df, dest):
    if exists(dest):
        df_old = spark.read.parquet(dest)
        df = df.union(df_old).toPandas()
        shutil.rmtree(dest)
        df = spark.createDataFrame(df)
    df.coalesce(1).write.mode("append").parquet(dest)

def execute(spark, src, dest, country, country_abrv):
    out_file = join(dest, f"{country_abrv}.parquet")
    df = spark.read.option("header", True).csv(src)
    df = select_country(df, country)
    df = select_columns(spark, df)
    if df.count() > 1:
        df = group_data(spark, df)
    save_time_series(spark, df, out_file)
    

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    extract_date = sys.argv[3]
    countries = sys.argv[4]
    countries_abrv = sys.argv[5].split(',')
    start_date = dt(2021, 1, 1)
    end_date = dt(2022, 12, 25)
    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]
    
    spark = SparkSession\
            .builder\
            .appName("covid_transform")\
            .getOrCreate()

    # execute(spark, src, dest, country, countries_abrv[i])
    
    for date in dates:
        extract_date = dt.strftime(date, "%Y-%m-%d")
        src2 = join(src, f"extract_date={extract_date}")
        for i, country in enumerate(countries.split(",")):
            execute(spark, src2, dest, country, countries_abrv[i])
    
    spark.stop()
