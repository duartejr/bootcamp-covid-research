import sys
import shutil
from os.path import join, exists
from datetime import datetime as dt
from pyspark.sql import SparkSession


def select_country(df, country):
    return df.filter(df.Country_Region == country)

def select_columns(spark, df):
    df.createOrReplaceTempView("tempView")
    
    try:
        df = spark.sql("SELECT Country_Region, date, "+\
                    "CAST(Confirmed AS DOUBLE) Confirmed, "+\
                    "CAST(Deaths AS DOUBLE) Deaths, "+\
                    "CAST(Recovered AS DOUBLE) Recovered "+
                    "FROM tempView")
    except:
        df = df.withColumnRenamed("Country/Region", "Country_Region")
        df.createOrReplaceTempView("tempView")
        df = spark.sql("SELECT Country_Region, date, "+\
                    "CAST(Confirmed AS DOUBLE) Confirmed, "+\
                    "CAST(Deaths AS DOUBLE) Deaths, "+\
                    "CAST(Recovered AS DOUBLE) Recovered "+
                    "FROM tempView")
    return df

def group_data(spark, df):
    df.createOrReplaceTempView("tempView")
    df = spark.sql("SELECT Country_Region, date, "+\
                   "SUM(Confirmed) AS Confirmed, "+\
                   "SUM(Deaths) AS Deaths, "+\
                   "SUM(Recovered) AS Recovered "+\
                   "FROM tempView GROUP BY date, Country_Region")
    return df                                           

def save_time_series(spark, df, dest):
    if exists(dest):
        df_old = spark.read.parquet(dest)
        df = df.union(df_old).toPandas()
        shutil.rmtree(dest)
        df = spark.createDataFrame(df)
        df = df.dropDuplicates()
    df.coalesce(1).write.mode("append").parquet(dest)

def add_empty_row(spark, df, date, country):
    df_collect = df.collect()
    df_collect.append({"Country_Region": country, "date": date, 
                       "Confirmed":0.0, "Deaths":0.0, "Recovered":0.0})
    new_df = spark.createDataFrame(df_collect)
    new_df.createOrReplaceTempView("tempView")
    new_df = spark.sql("SELECT Country_Region, date, "+\
                    "CAST(Confirmed AS DOUBLE) Confirmed, "+\
                    "CAST(Deaths AS DOUBLE) Deaths, "+\
                    "CAST(Recovered AS DOUBLE) Recovered "+
                    "FROM tempView")
    return new_df

def execute(spark, src, dest, country, country_abrv):
    out_file = join(dest, f"{country_abrv}.parquet")
    df = spark.read.option("header", True).csv(src)
    df = select_columns(spark, df)
    df = select_country(df, country)
       
    if df.count() > 1:
        df = group_data(spark, df)
    
    if df.count() == 0:
        date = dt.strftime(dt.strptime(src.split("=")[-1], '%Y-%m-%d'), '%m-%d-%Y')
        df = add_empty_row(spark, df, date, country)
    
    save_time_series(spark, df, out_file)
    

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    extract_date = sys.argv[3]
    countries = sys.argv[4]
    countries_abrv = sys.argv[5].split(',')

    spark = SparkSession\
            .builder\
            .appName("covid_transform")\
            .getOrCreate()

    if ',' in countries:
        for i, country in enumerate(countries.split(',')):
            execute(spark, src, dest, country, countries_abrv[i])
    else:
        execute(spark, src, dest, countries, countries_abrv)
    
    spark.stop()
