import sys
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime as dt

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
    df = spark.sql("select date, Country_Region, "+\
                   "sum(Confirmed) as Confirmed, "+\
                   "sum(Deaths) as Deaths, "+\
                   "sum(Recovered) as Recovered, "+\
                   "sum(Active) as Active, "+\
                   "avg(Incident_Rate) as Incident_Rate, "+\
                   "avg(Case_Fatality_Ratio) as Case_Fatality_Ratio "+\
                   "from tempView group by date, Country_Region")
    return df                                           

def save_time_series(df, dest):
    df.coalesce(1).write.mode("append").parquet(dest)

def execute(spark, src, dest, extract_date, country):
    out_file = join(dest, country)
    
    df = spark.read.option("header", True)\
                   .csv(src)
    
    df = select_country(df, country)
    df = select_columns(spark, df)
    
    if df.count() > 1:
        df = group_data(spark, df)
        
    save_time_series(df, out_file)
    

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    extract_date = sys.argv[3]
    countries = sys.argv[4]
    
    spark = SparkSession\
            .builder\
            .appName("covid_transform")\
            .getOrCreate()

    for country in countries.split(","):
        execute(spark, src, dest, extract_date, country)
    
    spark.stop()
