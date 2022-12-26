import sys
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime as dt

def select_country(df, country):
    return df.filter(df.Country_region == country)

def select_columns(df):
    df = df.select(f.col("Country_Region"),
                   f.col("date"),
                   f.col("Confirmed").cast("double"),
                   f.col("Deaths").cast("double"),
                   f.col("Recovered").cast("double"),
                   f.col("Active").cast("double"),
                   f.col("Incident_Rate").cast("double"),
                   f.col("Case_Fatality_Ratio").cast("double"))
    return df


def group_data(spark, df):
    df.createOrReplaceTempView("tempView")
    df = spark.sql("select date, Country_Region, "+\
                   "sum(Confirmed) as Confirmed, "+\
                   "sum(Deaths) as Deaths, "+\
                   "sum(Recovered) as Recovered, "+\
                   "sum(Active) as Active, "+\
                   "avg(Incident_Rate) as Incident_Rate, "+\
                   "avg(Case_Fatality_Ratio) as Case_Fatality_Ratio "+\
                   "from try group by date, Country_Region")
    return df                                           

def save_time_series(df, dest):
    df.coalesce(1).write("append").parquet(dest)

def execute(spark, src, dest, extract_date, country):
    extract_date = dt.strftime(extract_date, "%Y-%m-%d")
    in_file = join(src, f"extract_date={extract_date}")
    out_file = join(dest, "time_series", country)
    
    df = spark.read.option("header", True)\
                   .csv(in_file)
    
    df = select_country(df, country)
    df = select_columns(df)
    
    if df.count() > 1:
        df = group_data(spark, df)
        
    save_time_series(df, out_file)
    

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.arg[2]
    extract_date = sys.argv[3]
    country = sys.argv[4]
    
    spark = SparkSession\
            .builder\
            .appName("covid_transform")\
            .getOrCreate()
    
    execute(spark, src, dest, extract_date)
    spark.stop()
