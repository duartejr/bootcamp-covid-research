import sys
import shutil
from os.path import join, exists
from pyspark.sql import SparkSession

def save(df, dest):
    if exists(dest):
        shutil.rmtree(dest)
    df.coalesce(1).write.parquet(dest)

def calc_fields(spark, df):
    df = df.toPandas()
    columns = ['Confirmed', 'Deaths', 'Recovered']
    new_col = ['New_Confirmed', 'New_Deaths', 'New_Recovered']
    
    for i in range(len(columns)):
        df[new_col[i]] = df[columns[i]].diff()

    df = df.fillna(0)
    df['Active'] = df['Confirmed'] - df['Deaths'] - df['Recovered']
    df = df[['date', 'Country_Region', 'Confirmed', 'Deaths', 
             'Recovered', 'Active', 'New_Confirmed', 'New_Deaths', 
             'New_Recovered']]
    df = spark.createDataFrame(df)
    return df

def format_dates(spark, df):
    df.createOrReplaceTempView("tempView")
    df = spark.sql("SELECT Country_Region, "+\
                "TO_DATE(date,'MM-dd-yyyy') date, "+\
                "Confirmed, Deaths, Recovered "+\
                "FROM tempView")
    df = df.dropDuplicates()
    df = df.orderBy("date")
    return df

def read_data(spark, src):
    df = spark.read.parquet(src)
    df = format_dates(spark, df)
    return df

def execute(spark, src, dest, country):
    src = join(src, f'{country}.parquet')
    dest = join(dest, f'{country}.parquet')
    
    if exists(src):
        df = read_data(spark, src)
        df = calc_fields(spark, df)
        save(df, dest)


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
