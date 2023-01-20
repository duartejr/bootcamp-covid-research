import sys
import csv
import shutil
import pandas as pd
from os.path import join, exists
from pyspark.sql import functions as f
from pyspark.sql import SparkSession, Row
from statsmodels.tsa.arima.model import ARIMAResults
from pyspark.sql.types import StructType, StructField, DateType, FloatType
import nltk

stopword = nltk.corpus.stopwords.words('spanish')


map_countries = {'AR':1, 'CL':2, 'EC':3, 'ES':4, 'MX':5}

def remove_stopwords(text):
    text = [word for word in text.lower().split(" ") if word not in stopword]
    text = " ".join(text).replace('covid', '').replace('19', '')
    return text

def save(df, dest):
    df = df.toPandas()
    df['country'] = df['country'].replace(map_countries)
    df['texto'] = [remove_stopwords(text) for text in df['texto']]
    df['date'] = [date[:10] for date in df['date']]
    df.to_csv(dest, index=False, quoting=csv.QUOTE_NONNUMERIC)
    
def read_data(spark, src):
    df = spark.read.json(src)
    return df

def order_columns_tweet(df):
    df = df.orderBy('date')
    df = df.select('date', 'country', 'texto', 'neg', 'neu', 'pos', 'compound')
    return df

def execute(spark, src, dest, countries):
    
    for i, country in enumerate(countries):
        src_data = join(src, f'{country}')
        dest_data = join(dest, f'tweet_sentiments_{country}.csv')
        print(src_data)
        print(dest_data)
        df = read_data(spark, src_data)
        df = df.withColumn("country", f.lit(country))
        df = order_columns_tweet(df)
        save(df, dest_data)
        
        if i == 0:
            df_tot = df
        else:
            df_tot = df_tot.union(df)
    
    dest_data_tot = join(dest, 'tweet_sentiments_all.csv')
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