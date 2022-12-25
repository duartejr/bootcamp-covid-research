from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from nltk.sentiment import SentimentIntensityAnalyzer
import sys
from calendar import monthrange
import pandas as pd
import translators as ts
from os.path import join


def translate(df, w):
    ans = []
    i=0
    for tweet in df.toPandas()['texto']:
        ans.append({'translation': ts.google(tweet, to_language='en')})
        i+=1
    
    ans = spark.createDataFrame(ans)
    df2 = df.withColumn('row_id', f.row_number().over(w))
    ans = ans.withColumn('row_id', f.row_number().over(w))

    df2 = df2.join(ans, on=['row_id'])\
             .drop('document')\
             .drop('sentence')
    return df2


def sentiment_analysis(df, w):
    sentiment_analyser = SentimentIntensityAnalyzer()
    sentiments = []
    for tweet in df.toPandas()['translation']:
        sentiments.append(sentiment_analyser.polarity_scores(tweet))

    sentiments = spark.createDataFrame(sentiments)
    sentiments = sentiments.withColumn('row_id', f.row_number().over(w))
    df2 = df.join(sentiments, on=['row_id'])
    return df2

# def create_date_column(df):
#     df = df.withColumn('date',
#                        df.created_at.substr(1, 10))
#     df = df.withColumn('date', f.to_date(df.date))
#     df2 = df.filter(f.col("date")\
#                      .between(pd.to_datetime(f'{year}-{month}-{day}'),
#                               pd.to_datetime(f'{year}-{month}-{day}')))
#     return df2


def prep_text(df):
    text = df.select('created_at', 
                     f.regexp_replace(f.lower(f.col('text')),
                                      "[^A-Za-z0-9À-ÿ\-\ @#-]", "")\
                      .alias("texto"))
    df2 = df.join(text, on=['created_at'])\
            .drop('created_at')\
            .drop('text')\
            .drop('process_date')
    return df2


def export_json(df, dest):
    df.coalesce(1).write.mode('overwrite').json(dest)


def tweet_sentiment_analysis(spark, window, src, dest, process_date, table_name):
    df = spark.read.json(src)
    # df2 = create_date_column(df)
            
    if df.rdd.isEmpty():
        return None
    
    df = sentiment_analysis(translate(prep_text(df), window), window)
    table_dest = join(dest, table_name, f"process_date={process_date}")
    export_json(df, table_dest)
    

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    process_date = sys.argv[3]
    table_name = sys.argv[4]
    
    spark = SparkSession\
            .builder\
            .appName("twitter_sentiments_analysis")\
            .getOrCreate()
    window = Window().orderBy(f.lit('A'))

    tweet_sentiment_analysis(spark, window, src, dest, process_date, table_name)
    spark.stop()
    