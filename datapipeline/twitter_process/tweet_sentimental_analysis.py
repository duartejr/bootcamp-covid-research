# tweet_sentimental_analysis.py - Script to make sentimental analysis in tweets.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
import translators as ts
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from nltk.sentiment import SentimentIntensityAnalyzer


def translate(spark: "spark.session", df: "spark.dataframe", w: "window") -> "spark.dataframe":
    """
    This function translates the tweets using the Google Translator API.

    Args:
        sapark (spark.session): A Spark Session
        df (spark.dataframe): Spark dataframe with the tweets to translate.
        w (window): A spark window object.

    Returns:
        spark.dataframe: Spark dataframe with the tweets translated
    """
    ans = []
    i=0
    # Loop to interante in the tweets
    for tweet in df.toPandas()['texto']:
        # Translates the text from any language to English
        ans.append({'translation': ts.google(tweet, to_language='en')})
        i+=1
    
    # Create a new Spark dataframe with the translated tweets.
    ans = spark.createDataFrame(ans)
    df2 = df.withColumn('row_id', f.row_number().over(w))
    ans = ans.withColumn('row_id', f.row_number().over(w))

    # Concatenate the original dataframe with the new one (tranlations) and
    # removes unnecessary columns from the dataframe.
    df2 = df2.join(ans, on=['row_id'])\
             .drop('document')\
             .drop('sentence')
    
    return df2


def sentiment_analysis(spark: "spark.session", df: "spark.datafram", w: "window") -> "spark.dataframe":
    """
    It uses natural language processing techniques and a pre-trained sentiment 
    analysis model to analyze the text of each tweet and assign a sentiment 
    score. The function then adds the sentiment scores as columns to the input 
    dataframe, indicating the overall polarity and intensity of the tweets for 
    each country.

    Args:
        spark (spark.session): A Spark Session.
        df (spark.datafram): Spark dataframe with the tweets
        w (window): A spark window object.

    Returns:
        spark.dataframe: Spark dataframe with the sentiment analysis results.
    """
    # Initialize the sentiment analyser. Here is used the VADER algorithm.
    sentiment_analyser = SentimentIntensityAnalyzer() 
    
    # Excecute the sentiment analysiser and extract sentiments from each tweet.
    sentiments = []
    for tweet in df.toPandas()['translation']:
        sentiments.append(sentiment_analyser.polarity_scores(tweet))

    # Creates a new dataframe to storage the sentiment results.
    sentiments = spark.createDataFrame(sentiments)
    sentiments = sentiments.withColumn('row_id', f.row_number().over(w))
    
    # Combine the sentiments dataframe with que original data
    df2 = df.join(sentiments, on=['row_id'])
    
    return df2


def prep_text(df: "spark.dataframe") -> "spark.dataframe":
    """
    It selects the columns that are relevant to the sentiment analysis process 
    and discards any unnecessary data.

    Args:
        df (spark.dataframe): Spark dataframe with the tweets.

    Returns:
        spark.dataframe: Spark dataframe with the necessary columns.
    """
    text = df.select('created_at', 
                     f.regexp_replace(f.lower(f.col('text')),
                                      "[^A-Za-z0-9À-ÿ\-\ @#-]", "")\
                      .alias("texto"))
    df2 = df.join(text, on=['created_at'])\
            .drop('text')
    return df2


def export_json(df: "spark.dataframe", dest: "str"):
    """
    It saves the spark dataframe in a JSON format in the specified destination 
    folder.

    Args:
        df (spark.dataframe): Spark dataframe with the data
        dest (str): Destination folder
    """
    df.coalesce(1).write.mode('overwrite').json(dest)


def tweet_sentiment_analysis(spark: "spark.session", window: "window", src: str,
                             dest: str, process_date: str):
    """
    It executes thw tweet sentiment analysis.

    Args:
        spark (spark.session): A Spark Session.
        window (window): Spark window object
        src (str): Source folder with the original data.
        dest (str): Destination folder
        process_date (str): The date when the tweet were collected.
    """
    # Read the original data from the source folder.
    src = join(src, f"process_date={process_date}")
    df = spark.read.json(src)
    
    # If the datafram is empty stop the process.
    if df.rdd.isEmpty():
        return None
    
    # Does the sentiment analysis, 
    df = sentiment_analysis(spark,
                            translate(spark,
                                      prep_text(df),
                                      window), 
                            window)
    
    # Rename the columns created_at to just "date"
    df = df.withColumnRenamed("created_at", "date")
    
    # Creates the final dataframe
    table_dest = join(dest, f"process_date={process_date}")
    
    # Export the transformed data
    export_json(df, table_dest)
    

if __name__ == "__main__":
    src = sys.argv[1] # Get the source folder name
    dest = sys.argv[2] # Get the destination folder name
    process_date = sys.argv[3] # Get the process date
    
    # Initialize a Spark Session
    spark = SparkSession\
            .builder\
            .appName("twitter_sentiments_analysis")\
            .getOrCreate()
    
    window = Window().orderBy(f.lit('A')) # Spark window object

    # Exececutes the sentiment analysis
    tweet_sentiment_analysis(spark, window, src, dest, process_date)
    
    # Stop the Spark Session
    spark.stop()
