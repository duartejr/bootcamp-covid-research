# tweet_transformation.py - Script to extract only the import data from the
# Twitter API response.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def get_tweets_data(df: "spark.dataframe") -> "spark.dataframe":
    """
    Select the important attributes in the tweets.

    Args:
        df (spark.dataframe): Spark dataframe with the tweets.

    Returns:
        spark.dataframe: Spark dataframe with the selected columns.
    """
    return df.select(f.explode("data").alias("tweets"))\
             .select("tweets.author_id",
                     "tweets.created_at",
                     "tweets.text")


def get_user_data(df: "spark.dataframe") -> "spark.dataframe":
    """
    Selects the user data information

    Args:
        df (spark.dataframe): Spark dataframe with the original tweets.

    Returns:
        spark.dataframe: Spark dataframe with the user information.
    """
    return df.select(f.explode("includes.users").alias("users"))\
             .select("users.id", "users.location")


def join_tweet_user(df_tweet: "spark.dataframe", df_user: "spark.dataframe") -> "spark.dataframe":
    """
    Joins the the user data and tweets content in a single dataframe.

    Args:
        df_tweet (spark.dataframe): Dataframe with the tweets content.
        df_user (spark.dataframe): Dataframe with the users data.

    Returns:
        spark.dataframe: A single dataframe that contains both the tweet content and user data.
    """
    return df_tweet.join(df_user,
                         df_tweet.author_id == df_user.id,
                         "inner")\
                   .select("created_at", "text", "location")\
                   .dropDuplicates()


def export_json(df: "spark.datafram", dest: str):
    """
    Exports the dataframe as a JSON file.

    Args:
        df (spark.datafram): Spark dataframe with the tweets data.
        dest (str): Destination folder.
    """
    df.coalesce(1).write.mode("overwrite").json(dest)


def twitter_transform(spark: "spark.session", src: str, dest: str,
                      process_date: str, table_name: str):
    """
    It applies the specified transformations to the tweets data.

    Args:
        spark (spark.session): A Spark session
        src (str): Source folder name
        dest (str): Destination folder
        process_date (str): The date when the tweets were extracted
        table_name (str): Name of the subdirectory destination
    """
    df = spark.read.json(src)
    tweets_df = get_tweets_data(df, process_date)
    user_df = get_user_data(df)
    twitter_df = join_tweet_user(tweets_df, user_df)
    table_dest = join(dest, table_name, f"process_date={process_date}")
    export_json(twitter_df, table_dest)


if __name__ == "__main__":
    src = sys.argv[1] # Get the source folder name
    dest = sys.argv[2] # Get the destination folder name
    process_date = sys.argv[3] # Get the process date
    table_name = sys.argv[4] # Get the table name
    
    # Initialize a Spark Session
    spark = SparkSession\
            .builder\
            .appName("twitter_transformation")\
            .getOrCreate()
    
    # Does the transformation into the tweets
    twitter_transform(spark, src, dest, process_date, table_name)
    
    # Stop the Spark session
    spark.stop()
