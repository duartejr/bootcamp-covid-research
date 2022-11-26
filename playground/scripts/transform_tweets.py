import argparse
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def get_tweets_data(df, date_limit):
    return df.select(
            f.explode("data").alias("tweets")
        ).select(
            "tweets.author_id",
            "tweets.created_at",
            "tweets.text"
        ).filter(f.col("created_at") < f.lit(date_limit))

def get_user_data(df):
    return df.select(
            f.explode("includes.users").alias("users")
        ).select(
            "users.id", "users.location"
        )

def join_tweet_user(df_tweet, df_user):
    return df_tweet.join(df_user,
                         df_tweet.author_id == df_user.id,
                         "inner")\
                   .select("created_at", "text", "location")\
                   .dropDuplicates()

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

def twitter_transform(spark, src, dest, process_date, table_name):
    df = spark.read.json(src)

    tweets_df = get_tweets_data(df, process_date)
    user_df = get_user_data(df)
    twitter_df = join_tweet_user(tweets_df, user_df)

    table_dest = join(dest, table_name, f"process_date={process_date}")

    export_json(twitter_df, table_dest)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    parser.add_argument('--table-name', required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()
        
    twitter_transform(spark, args.src, args.dest, args.process_date,  args.table_name)
    print('finished')
    