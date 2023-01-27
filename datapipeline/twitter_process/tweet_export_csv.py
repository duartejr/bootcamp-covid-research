# tweet_export_csv.py - Script export the tweets, without stopwords, as CSV file.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
import csv
import nltk
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Control variables
STOPWORDS = nltk.corpus.stopwords.words('spanish') # Get list of Spanish stopwords.
MAP_COUNTRIES = {'AR':1, 'CL':2, 'EC':3, 'ES':4, 'MX':5}


def remove_stopwords(text: str) -> str:
    """
    This function removes stopwords from the given text.

    Args:
        text (str): Text, in spanish, you want to remove the stopwords

    Returns:
        str: Return a string without the stopwords.
    """
    text = [word for word in text.lower().split(" ") if word not in STOPWORDS]
    text = " ".join(text).replace('covid', '').replace('19', '')
    return text


def save(df: "spark.DataFrame", dest: str):
    """
    This function saves the dataframe into the specified destination folder in
    CSV format.

    Args:
        df (spark.DataFrame): Dataframe with the data.
        dest (str): Destination folder.
    """
    # Convert spark dataframe to pandas dataframe.
    df = df.toPandas()
    
    # Replace the country names with the codes in MAP_COUNTRIES
    df['country'] = df['country'].replace(MAP_COUNTRIES)
    
    # Remove the stopwords
    df['texto'] = [remove_stopwords(text) for text in df['texto']]
    
    # Formating date to keep only year-month-day
    df['date'] = [date[:10] for date in df['date']]
    
    # Export CSV
    df.to_csv(dest, index=False, quoting=csv.QUOTE_NONNUMERIC)
    

def read_data(spark: "spark.session", src: str) -> "spark.dataframe":
    """
    This function reads the data into source folder and returns a spark dataframe.

    Args:
        spark (spark.session): A spark session
        src (str): Source folder

    Returns:
        spark.dataframe: Dataframe with the data
    """
    df = spark.read.json(src) # Read all JSON files in the source folder.
    return df


def order_columns_tweet(df: "spark.dataframe") -> "spark.dataframe":
    """
    This function sorts the dataframe by the date column.

    Args:
        df (spark.dataframe): Spark dataframe with the data.

    Returns:
        spark.dataframe: Spark dataframe ordered by date column.
    """
    df = df.orderBy('date') # Order by column date
    # Select the specified columns
    df = df.select('date', 'country', 'texto', 'neg', 'neu', 'pos', 'compound')
    return df


def execute(spark: "spark.session", src: str, dest: str, countries: list):
    """
    This function performs the necessary processing on the tweets and then 
    exports the resulting data.

    Args:
        spark (Spark.session): A Spark session.
        src (str): Source folder of data.
        dest (str): Destination folder.
        countries (list): A list with the country names.
    """
    # Loop that interates in each country.
    for i, country in enumerate(countries):
        # Configure the name of the data folders
        src_data = join(src, f'{country}')
        dest_data = join(dest, f'tweet_sentiments_{country}.csv')

        df = read_data(spark, src_data)
        df = df.withColumn("country", f.lit(country))
        df = order_columns_tweet(df)
        save(df, dest_data)
        
        # Combine all the tweets in an unique dataframe
        if i == 0:
            df_tot = df
        else:
            df_tot = df_tot.union(df)
    
    # Exports all dataframes in an unique file
    dest_data_tot = join(dest, 'tweet_sentiments_all.csv')
    save(df_tot, dest_data_tot)


if __name__ == "__main__":
    src = sys.argv[1] # Get source folder name
    dest = sys.argv[2] # Get destination folder name
    countries = sys.argv[3].split(',') # Get list of country names
    
    # Creates a spark session
    spark = SparkSession\
            .builder\
            .appName("covid_export_csv")\
            .getOrCreate()

    # Execute the text processing
    execute(spark, src, dest, countries)
    
    # Ends the Spark session
    spark.stop()