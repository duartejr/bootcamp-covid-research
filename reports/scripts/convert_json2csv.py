# convert_json2csv.py - Script to transform JSON files into CSV files.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import os
import csv
from pyspark.sql import SparkSession

# Start a new Spark session
spark = SparkSession\
        .builder\
        .appName("conver_json2csv")\
        .getOrCreate()

# Country name abreviations
countries = ['AR', 'MX', 'CL', 'EC', 'ES']

# Interates in each country
for country in countries:
    # Path of source folder
    src = f'../../datalake/silver/twitter/sentimental_analysis/json/{country}'
    df = spark.read.json(src) # Read json files as Spark dataframe
    df = df.na.drop() # Remove NaN values
    df = df.toPandas() # Spark to Pandas
    # Selects specific columns
    df = df[['date', 'location', 'compound', 'neg', 'neu', 'pos', 'texto', 
             'translation']]
    # Destinaton folder
    src_out = f'../../datalake/silver/twitter/sentimental_analysis/csv/{country}'
    # It creates the destination folder if it does not exist.
    if not os.path.exists(src_out):
        os.makedirs(src_out)
    # Path of the output file
    output_file = f'{src_out}/twitter_sentimental_analysis_{country}.csv'
    print(output_file)
    # Export inton CSV format
    df.to_csv(output_file, index=None, quoting=csv.QUOTE_NONNUMERIC)
