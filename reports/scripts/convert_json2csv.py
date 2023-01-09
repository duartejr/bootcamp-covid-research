from pyspark.sql import SparkSession
import os
import csv

spark = SparkSession\
        .builder\
        .appName("twitter_sentimental_analysis")\
        .getOrCreate()
countries = ['AR', 'MX', 'CL', 'EC', 'ES']

for country in countries:
    src = f'../../datalake/silver/twitter/sentimental_analysis/json/{country}'
    df = spark.read.json(src)
    df = df.na.drop()
    df = df.toPandas()
    df = df[['date', 'location', 'compound', 'neg', 'neu', 'pos', 'texto', 'translation']]
    src_out = f'../../datalake/silver/twitter/sentimental_analysis/csv/{country}'
    if not os.path.exists(src_out):
        os.makedirs(src_out)
    output_file = f'{src_out}/twitter_sentimental_analysis_{country}.csv'
    print(output_file)
    df.to_csv(output_file, index=None, quoting=csv.QUOTE_NONNUMERIC)

# spark.exit()