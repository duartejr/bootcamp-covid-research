from pyspark.sql import SparkSession
import os

spark = SparkSession\
        .builder\
        .appName("twitter_sentimental_analysis")\
        .getOrCreate()
countries = ['AR', 'MX', 'CL', 'EC', 'ES']

for country in countries:
    src = f'../../datalake/silver/twitter/sentimental_analysis/{country}'
    df = spark.read.json(src)
    df = df.toPandas()
    df = df[['date', 'location', 'texto', 'translation', 'compound',
             'neg', 'neu', 'pos']]
    src_out = f'../../datalake/silver/twitter/sentimental_analysis/csv/{country}'
    if not os.path.exists(src_out):
        os.makedirs(src_out)
    output_file = f'{src_out}/twitter_sentimental_analysis_{country}.csv'
    print(output_file)
    df.to_csv(output_file, index=None)