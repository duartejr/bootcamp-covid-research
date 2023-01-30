# tweet_sentiment_analysis v1 - 
# This script aims to extract sentiments from tweets using the SparkNLP library,
# which contains advanced NLP algorithms. However, it requires a lot of 
# computational resources, making it only suitable for use on supercomputers 
# with more than 10 GB of memory and high computational power. As a result, this 
# script was not used in the final project and a different approach was taken for 
# sentiment analysis, as described in the Twitter Process section.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.

import sparknlp
import pandas as pd
from calendar import monthrange
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from sparknlp.pretrained import PretrainedPipeline 
from nltk.sentiment import SentimentIntensityAnalyzer

# This test is runing for the year 2000
year = 2020

# An this is runing just from Marh to June.
for month in range(3, 7):
    start_day = 1 # First day of the month.
    end_day = monthrange(year, month)[1] # Last day of the month
    print(month)
    
    # This iterates in each day of the month
    for day in range(start_day, end_day+1):
        print(day)
        for country in ['AR', 'ES', 'MX', 'CL', 'EC']:
            print(country)
            
            # It stars a new SparkNLP session
            spark_nlp = sparknlp.start()
            
            # The next line of code creates a new instance of a translator a
            # vailable in SparkNLP.
            # The use of a language translator is important in sentiment analysis, 
            # as the sentiment of a tweet may be affected by the language it is 
            # written in. The translator is used to translate the tweets from 
            # any language to English, allowing for easier sentiment analysis. 
            # The translator uses the same engine as the Bing Translator, which 
            # is considered to be one of the best language translators available.
            translator = PretrainedPipeline("translate_mul_en", lang = "xx")
            
            # It instaciates a new object of the SentimentAnalyzer available
            # in SparkNLP.
            sentimentl_analyser = SentimentIntensityAnalyzer()
            
            # It creats a window just to order the Spark dataframe.
            w = Window().orderBy(f.lit('A'))
            src = f'../../datalake/silver/twitter/covid/{country}' # Path of the data files.
            
            # Read and create the tweets dataframe.
            df = spark_nlp.read.json(src)
            df = df.withColumn('date', 
                   df.created_at.substr(1, 10))
            df = df.withColumn('date', f.to_date(df.date))
            df2 = df.filter(
                    f.col("date")\
                     .between(pd.to_datetime(f'{year}-{month}-{day}'),
                              pd.to_datetime(f'{year}-{month}-{day}')))
            
            # If has no data into the path it skips to the next iteration.
            if df2.rdd.isEmpty():
                continue
            
            # It selects the text of the tweets.
            text = df2.select('created_at', 
                              f.regexp_replace(f.lower(f.col('text')),
                                               "[^A-Za-z0-9À-ÿ\-\ @#-]", ""
                                               ).alias("texto")
                              )
            
            # it inserts the text cleaned in the dataframe.
            df2 = df2.join(text, on=['created_at'])\
                    .drop('created_at')\
                    .drop('text')\
                    .drop('process_date')

            print('len', df2.count())
            
            # The next loop performs the translation of the tweets.
            ans = []
            for tweet in df2.toPandas()['texto']:
                ans.append(translator.annotate(tweet))
            
            # It creates a new dataframe to store the translations
            ans = spark_nlp.createDataFrame(ans)
            ans = ans.withColumn('translation', f.concat_ws('', 'translation'))
            df2 = df2.withColumn('row_id', f.row_number().over(w))
            ans = ans.withColumn('row_id', f.row_number().over(w))

            # It inserts the translations into the original dataframe.
            df2 = df2.join(ans, on=['row_id'])\
                     .drop('document')\
                     .drop('sentence')

            # The next loop performs the sentiment analyse.
            sentimentals = []
            for tweet in df2.toPandas()['translation']:
                sentimentals.append(sentimentl_analyser.polarity_scores(tweet))

            # It creates a new dataframe to store the sentiment results.
            sentimentals = spark_nlp.createDataFrame(sentimentals)
            sentimentals = sentimentals.withColumn('row_id', f.row_number().over(w))

            # It inserts the sentiments results into the original dataframe.
            df2 = df2.join(sentimentals, on=['row_id'])
            
            # The next lines of code configures the name of the output file.
            date = f'process_date={year}-{month:02d}-{day:02d}'
            dest = f'../../datalake/silver/twitter/sentimental_analysis/{country}/{date}'
            df2.coalesce(1).write.mode("overwrite").json(dest)
            dest_csv = f'../../datalake/gold/twitter/sentimental_analysis/{country}/{date}.csv'
            df2.toPandas().to_csv(dest_csv, index=False)
            
            # It is necessary to delete the variables to clean the memory.
            del df2, sentimentals, ans, text, df, sentimentl_analyser, translator
            
            spark_nlp.stop() # Stop the SparkNLP session
