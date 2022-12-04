from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
from calendar import monthrange
import pandas as pd
import translators as ts

year = 2020
spark = SparkSession\
        .builder\
        .appName("twitter_sentimental_analysis")\
        .getOrCreate()
sentimental_analyser = SentimentIntensityAnalyzer()
w = Window().orderBy(f.lit('A'))
        
for month in range(6, 7):
    if month == 6:
        start_day = 25
    else:
        start_day = 1
    
    end_day = monthrange(year, month)[1]
    print(month)
    
    for day in range(start_day, end_day+1):
        print(day)
        for country in ['MX']:
            if month == 7 and day == 20 and country in ['AR', 'ES', 'MX', 'CL']:
                continue
            print(country)
            
            src = f'../../datalake/silver/twitter/covid/{country}'
            df = spark.read.json(src)
            df = df.withColumn('date', 
                   df.created_at.substr(1, 10))
            df = df.withColumn('date', f.to_date(df.date))
            df2 = df.filter(
                    f.col("date")\
                     .between(pd.to_datetime(f'{year}-{month}-{day}'),
                              pd.to_datetime(f'{year}-{month}-{day}')))
            #del df
            if df2.rdd.isEmpty():
                continue
            text = df2.select('created_at', 
                              f.regexp_replace(
                                f.lower(
                                    f.col('text')
                                ),
                                "[^A-Za-z0-9À-ÿ\-\ @#-]", ""
                                ).alias("texto")
                            )
            df2 = df2.join(text, on=['created_at'])\
                    .drop('created_at')\
                    .drop('text')\
                    .drop('process_date')

            print('len', df2.count())
            ans = []
            i=0
            for tweet in df2.toPandas()['texto']:
                ans.append({'translation': ts.google(tweet, to_language='en')})
                print(i)
                i+=1
            # print(ans)
            # print(type(ans))
                
            ans = spark.createDataFrame(ans)
            # ans = ans.withColumn('translation', f.concat_ws('', 'translation'))

            df2 = df2.withColumn('row_id', f.row_number().over(w))
            ans = ans.withColumn('row_id', f.row_number().over(w))

            df2 = df2.join(ans, on=['row_id'])\
                     .drop('document')\
                     .drop('sentence')

            sentimentals = []
            
            for tweet in df2.toPandas()['translation']:
                sentimentals.append(sentimental_analyser.polarity_scores(tweet))

            sentimentals = spark.createDataFrame(sentimentals)
            sentimentals = sentimentals.withColumn('row_id', f.row_number().over(w))

            df2 = df2.join(sentimentals, on=['row_id'])
            date = f'process_date={year}-{month:02d}-{day:02d}'
            dest = f'../../datalake/silver/twitter/sentimental_analysis/{country}/{date}'
            df2.coalesce(1).write.mode("overwrite").json(dest)
            dest_csv = f'../../datalake/gold/twitter/sentimental_analysis/{country}/{date}.csv'
            df2.toPandas().to_csv(dest_csv, index=False)
            #del df2, sentimentals, ans
            # del df2, sentimentals, ans, text, df, sentimentl_analyser, translator
spark.stop()
