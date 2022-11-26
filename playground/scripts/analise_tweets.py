# from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import sparknlp
from sparknlp.pretrained import PretrainedPipeline 
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
from calendar import monthrange
import pandas as pd

year = 2020

for month in range(3, 7):
    if month == 3:
        start_day = 19
    else:
        start_day = 1
    
    end_day = monthrange(year, month)[1]
    print(month)
    
    for day in range(start_day, end_day+1):
        print(day)
        for country in ['AR', 'ES', 'MX', 'CL', 'EC']:
            if month == 3 and day == 19 and country in ['AR', 'ES', 'MX']:
                continue
            print(country)
            spark_nlp = sparknlp.start()
            translator = PretrainedPipeline("translate_mul_en", lang = "xx")
            sentimentl_analyser = SentimentIntensityAnalyzer()
            w = Window().orderBy(f.lit('A'))
            src = f'../../datalake/silver/twitter/covid/{country}'
            df = spark_nlp.read.json(src)
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
            for tweet in df2.toPandas()['texto']:
                ans.append(translator.annotate(tweet))
            ans = spark_nlp.createDataFrame(ans)
            ans = ans.withColumn('translation', f.concat_ws('', 'translation'))

            df2 = df2.withColumn('row_id', f.row_number().over(w))
            ans = ans.withColumn('row_id', f.row_number().over(w))

            df2 = df2.join(ans, on=['row_id'])\
                     .drop('document')\
                     .drop('sentence')

            sentimentals = []
            for tweet in df2.toPandas()['translation']:
                sentimentals.append(sentimentl_analyser.polarity_scores(tweet))

            sentimentals = spark_nlp.createDataFrame(sentimentals)
            sentimentals = sentimentals.withColumn('row_id', f.row_number().over(w))

            df2 = df2.join(sentimentals, on=['row_id'])
            date = f'process_date={year}-{month:02d}-{day:02d}'
            dest = f'../../datalake/silver/twitter/sentimental_analysis/{country}/{date}'
            df2.coalesce(1).write.mode("overwrite").json(dest)
            dest_csv = f'../../datalake/gold/twitter/sentimental_analysis/{country}/{date}.csv'
            df2.toPandas().to_csv(dest_csv, index=False)
            #del df2, sentimentals, ans
            del df2, sentimentals, ans, text, df, sentimentl_analyser, translator
            spark_nlp.stop()
