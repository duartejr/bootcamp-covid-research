import subprocess
from datetime import date, timedelta
from datetime import datetime
import sys
from pyspark.sql import SparkSession
sys.path.insert(0, '../../datapipeline/covid_data_process')
from covid_cases_forecast import execute

src = '/mnt/d/bootcamp-covid/datalake/silver/covid_data/series_with_calc_fields'
dest = '/mnt/d/bootcamp-covid/datalake/silver/covid_data/forecast_v2'
countries = "ES,EC,CH,MX,AR"
start_date = date(2020, 1, 22)
end_date = date(2022, 12, 31)
dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days)]

spark = SparkSession.builder\
                    .appName("covid_cases_forecast")\
                    .getOrCreate()
for date in dates:
    print('='*100)
    print(date)
    print('='*100)
    fcst_date = str(date)
    print(date)

    for country in countries.split(','):
        execute(spark, src, dest, country, fcst_date=fcst_date)

spark.stop()
