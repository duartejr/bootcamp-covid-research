import covid_data_transformation
import covid_data_calc_fields
import covid_cases_forecast
import covid_export_csv
from datetime import datetime, timedelta
from datetime import datetime as dt
from pyspark.sql import SparkSession
from os.path import join
from pathlib import Path
from os.path import exists

src = '/mnt/d/bootcamp-covid/datalake/bronze/covid_data'
dest = '/mnt/d/bootcamp-covid/datalake/silver/covid_data'
start_date = datetime(2020, 1, 22)
end_date = datetime(2023, 1, 1)
BASE_FOLDER = join(str(Path("/mnt/d/bootcamp-covid")),
                       "datalake/{stage}/covid_data/{partition}")
EXTRACT_DATE = dt.now() - timedelta(days=1)
PARTITION_FOLDER = f"extract_date={dt.strftime(EXTRACT_DATE, '%Y-%m-%d')}"
COUNTRIES = ["Spain", "Chile", "Mexico", "Argentina"]
COUNTRIES_ABRV = ["ES", "CH", "MX", "AR", "EC"]
# COUNTRIES = ["Ecuador"]
# COUNTRIES_ABRV = ["EC"]
dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]
spark = SparkSession\
            .builder\
            .appName("covid_transform")\
            .getOrCreate()
src  = '/mnt/d/bootcamp-covid/datalake/silver/covid_data/forecast'
dest = '/mnt/d/bootcamp-covid/datalake/gold/covid_data/forecast/arima'
covid_export_csv.execute(spark, src, dest, COUNTRIES_ABRV)

# for date in dates:
#     extract_date = dt.strftime(date, '%Y-%m-%d')
#     print(extract_date)
    
    # partition_folder = f"extract_date={dt.strftime(date, '%Y-%m-%d')}"
    # src_transform = BASE_FOLDER.format(stage = "bronze",
    #                                    partition = partition_folder)
    # dest_transform = BASE_FOLDER.format(stage = "silver",
    #                                     partition = "time_series")
    
    # print(extract_date)
    # print('Transforming data:')
    # print('src_transform:', src_transform)
    # for i, country in enumerate(COUNTRIES):
    #     print(country)
    #     # print(country)
    #     country_abrv = COUNTRIES_ABRV[i]
        # covid_data_transformation.execute(spark, src_transform, dest_transform,
        #                                   country, country_abrv)
        
        # if exists(dest_transform):
        #     print('dest_transform', dest_transform)
        # print('calc_fields:')
        # src_calc_fields = BASE_FOLDER.format(stage = 'silver',
        #                                     partition = 'time_series')
        # dest_calc_fields = BASE_FOLDER.format(stage = 'silver',
        #                                       partition = 'series_with_calc_fields')
        # covid_data_calc_fields.execute(spark, src_calc_fields,
        #                                dest_calc_fields,
        #                                country_abrv)
        # print('dest_calc_fields:', dest_calc_fields)
        
        # print('Doing forecasts:')
        # src_forecast = BASE_FOLDER.format(stage = 'silver',
        #                                   partition = 'series_with_calc_fields')
        # dest_forecast = BASE_FOLDER.format(stage = 'silver',
        #                                    partition = 'forecast')
        # covid_cases_forecast.execute(spark, src_forecast, dest_forecast,
        #                              country_abrv, fcst_date=date.date())
        

spark.stop()