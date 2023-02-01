# test_covid_cases_forecast.py - Script to test the COVID cases forecast models.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
from datetime import date, timedelta
from pyspark.sql import SparkSession

# Adding the path of the model into the Seach paths used by Python
sys.path.insert(0, '../../datapipeline/covid_data_process')
from covid_cases_forecast import execute

# Source folder of the data.
src = '/mnt/d/bootcamp-covid/datalake/silver/covid_data/series_with_calc_fields'

# Destination folder of the forecasts.
dest = '/mnt/d/bootcamp-covid/datalake/silver/covid_data/forecast_v2'

countries = "ES,EC,CH,MX,AR"   # List of countries
start_date = date(2020, 1, 22) # First date to do a forecast.
end_date = date(2022, 12, 31)  # Last date to do a forecast.

# Generates all the dates between the start date and the end date.
dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days)]

# Start a new Spark session
spark = SparkSession.builder\
                    .appName("covid_cases_forecast")\
                    .getOrCreate()

# Interate to make a forecast to each date.
for date in dates:
    print('='*100)
    print(date)
    print('='*100)
    fcst_date = str(date)
    print(date)

    # Interate in each country
    for country in countries.split(','):
        # Execute the forecast model
        execute(spark, src, dest, country, fcst_date=fcst_date)

# Stop the Spark session
spark.stop()
