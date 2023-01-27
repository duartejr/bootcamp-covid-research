# covid_cases_forecast - Script to made COVID cases forecasts..
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.

import sys
import numpy
import shutil
import pandas as pd
from os.path import join, exists
from pyspark.sql import functions as f
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from statsmodels.tsa.arima.model import ARIMAResults
from pyspark.sql.types import StructType, StructField, DateType, FloatType


def forecast(obs_data: "spark.dataframe", src_model: str, fcst_horizon=7, 
             format_BI=True, fcst_date=None) -> "pandas.dataframe":
    """
    This function selects the data in the dataframe that is up to the date 
    immediately preceding the forecast date. This data will be used as the 
    observed data for the forecast. If no forecast date is specified, it will 
    use the last date in the dataframe as the observed data.

    Args:
        obs_data (spark.dataframe): Spark dataframe with the COVID data.
        src_model (str): The location where the forecasting model is stored.
        fcst_horizon (int, optional): Number of days in the future for which 
                                      the model will predict COVID-19 cases.
                                      Defaults to 7.
        format_BI (bool, optional): Determines if the output data will be in a 
                                    format suitable for use in business 
                                    intelligence tools or if it's just a test 
                                    run. Defaults to True.
        fcst_date (_type_, optional): The date from which the forecast will begin.
                                      Defaults to None.

    Returns:
        pandas.dataframe: Dataframe with the forecasts.
    """
    model = ARIMAResults.load(src_model) # Load the model.
    obs_data = obs_data.toPandas()       # Convert the Spark dataframe to pandas dataframe.
    obs_data = obs_data.set_index("date")# Set the column date as index of the dataframe.
    
    # If a forecast date is provided, the function will perform the forecast 
    # using the specified date. If no forecast date is provided, it will use 
    # the last date in the dataframe as the starting point for the forecast.
    if fcst_date:
        fcst_date = datetime.strptime(fcst_date, '%Y-%m-%d').date()
        # Select the observed data until the last date before the forecast date.
        obs_data = obs_data.loc[:fcst_date] 

    # If there are more than 150 days in the obs_data, it will truncate the 
    # dataframe to use only the last 150 records.
    if len(obs_data) > 150:
        obs_data = obs_data.iloc[len(obs_data)-150:]
    
    # Apply the obs data into the model.
    model = model.apply(obs_data)
    
    # Get the forecasts.
    fcst = model.get_forecast(fcst_horizon).summary_frame()
    
    # If the output format is BI
    if format_BI:
        fcst = fcst.reset_index()
        fcst = fcst.drop(columns='mean_se') # Remove a uncessary column.
        # The first column of the dataframe is set as the last observed date, 
        # which is the date when the forecast is made.
        fcst['forecast_date'] = obs_data.index[-1]
        # Rename the columns
        fcst.columns = ['forecasted_date', 'forecast', 'mean_ci_lower',
                        'mean_ci_upper', 'forecast_date']
        fcst = fcst[['forecast_date', 'forecasted_date', 'forecast',
                     'mean_ci_lower', 'mean_ci_upper']]
        
        # If the type of the forecasted date is int64 it converts to the correct format.
        if type(fcst.forecasted_date[0]) == numpy.int64:
            forecasted_dates = [fcst.forecast_date[i] + timedelta(days=i+1) for i in range(len(fcst))]
            fcst['forecasted_date'] = forecasted_dates
    # If the output is a test format execute this.
    else:
        fcst = fcst.mean.values[:]
        total_fcst = fcst.sum()
        fcst = [obs_data.index[-1]] + list(fcst) + [total_fcst]
        horizons = [f'd{i}' for i in range(1, fcst_horizon+1)]
        fcst = pd.DataFrame(fcst).T
        fcst.columns = ['forecast_date'] + horizons + ['total']
        
    return fcst


def save(spark: "spark.session", df: "spark.dataframe", dest: str):
    """
    This function saves the provided dataframe to the specified destination folder.

    Args:
        spark (spark.session): A Spark session object.
        df (spark.dataframe): Spark dataframe with the data.
        dest (str): The destination folder where the data will be saved.
    """
    # This part of the code will be executed if the destination folder exists.
    if exists(dest):
        df_old = spark.read.parquet(dest) # Reads the old data
        df = df.union(df_old).toPandas()  # Combines the new dataframe with the old.
        shutil.rmtree(dest)               # Deletes the old files.
        df = spark.createDataFrame(df)    # Creates a new spark dataframe.
        df = df.dropDuplicates()          # Remove the duplicated rows.
    
    # Save the data in the destination folder.
    df.coalesce(1).write.parquet(dest)
    

def read_data(spark: "spark.session", src: str) -> "spark.dataframe":
    """
    This function reads the data as a spark dataframe.

    Args:
        spark (spark.session): Spark session object._
        src (str): Source folder.

    Returns:
        spark.dataframe: Spark dataframe with the data
    """
    df = spark.read.parquet(src)
    return df


def select_data(df: "spark.dataframe", col: str) -> "spark.dataframe":
    """
    This function returns a Spark dataframe containing only the specified column.

    Args:
        df (spark.dataframe): A Spark dataframe with the data.
        col (str): Column name that will be selected.

    Returns:
        spark.dataframe: Spark dataframe with the selected column.
    """
    return df.select([f.col("date"), f.col(col)])


def pd_to_spark(spark: "spark.session", df: "pandas.dataframe",
                format_BI=True) -> "spark.dataframe":
    """
    This function transforms a Pandas dataframe into a Spark dataframe.

    Args:
        spark (spark.session): A Spark session objetc.
        df (pandas.dataframe): A Pandas dataframe with the data.
        format_BI (bool, optional): Determines if the output data will be in a 
                                    format suitable for use in business 
                                    intelligence tools or if it's just a test 
                                    run. Defaults to True.

    Returns:
        spark.dataframe
    """
    # If the output is BI format just convert the input dataframet directly
    # into a Spark dataframe.
    if format_BI:
        df = spark.createDataFrame(df)
        return df
    # But if the output format isn't BI more actions are needed.
    else:
        content = df.values.tolist()[0] # Get the dataframe values as a list.
        
        # Configure the row of the Spark dataframe
        data = [
            Row(date   = content[0],
                value1 = float(content[1]),
                value2 = float(content[2]),
                value3 = float(content[3]),
                value4 = float(content[4]),
                value5 = float(content[5]),
                value6 = float(content[6]),
                value7 = float(content[7]),
                value8 = float(content[8]))
            ]
        
        # Creates the data schema
        schema = StructType([
            StructField("forecast_date", DateType(), True),
            StructField("d1", FloatType(), True),
            StructField("d2", FloatType(), True),
            StructField("d3", FloatType(), True),
            StructField("d4", FloatType(), True),
            StructField("d5", FloatType(), True),
            StructField("d6", FloatType(), True),
            StructField("d7", FloatType(), True),
            StructField("total", FloatType(), True)
        ])
    
        df = spark.createDataFrame(data, schema)
        return df

   
def execute(spark: "spark.session", src: str, dest: str, country: str,
            fcst_horizon=7, col="New_Confirmed", fcst_date=None, format_BI=True):
    """
    This function performs all the necessary tasks to make the COVID case
    forecasts, including loading the model, selecting the observed data, and 
    making the predictions. It also saves the forecasted data to a designated 
    folder.

    Args:
        spark (spark.session): A Spark session object
        src (str): Source folder with the COVID data.
        dest (str): Destination folder where the forecasts will be stored.
        country (str): Country name. Used to call the right forecast model.
        fcst_horizon (int, optional): Number of days to do forecasts.
                                      Defaults to 7.
        col (str, optional): Columna name with the observed COVID cases.
                             Defaults to "New_Confirmed".
        fcst_date (_type_, optional): The day on which the forecast is made.
                                      Defaults to None.
        format_BI (bool, optional): Determines if the output data will be in a 
                                    format suitable for use in business 
                                    intelligence tools or if it's just a test
                                    run. Defaults to True.
    """
    src = join(src, f'{country}.parquet')   # Configure the source folder name.
    dest = join(dest, f'{country}.parquet') # Configure the destination folder name.
    
    # It will be executed just if the source folder exists.
    if exists(src):
        # Path to access the model.
        src_model = f'/mnt/d/bootcamp-covid/model/arima_{country}.pkl'
        df = read_data(spark, src) # Reads the data
        df = select_data(df, col)  # Selects the columns of interest from a dataframe.
        
        # Made the forecasts
        fcst = forecast(df, src_model,
                        fcst_horizon = fcst_horizon,
                        fcst_date = fcst_date,
                        format_BI = format_BI)
        
        # Convert Pandas daframe to Spark dataframe
        fcst = pd_to_spark(spark, fcst)
        
        # Save the forecasts.
        save(spark, fcst, dest)


if __name__ == "__main__":
    src = sys.argv[1]       # Get the source folder.
    dest = sys.argv[2]      # Get the destination folder.
    countries = sys.argv[3] # Get the country names.
    fcst_date = sys.argv[4] # Get the forecast date.
    
    # Initialize a Spark session
    spark = SparkSession\
                .builder\
                .appName("covid_cases_forecast")\
                .getOrCreate()
    
    # If more the one country name is informed.
    # The country names need to be separeted by comma.
    if ',' in countries: 
        for country in countries.split(','): # Interats in each country name
            # It does the forecasts.
            execute(spark, src, dest, country, fcst_date=fcst_date)
    else:
        # It does the forecasts
        execute(spark, src, dest, countries, fcst_date=fcst_date)
    
    # Stop the Spark session
    spark.stop()
