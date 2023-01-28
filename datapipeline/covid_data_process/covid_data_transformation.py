# covid_data_transformation - Script to select only the important fields in 
# COVID data files.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
import shutil
from os.path import join, exists
from datetime import datetime as dt
from pyspark.sql import SparkSession


def select_country(df: "spark.dataframe", country: str) -> "spark.dataframe":
    """
    This function filters the dataframe to include only information related to 
    the specified country.

    Args:
        df (spark.dataframe): Spark dataframe with all the COVID data.
        country (str): Name of the country of interest.

    Returns:
        spark.dataframe: Dataframe with the data realted with the interest country.
    """
    return df.filter(df.Country_Region == country)


def select_columns(spark: "spark.session", df: "spark.dataframe") -> "spark.dataframe":
    """
    This function selects specific columns from the dataframe that contain the 
    desired data.

    Args:
        spark (spark.session): A Spark session object.
        df (spark.dataframe): A dataframe with the COVID data.

    Returns:
        spark.dataframe: A dataframe with only the selected columns.
    """
    # It create a temporary view of the provided dataframe.
    df.createOrReplaceTempView("tempView")
    
    try:
        # First tries to select the specified columns.
        df = spark.sql("SELECT Country_Region, date, "+\
                       "CAST(Confirmed AS DOUBLE) Confirmed, "+\
                        "CAST(Deaths AS DOUBLE) Deaths, "+\
                        "CAST(Recovered AS DOUBLE) Recovered "+
                        "FROM tempView")
    except:
        # If there is an error in the previous step of selecting columns, this
        # function renames the column with the country names before continuing 
        # with the column selection.
        df = df.withColumnRenamed("Country/Region", "Country_Region")
        df.createOrReplaceTempView("tempView") # It creates a new temporary view
        
        # It selects the desired columns.
        df = spark.sql("SELECT Country_Region, date, "+\
                       "CAST(Confirmed AS DOUBLE) Confirmed, "+\
                       "CAST(Deaths AS DOUBLE) Deaths, "+\
                       "CAST(Recovered AS DOUBLE) Recovered "+
                       "FROM tempView")
    return df


def group_data(spark: "spark.session", df: "spark.dataframe") -> "spark.dataframe":
    """
    This function takes a dataframe with COVID information and groups the data
    by the date and country name, aggregating the relevant information in a 
    summarized manner. The output is a new dataframe with the grouped data.

    Args:
        spark (spark.session): A Spark session.
        df (spark.dataframe): A dataframe with the COVID data.

    Returns:
        spark.dataframe: A dataframe with the COVID data grouped by date and country.
    """
    df.createOrReplaceTempView("tempView") # It create a new temporary view
    
    # This query aggregates the selected data by date and country, creating a 
    # summarized representation of the data.
    df = spark.sql("SELECT Country_Region, date, "+\
                   "SUM(Confirmed) AS Confirmed, "+\
                   "SUM(Deaths) AS Deaths, "+\
                   "SUM(Recovered) AS Recovered "+\
                   "FROM tempView GROUP BY date, Country_Region")
    return df                                           


def save_time_series(spark: "spark.session", df: "spark.dataframe", dest: str):
    """
    This function saves the provided dataframe to the specified destination folder.

    Args:
        spark (spark.session): A Spark session object.
        df (spark.dataframe): A dataframe with the COVID data.
        dest (str):The destination folder path.
    """
    # This function performs an update of the data in the destination folder. 
    # If the destination folder exists, it will make a copy of the data into 
    # the folder, then delete the existing files in the folder, and create new 
    # files with the combined new and old data.
    if exists(dest):
        df_old = spark.read.parquet(dest) # Read the old data.
        df = df.union(df_old).toPandas()  # Combine the new and old data.
        shutil.rmtree(dest)               # Delete the old files.
        df = spark.createDataFrame(df)    # Create a new dataframe.
        df = df.dropDuplicates()          # Remove the duplicated rows.
    
    df.coalesce(1).write.mode("append").parquet(dest) # Save the data into the destination folder.


def add_empty_row(spark: "spark.session", df: "spark.dataframe", date: str,
                  country: str) -> "spark.dataframe":
    """
    This function handles the scenario where the country name is not found in 
    the COVID data files by inserting a row with zero data for that country. 
    In other words, it fills the missing values with zeros, assuming that the 
    number of COVID cases for that country is zero, given the countries of 
    interest in this project.

    Args:
        spark (spark.session): A Spark session object.
        df (spark.dataframe): A dataframe with the COVID data.
        date (str): The date when the COVID data about the country was not found.
        country (str): The country name.

    Returns:
        spark.dataframe: A dataframe with a new row that contains zero values 
                         for the number os COVID cases in the country in the
                         specified date.
    """
    df_collect = df.collect() # It collects the previous data into the dataframe.
    
    # It appends new columns in the original data with zero values
    df_collect.append({"Country_Region": country, "date": date, 
                       "Confirmed":0.0, "Deaths":0.0, "Recovered":0.0})
    new_df = spark.createDataFrame(df_collect) # It creates a new dataframe
    new_df.createOrReplaceTempView("tempView") # It creates a new temporary view
    
    # This query selects and cast the fields of interest
    new_df = spark.sql("SELECT Country_Region, date, "+\
                       "CAST(Confirmed AS DOUBLE) Confirmed, "+\
                       "CAST(Deaths AS DOUBLE) Deaths, "+\
                       "CAST(Recovered AS DOUBLE) Recovered "+
                       "FROM tempView")
    
    return new_df


def execute(spark: "spark.session", src: str, dest: str, country: str,
            country_abrv: str):
    """
    This function performs all necessary transformations in the data as
    described in the previous functions.

    Args:
        spark (spark.session): A Spark session object.
        src (str): Path of the source folder.
        dest (str): Path of the destination folder.
        country (str): The country name.
        country_abrv (str): This is the abbreviation used to identify the 
                            country of interest.
    """
    # It sets the name of the output file.
    out_file = join(dest, f"{country_abrv}.parquet") 
    
    # Read the data into the source folder which contains datas provided by Jhons Hopkins.
    df = spark.read.option("header", True).csv(src) 
    df = select_columns(spark, df)
    df = select_country(df, country)
    
    # If at least one register is found does this.
    if df.count() > 1:
        df = group_data(spark, df)
    
    # If not data is found about the country of interest does this.
    if df.count() == 0:
        date = dt.strftime(dt.strptime(src.split("=")[-1], '%Y-%m-%d'), '%m-%d-%Y')
        df = add_empty_row(spark, df, date, country)
    
    # It saves the transformed data into the destination folder.
    save_time_series(spark, df, out_file)
    

if __name__ == "__main__":
    src = sys.argv[1]                      # It gets the path of the source folder.
    dest = sys.argv[2]                     # It gets the path of the destination folder.
    extract_date = sys.argv[3]             # It gets the date when the data was collected.
    countries = sys.argv[4]                # It gets the country name list.
    countries_abrv = sys.argv[5].split(',')# It gets the country abbreviation list.

    # Start a new Spark session
    spark = SparkSession\
            .builder\
            .appName("covid_transform")\
            .getOrCreate()

    # Execute this if a list of country names, separeted by comma, is provided.
    if ',' in countries: 
        for i, country in enumerate(countries.split(',')):
            execute(spark, src, dest, country, countries_abrv[i])
    # If just a country name is provided execute this.
    else:
        execute(spark, src, dest, countries, countries_abrv)
    
    # Stop the Spark session.
    spark.stop()
