# covid_data_calc_filedst - Script to calculate the fields: new cases, new deaths,
# new recovered 
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
import shutil
from os.path import join, exists
from pyspark.sql import SparkSession


def save(df: "spark.dataframe", dest: str):
    """
    This function saves the provided dataframe to the specified destination folder.

    Args:
        df (spark.dataframe): Spark dataframe with the data.
        dest (str): The destination folder where the data will be saved.
    """
    if exists(dest):
        shutil.rmtree(dest)
    df.coalesce(1).write.parquet(dest)


def calc_fields(spark: "spark.session", df: "spark.dataframe") -> "spark.dataframe":
    """
    This function computes the fields for new confirmed cases, new deaths, 
    new recoveries, and active cases.

    Args:
        spark (spark.session): A Spark session object.
        df (spark.dataframe): The dataframe with the COVID data.

    Returns:
        spark.dataframe: A dataframe with the new fields.
    """
    df = df.toPandas() # Convert spark.dataframe to pandas.dataframe.
    columns = ['Confirmed', 'Deaths', 'Recovered'] # Columns with original data.
    new_col = ['New_Confirmed', 'New_Deaths', 'New_Recovered'] # Name of the new columns.
    
    # Computes the new coolumns
    for i in range(len(columns)):
        # Calculates the difference between the current day and previous day.
        df[new_col[i]] = df[columns[i]].diff()

    df = df.fillna(0) # Fill NaN values with zeros.
    
    # This calculates the active cases by subtracting the sum of deaths and 
    # recoveries from the total number of confirmed cases until the present day.
    df['Active'] = df['Confirmed'] - df['Deaths'] - df['Recovered']
    
    # Reorder the columns
    df = df[['date', 'Country_Region', 'Confirmed', 'Deaths', 
             'Recovered', 'Active', 'New_Confirmed', 'New_Deaths', 
             'New_Recovered']]
    df = spark.createDataFrame(df) # Convert pandas.dataframe to spark.dataframe
    return df

def format_dates(spark: "spark.session", df: "spark.dataframe") -> "spark.dataframe":
    """
    This function converts the data in the "date" column to the DATE Format.

    Args:
        spark (spark.session): A Spark session object.
        df (spark.dataframe): Dataframe with the COVID data.

    Returns:
        spark.dataframe: Dataframe reordered and with the "date" column in the 
                         right format. 
    """
    df.createOrReplaceTempView("tempView") # Creates a temporary dataframe.
    
    # This selects specific columns from the data and converts the format of 
    # the "date" column to the DATE format.
    df = spark.sql("SELECT Country_Region, "+\
                   "TO_DATE(date,'MM-dd-yyyy') date, "+\
                   "Confirmed, Deaths, Recovered "+\
                   "FROM tempView")
    df = df.dropDuplicates() # This step removes duplicated rows.
    df = df.orderBy("date") # This steo orders the date by the column date.
    return df


def read_data(spark: "spark.session", src: str) -> "spark.dataframe":
    """
    This function reads the data as a spark dataframe.

    Args:
        spark (spark.session): Spark session object._
        src (str): Source folder.

    Returns:
        spark.dataframe: Spark dataframe with the data
    """
    df = spark.read.parquet(src) # Here it is using the Parquet format.
    df = format_dates(spark, df)
    return df


def execute(spark: "spark.session", src: str, dest: str, country: str):
    """
    This function performs all necessary operations to calculate the new fields
    and saving the processed data as Parquet files in the specified 
    destination folder.

    Args:
        spark (spark.session): A Spark session object.
        src (str): Path of the source folder.
        dest (str): Path of the destination folder.
        country (str): A list of country names.
    """
    # It configures the paths to access the Parquet files into the folders.
    src = join(src, f'{country}.parquet')
    dest = join(dest, f'{country}.parquet')
    
    # It carries out the specified tasks only if the source folder exists.
    if exists(src):
        df = read_data(spark, src)
        df = calc_fields(spark, df)
        save(df, dest)


if __name__ == "__main__":
    src = sys.argv[1] # Gets the source folder path.
    dest = sys.argv[2] # Gets the destination folder path.
    countries = sys.argv[3] # Gets the contry names list. 
    
    # Start a Spark session
    spark = SparkSession\
                .builder\
                .appName("covid_calc_fields")\
                .getOrCreate()
    
    # When have more the one country listed. The names need to be separeted
    # by comma.
    if ',' in countries:
        for country in countries.split(','): # Iterates in each country name
            execute(spark, src, dest, country) # Executes the tasks.
    else: # When just onde country name is provided.
        execute(spark, src, dest, countries) # Execute the tasks
    
    # Stop the Spark session.
    spark.stop()
