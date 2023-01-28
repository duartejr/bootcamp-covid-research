# covid_export_csv - Script to export the COVID data into CSV format.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import sys
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Dictionary with the abreviation name and ID used to each country in this project.
COUNTRIES_MAP = {'AR':1, 'CH':2, 'EC':3, 'ES':4, 'MX':5}


def save(df: "spark.dataframe", dest: str):
    """
    This function saves the provided dataframe into de destination folder in the CSV format.

    Args:
        df (spark.dataframe): Spark dataframe with the COVID data.
        dest (str): Path of the destination folder.
    """
    df = df.toPandas() # Converts the Spark dataframe to Pandas dataframe.
    df['country'] = df['country'].replace(COUNTRIES_MAP) # Replace the country name by the ID.
    df.to_csv(dest, index=False) # Save the data into the destination folder.
    

def read_data(spark: "spark.session", src: str) -> "spark.dataframe":
    """
    This function reads the data that is in the provided source folder. 

    Args:
        spark (spark.session): A Spark session object.
        src (str): Path of the source folder.

    Returns:
        spark.dataframe: The dataframe with the COVID data.
    """
    df = spark.read.parquet(src)
    return df


def order_columns_forecast(df: "spark.dataframe") -> "spark.dataframe":
    """
    This functions orders the dataframe by the columns: forecast_date and
    forecasted_date.

    Args:
        df (spark.dataframe): Spark dataframe with forecasts of COVID cases.

    Returns:
        spark.dataframe: A dataframe that has been sorted based on the specified columns.
    """
    df = df.orderBy('forecast_date', 'forecasted_date')
    df = df.select('country', 'forecast_date', 'forecasted_date', 'forecast')
    return df


def order_columns_jhons(df: "spark.dataframe") -> "spark.dataframe":
    """
    This function orders the COVID data, provided by Jhons Hopkins, by the
    column date.

    Args:
        df (spark.dataframe): A Spark dataframe with the unordered data.

    Returns:
        spark.dataframe: A Spark dataframe with the ordered data.
    """
    df = df.orderBy('date')
    df = df.select('country', 'date', 'Confirmed', 'Deaths',
                   'Active', 'New_Confirmed', 'New_Deaths',
                   'New_Recovered')
    return df


def execute(spark: "spark.session", src: str, dest: str, countries: list,
            table: str):
    """
    It executes the necessary steps to perform transformations on the unordered 
    dataframe and finally export it to the specified destination folder in the 
    correct format (CSV, for example). The exact steps involved in the process 
    will depend on the specific functions defined earlier.

    Args:
        spark (spark.session): A Spark session object.
        src (str): Path of the source folder.
        dest (str): Path of the destination folder
        countries (list): The list of country names.
        table (str): The folder where the will be storaged.
    """
    # It does a loop that interate in each country name.
    for i, country in enumerate(countries):
        # Configures the source path to read only Parquet files.
        src_data = join(src, f'{country}.parquet')
        
        # If it is trying do export the data in the forecast folder does this.
        if table == 'forecast':
            # Considering that the forecast model is the ARIMA.
            dest_data = join(dest, f'arima_{country}.csv') 
        # In other case does this.
        else:
            dest_data = join(dest, f'covid_jhons_{country}.csv')
        
        # These steps are reading the data.
        df = read_data(spark, src_data)
        df = df.withColumn("country", f.lit(country))
        
        # This conditional structure organize the data according with the 
        # destination folder.
        if table == 'forecast':
            df = order_columns_forecast(df)
        else:
            df = order_columns_jhons(df)
        
        # It saves the data.
        save(df, dest_data)
        
        # The next steps are used to combine the data from each country in a
        # unique dataframe.
        if i == 0:
            df_tot = df
        else:
            df_tot = df_tot.union(df)
    
    # The next steps are executed to export a unique file with the data about
    # all the countries.
    if table == 'forecast':
        dest_data_tot = join(dest, 'arima_all.csv')
    else:
        dest_data_tot = join(dest, 'jhons_hopkins_all.csv')

    # It saves the data, of all the countries, in a unique file.
    save(df_tot, dest_data_tot)


if __name__ == "__main__":
    src = sys.argv[1]                  # It gets the source folder path.
    dest = sys.argv[2]                 # It gets the destination folder path.
    countries = sys.argv[3].split(',') # It gets the list of country names.
    table = sys.argv[4]                # it gets the name of the destination folder.
    
    # Start a new Spark session
    spark = SparkSession\
            .builder\
            .appName("covid_export_csv")\
            .getOrCreate()

    # Execute all the neeed tasks.
    execute(spark, src, dest, countries, table)
    
    # Stop the Spark session.
    spark.stop()
