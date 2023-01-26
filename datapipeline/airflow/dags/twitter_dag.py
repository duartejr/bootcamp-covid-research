# twitter_dag - Dag to do ETL in Twitter data.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import subprocess
from os.path import join
from datetime import datetime, timedelta

# Import airflow functions 
from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Import operator to download Tweets
from operators.twitter_operator import TwitterOperator 

# Control variables
DIR_TWITTER_SCRIPTS = Variable.get("dir_twitter_scripts")
DIR_SQL_SCRIPTS     = Variable.get("dir_sql_scripts")
DIR_DATALAKE        = Variable.get("dir_datalake")
ARGS                = {"owner"           : "airflow",
                       "depends_on_past" : False,
                       "start_date"      : days_ago(1)}
TIMESTAMP_FORMAT    = "%Y-%m-%dT%H:%M:%S.00Z"
START_DATE          = datetime.today() - timedelta(1)
BASE_FOLDER         = join(DIR_DATALAKE,
                           "{stage}/twitter/query_covid/{country}/{partition}")
PARTITION_FOLDER    = "extract_date={}".format(datetime.strftime(START_DATE ,
                                                                 '%Y-%m-%d'))
COUNTRIES           = ["CL", "AR", "MX", "EC", "ES"]


def transform_twitter(**kwargs):
    """
    This function calls the script "tweet_transformation.py" which extracts only
    the important information from the Twitter API reponse and save the data in
    the destination folder, separeted by country.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    process_date (str) : The date on which the data was collected.
    table_name (str): The name of the folder where the final data will be saved.
    """
    src          = kwargs["src"]
    dest         = kwargs["dest"]
    process_date = kwargs["process_date"]
    table_name   = kwargs["table_name"]
    subprocess.run(["python",
                    f"{DIR_TWITTER_SCRIPTS}/tweet_transformation.py",
                    src, dest, process_date, table_name])


def sentimental_analysis(**kwargs):
    """
    This function calls the script "tweet_sentimental_analysis.py" which performs
    a sentiment analysis of the tweet provided.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    process_date (str) : The date on which the data was collected.
    """
    src          = kwargs["src"]
    dest         = kwargs["dest"]
    process_date = kwargs["process_date"]
    subprocess.run(["python",
                    f"{DIR_TWITTER_SCRIPTS}/tweet_sentimental_analysis.py",
                    src, dest, process_date])

def export_csv(**kwargs):
    """
    This function calls the script "tweet_export_csv.py" which saves the data
    in CSV format in the destination folder.
    
    Attributes:
    src (str) : The directory where the original data is located.
    dest (str) : The directory where the transformed data will be saved.
    countries (str) : A list of country names separeted by commas.
    """
    src       = kwargs["src"]
    dest      = kwargs["dest"]
    countries = kwargs["countries"]
    subprocess.run(["python",
                    f"{DIR_TWITTER_SCRIPTS}/tweet_export_csv.py",
                    src, dest, countries])

def export_sql(**kwargs):
    """
    This function calls the script "insert_into_datawarehouse.py" which exports
    the data to a data warehouse.
    
    Attributes:
    src (str) : The directory where the original data is located.
    table (str): The name of the table where the final data will be storaged.
    extract_date (str) : The date on which the data was collected. The default 
                         is None.
    """
    src       = kwargs["src"]
    table     = kwargs["table"]
    
    if "extract_date" in kwargs.keys():
        extract_date = kwargs["extract_date"]
    else:
        extract_date = None # If no extract_Date is provided the default is None

    if extract_date:
        # If a extract date is provided, this call will add a new row to the
        # table in the data warehouse, rather then overwriting existing data.
        subprocess.run(["python",
                        f"{DIR_SQL_SCRIPTS}/insert_into_datawarehouse.py",
                        src, table, extract_date])
    else:
        # Otherwise, if no extract data is provided, al the data in the table
        # will be replaced with the new data.
        subprocess.run(["python",
                        f"{DIR_SQL_SCRIPTS}/insert_into_datawarehouse.py",
                        src, table]) 

# Implementation of a DAG to control the ETL process of Twitter data.
with DAG(dag_id            = "Twitter_dag", # DAG name for the Airflow control panel
         default_args      = ARGS,          # Setting the default args
         schedule_interval ='0 1 * * *',    # Schedule the DAG to run every day at 1AM
         max_active_runs   = 1
         ) as dag:
    
    # Task to exort the tweets in the CSV format.
    twitter_export_csv = PythonOperator(
            task_id         = "twitter_export_csv",
            python_callable = export_csv,
            op_args         = [],
            op_kwargs       = {"src": f"{DIR_DATALAKE}/silver/twitter/sentiment_analysis",
                               "dest":  f"{DIR_DATALAKE}/gold/twitter/sentiment_analysis",
                               "countries": ",".join(COUNTRIES)}
        )

    # Task to export the tweets into the data warehouse.
    twitter_export_sql = PythonOperator(
            task_id         = "twitter_export_sql",
            python_callable = export_sql,
            op_args         = [],
            op_kwargs       = {"src":   f"{DIR_DATALAKE}/gold/twitter/sentiment_analysis",
                               "extract_date": datetime.strftime(START_DATE, '%Y-%m-%d'),
                               "table": "SENTIMENTOS"}
        )


    # Loop to performs individual task to each country.
    for country in COUNTRIES:
        # Taks to download the tweets
        twitter_operator = TwitterOperator(
            task_id    = "twitter_covid_" + country,
            query      = "covid", # Query is the word that Twiiter API will search in tweets.
            file_path  = join(BASE_FOLDER.format(stage     = "bronze",
                                                 country   = country,
                                                 partition = PARTITION_FOLDER),
                              "CovidTweets_{}.json".format(datetime.strftime(START_DATE, "%Y%m%d"))),
            start_time = datetime.strftime(START_DATE, "%Y-%m-%dT00:00:00.00Z"), # First datetime of the tweets
            end_time   = datetime.strftime(START_DATE, "%Y-%m-%dT23:59:59.00Z"), # Last datetime of the tweets
            country    = country, # Origin country of the tweets
        )

        # Task to filter only the important fields from the API response.
        twitter_transform = PythonOperator(
            task_id         = "twitter_transform_" + country,
            python_callable = transform_twitter,
            op_args         = [],
            op_kwargs       = {"src" : BASE_FOLDER.format(stage      = "bronze",
                                                          country    = country,
                                                          partition  = PARTITION_FOLDER),
                               "dest" : BASE_FOLDER.format(stage     = "silver",
                                                           country   = country,
                                                           partition = ""),
                               "process_date" : datetime.strftime(START_DATE , '%Y-%m-%d'),
                               "table_name": "tweet_processed"}
        )
        
        # Task to perform sentiment analysis of the tweets.
        twitter_sentiments_analysis = PythonOperator(
            task_id         = "twitter_sentiment_" + country,
            python_callable = sentimental_analysis,
            op_args         = [],
            op_kwargs       = {"src": BASE_FOLDER.format(stage      = "silver",
                                                         country    = country,
                                                         partition  = "tweet_processed"),
                               "dest":  f"{DIR_DATALAKE}/silver/twitter/sentiment_analysis/{country}",
                               "process_date": datetime.strftime(START_DATE , '%Y-%m-%d')}
        )

        # Executation order of the tasks
        twitter_operator >> twitter_transform >> twitter_sentiments_analysis >> twitter_export_csv >> twitter_export_sql
