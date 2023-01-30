# coid_operator - Operator to collect COVID data from the Jhons Hopkins repository.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
from os.path import join
from pathlib import Path
from hooks.covid_hook import CovidHook
from airflow.models import BaseOperator
from datetime import datetime, timedelta
from airflow.utils.decorators import apply_defaults


class CovidOperator(BaseOperator):
    """
    This class creates an instance of the CovidHook class and executes the 
    necessary methods to collect the COVID data. It is responsible for managing 
    the data collection process from the Jhons Hopkins Github repository.
    """
    
    template_fields = ["file_path", "date"]
    
    @apply_defaults
    def __init__(self, file_path: str, conn_id=None, date=None, *args, **kwargs):
        """
        This function create a new CovidOperator instance.

        Args:
            file_path (str): Path of the folder where the files will be saved.
            conn_id (optional): Parameters to stablish a connection with the 
                                Jhons Hopkins repository. Defaults to None.
            date (str, optional): The date when the COVID data will be collected.
                                  Defaults to None.
        """
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.conn_id = conn_id
        self.date = date
    
    def create_parent_folder(self):
        """
        This function creates the destination folder if it does not already 
        exist, to store the COVID data files.
        """
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
    
    def execute(self):
        """
        This function is resonsible to execute the tasks that are necessaary
        to collect the COVID data.
        """
        # it creates a hook to collect the data.
        hook = CovidHook(conn_id = self.conn_id,
                         date = self.date)
        self.create_parent_folder() # Creates the destination folder
        df = hook.run() # Gets the data
        df['date'] = self.date # Fill the column date with the collect date.
        df.to_csv(self.file_path, index=False) # Save the data as CSV filos.


if __name__ == "__main__":
    # This section is an example of how to use the functions related to 
    # collecting COVID data in an Airflow DAG. It will demonstrate how these 
    # functions will be called and executed when the Airflow pipeline is run. 
    # The specific steps and commands that need to be executed will depend on 
    # the implementation of the functions and the desired behavior of the 
    # Airflow DAG.
    start_date = datetime(2022, 12, 31) # It defines a start date to collect the data.
    end_date = datetime(2023, 1, 1) # It defines the last date to collect the data
    
    # It generates a list with all the dates between start_date and end_date.
    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]
    
    # Interates in each row.
    for date in dates:
        print(date)
        ds_date = datetime.strftime(date, "%Y-%m-%d") # It defines the date as a string.
        ds_date_nodash = datetime.strftime(date, '%Y%m%d') # The same is before but without the character /
        
        # Initialize a CovidOperator instance
        operator = CovidOperator(file_path = join("/mnt/d/bootcamp-covid/datalake/bronze",
                                                  "covid_data",
                                                  f"extract_date={ds_date}",
                                                  f"CovidData_{ds_date_nodash}.csv"),
                                 date = datetime.strftime(date, "%m-%d-%Y"),
                                 task_id = "test_run")
        operator.execute(None) # Executes the operator
