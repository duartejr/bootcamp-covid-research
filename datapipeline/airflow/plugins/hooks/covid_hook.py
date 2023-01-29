# covid_hook - Hook to collect COVID data from the Jhons Hopkins repository.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import pandas as pd
from airflow.providers.http.hooks.http import HttpHook


class CovidHook(HttpHook):
    """
   This class uses the Pandas library to read the CSV files from the Jhons 
   Hopkins Github repository and save them in the specified destination folder. 
   The class generates the URLs of the CSV files and uses them to collect the 
   COVID data provided by Jhons Hopkins. The data collected includes information 
   such as confirmed cases, deaths, and recoveries for various countries and 
   regions
   """
    
    def __init__(self, conn_id = None, date = None):
        """
        Initialize a new Covidhook objetc to collect the COVID data.

        Args:
            conn_id (, optional): Paramenters of the http connection. Defaults to None.
            date (str, optional): The date for which the COVID data is to be collected. Defaults to None.
        """
        self.conn_id = conn_id or "covid_default"
        self.date = date
        super().__init__(http_conn_id = self.conn_id)
        
    def create_url(self) -> str:
        """
        This function generates the URL to access the COVID data from a diven date.

        Returns:
            str: The URL to access the COVID data.
        """
        file = f'{self.date}.csv'
        url = f"{self.base_url}/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{file}"
        return url
    
    def read_file(self, url: str) -> "pandas.dataframe":
        """
        This function uses Pandas to read the COVID data directly from the 
        Jhons Hopkins repository.

        Args:
            url (str): The URL to access the COVID data.

        Returns:
            pandas.dataframe: A dataframe which contains the COVID of a given date.
        """
        df = pd.read_csv(url)
        return df
    
    def run(self) -> "pandas.dataframe":
        """
        This function is reponsible to executes all the tasks that are needed to
        collect the COVID data from the Jhons Hopikins repository.

        Returns:
            pandas.dataframe: _A dataframe which contains the COVID of a given date.
        """
        self.get_conn()
        url = self.create_url()
        print("#"*100)
         # This function prints the generated URL for the data collection on 
         # the screen for verification purposes.
        print(url)
        print("#"*100)
        df = self.read_file(url)
        return df
        

if __name__ == "__main__":
    covid_hook = CovidHook(date = '01-01-2021')
    covid_hook.run()
