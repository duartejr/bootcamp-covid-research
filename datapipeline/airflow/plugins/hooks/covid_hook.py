import pandas as pd
from airflow.providers.http.hooks.http import HttpHook

class CovidHook(HttpHook):
    
    def __init__(self, conn_id = None, date = None):
        self.conn_id = conn_id or "covid_default"
        self.date = date
        super().__init__(http_conn_id = self.conn_id)
        
    def create_url(self):
        file = f'{self.date}.csv'
        url = f"{self.base_url}/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{file}"
        return url
    
    def read_file(self, url):
        df = pd.read_csv(url)
        return df
    
    def run(self):
        self.get_conn()
        url = self.create_url()
        df = self.read_file(url)
        return df
        

if __name__ == "__main__":
    covid_hook = CovidHook(date = '01-01-2021')
    covid_hook.run()