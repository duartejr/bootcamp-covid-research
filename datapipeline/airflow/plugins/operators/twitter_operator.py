import json
from os.path import join
from pathlib import Path
from datetime import datetime, timedelta
from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook
import pandas as pd
import time

class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time",
        "country"
    ]

    @apply_defaults
    def __init__(self, query, file_path, conn_id=None, start_time=None, end_time=None, country=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time
        self.country = country
    
    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time,
            country = self.country
        )
        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")

if __name__ == "__main__":
    start_date = '2019-1-1'
    end_date = '2021-12-31'
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    for st_dt in pd.date_range(start=start_date, end=end_date):
        ds_date = datetime.strftime(st_dt, "%Y-%m-%d")
        ds_date_nodash = datetime.strftime(st_dt, "%Y%m%d")
        print(st_dt)
        for country in ["ES", 'EC', 'CL', 'MX', 'AR', 'BR']:
            print(country)
            
            with DAG(dag_id="TwitterTest", start_date=st_dt) as dag:
                to = TwitterOperator(
                    query = "covid",
                    file_path = join(
                        "/mnt/d/bootcamp-covid/datalake/bronze",
                        "twitter_bootcamp-covid",
                        country,
                        f"extract_date={ds_date}",
                        f"CovidTweets_{country}_{ds_date_nodash}.json"),
                    task_id = "test_run",
                    start_time=datetime.strftime(st_dt, TIMESTAMP_FORMAT),
                    end_time=datetime.strftime(st_dt + timedelta(days=1), TIMESTAMP_FORMAT),
                    country=country
                )
                
                #ti = TaskInstance(task=to)
                to.execute(None)
                print('sleeping')
                time.sleep(10)
            time.sleep(5)
        time.sleep(10)
        # break
        #         # time.sleep(5)
                
        #         #ti.run()