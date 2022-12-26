import pandas as pd
from os.path import join
from pathlib import Path
from datetime import datetime, timedelta
from airflow.models import BaseOperator, DAG
from airflow.utils.decorators import apply_defaults
from hooks.covid_hook import CovidHook
import pandas as pd
import time


class CovidOperator(BaseOperator):
    
    template_fields = ["file_path", "date"]
    
    @apply_defaults
    def __init__(self, file_path, conn_id=None, date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.conn_id = conn_id
        self.date = date
    
    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
    
    def execute(self, context):
        hook = CovidHook(conn_id = self.conn_id,
                         date = self.date)
        self.create_parent_folder()
        df = hook.run()
        df.to_csv(self.file_path, index=False)


if __name__ == "__main__":
    # date = '01-01-2021'
    date = datetime(2021, 1, 1)
    ds_date = datetime.strftime(date, "%Y-%m-%d")
    ds_date_nodash = datetime.strftime(date, '%Y%m%d')
    # with DAG(dag_in="CovidTest", start_date=date) as dag:
    operator = CovidOperator(file_path = join("/mnt/d/bootcamp-covid/datalake/bronze",
                                            "covid_data",
                                            f"extract_date={ds_date}",
                                            f"CovidData_{ds_date_nodash}.csv"),
                            date = datetime.strftime(date, "%d-%m-%Y"),
                            task_id = "test_run")
    operator.execute(None)