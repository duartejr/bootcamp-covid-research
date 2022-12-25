from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator

class AirflowPlugin(AirflowPlugin):
    name = "bootcamp-covid"
    operators = [TwitterOperator]
    