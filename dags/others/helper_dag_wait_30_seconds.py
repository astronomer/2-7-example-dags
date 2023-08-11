"""
## Helper DAG 

Waits 30 seconds before finishing.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import time


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["deferrable", "toy", "core"],
)
def helper_dag_wait_30_seconds():
    @task
    def wait_for_it():
        time.sleep(30)
        print("Theodosia writes me a letter every day...")

    wait_for_it()


helper_dag_wait_30_seconds()
