"""
## title

description
"""

from airflow.decorators import dag, task 
from pendulum import datetime 

@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
)
def setup_teardown_basic():
    @task 
    def t1():
        print("t1")

    t1()

setup_teardown_basic()