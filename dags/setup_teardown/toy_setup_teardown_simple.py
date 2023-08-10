"""
## Toy DAG to show a simple setup/teardown grouping

This DAG shows a simple setup/teardown grouping pipeline with mock tasks.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["setup/teardown", "toy"],
)
def toy_setup_teardown_simple():
    @task
    def create_cluster():
        return "Setting up the big cluster!"

    @task
    def run_query1():
        return "Running query 1!"

    @task
    def run_query2():
        return "Running query 2!"

    @task
    def delete_cluster():
        return "Tearing down the big cluster!"

    create_cluster_obj = create_cluster()

    (
        create_cluster_obj
        >> run_query1()
        >> run_query2()
        >> delete_cluster().as_teardown(setups=create_cluster_obj)
    )


toy_setup_teardown_simple()
