"""
## This is a helper DAG

This DAG shows the pipeline used in the basic setup/teardown example without
having setup/teardown in place.
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime
import os
import csv
import time


def get_params_helper(**context):
    folder = context["params"]["folder"]
    filename = context["params"]["filename"]
    cols = context["params"]["cols"]
    return folder, filename, cols


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    params={
        "folder": "include/my_data",
        "filename": "data.csv",
        "cols": ["id", "name", "age"],
        "fetch_bad_data": Param(False, type="boolean"),
    },
    tags=["helper"],
)
def no_setup_teardown():
    @task
    def create_csv(**context):
        folder, filename, cols = get_params_helper(**context)

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(f"{folder}/{filename}", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows([cols])

    @task
    def fetch_data(**context):
        bad_data = context["params"]["fetch_bad_data"]

        if bad_data:
            return [
                [1, "Joe", "Forty"],
                [2, "Tom", 29],
                [3, "Lea", 19],
            ]
        else:
            return [
                [1, "Joe", 40],
                [2, "Tom", 29],
                [3, "Lea", 19],
            ]

    @task
    def write_to_csv(data, **context):
        folder, filename, cols = get_params_helper(**context)

        with open(f"{folder}/{filename}", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)

        time.sleep(10)

    @task
    def get_average_age(**context):
        folder, filename, cols = get_params_helper(**context)

        with open(f"{folder}/{filename}", "r", newline="") as f:
            reader = csv.reader(f)
            next(reader)
            ages = [int(row[2]) for row in reader]

        return sum(ages) / len(ages)

    @task
    def delete_csv(**context):
        folder, filename, cols = get_params_helper(**context)

        os.remove(f"{folder}/{filename}")

        if not os.listdir(f"{folder}"):
            os.rmdir(f"{folder}")

    create_csv_obj = create_csv()
    fetch_data_obj = fetch_data()
    write_to_csv_obj = write_to_csv(fetch_data_obj)
    get_average_age_obj = get_average_age()
    delete_csv_obj = delete_csv()

    chain(create_csv_obj, write_to_csv_obj, get_average_age_obj, delete_csv_obj)


no_setup_teardown()
