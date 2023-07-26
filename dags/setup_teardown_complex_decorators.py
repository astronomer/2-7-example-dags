"""
## Use `@setup` and `@teardown` in a simple local example to enable setup/teardown functionality

DAG that uses setup/teardown to prepare a CSV file to write to and then showcases the
behavior in case faulty data is fetched.
"""

from airflow.decorators import dag, task_group, task, setup, teardown
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
import sqlite3
import os
import csv
import time


def get_params_helper(**context):
    folder = context["params"]["folder"]
    db_name_temp = context["params"]["db_name_temp"]
    db_name_perm = context["params"]["db_name_perm"]
    return folder, db_name_temp, db_name_perm


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    params={
        "folder": "include",
        "db_name_temp": "startrek_temp",
        "db_name_perm": "startrek_perm",
        "data_format_problem": Param(False, type="boolean"),
        "database_problem": Param(False, type="boolean"),
    },
    tags=["@setup", "@teardown", "setup/teardown"],
)
def setup_teardown_complex_decorators():
    @task
    def create_perm_db(**context):
        folder, db_name_temp, db_name_perm = get_params_helper(**context)
        conn = sqlite3.connect(f"{folder}/{db_name_perm}.db")
        conn.close()

    create_perm_db_obj = create_perm_db()

    @task
    def create_table_most_rated(**context):
        folder, db_name_temp, db_name_perm = get_params_helper(**context)
        conn = sqlite3.connect(f"{folder}/{db_name_perm}.db")
        c = conn.cursor()
        c.execute(
            """
                CREATE TABLE IF NOT EXISTS most_rated
                (series text, average_rating integer, number_of_ratings integer)
                """
        )
        conn.commit()
        conn.close()

    create_table_most_rated_obj = create_table_most_rated()
    create_perm_db_obj >> create_table_most_rated_obj

    @setup
    def create_temp_db(**context):
        folder, db_name_temp, db_name_perm = get_params_helper(**context)
        conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
        conn.close()

    create_temp_db_obj = create_temp_db()

    @setup
    def create_table_star_trek_series(**context):
        folder, db_name_temp, db_name_perm = get_params_helper(**context)
        conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
        c = conn.cursor()
        c.execute(
            """
                CREATE TABLE star_trek_series
                (series text, average_rating integer, number_of_ratings integer)
                """
        )
        conn.commit()
        conn.close()

    create_table_star_trek_series_obj = create_table_star_trek_series()

    @setup
    def create_table_ratings(**context):
        folder, db_name_temp, db_name_perm = get_params_helper(**context)
        conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
        c = conn.cursor()
        c.execute(
            """
                CREATE TABLE ratings
                (user text, series text, rating integer)
                """
        )
        conn.commit()
        conn.close()

    create_table_ratings_obj = create_table_ratings()

    @task_group
    def data_transformation():
        @task
        def fetch_data(**context):
            data_format_problem = context["params"]["data_format_problem"]
            if data_format_problem:
                return [
                    ["user1", "TNG", "5 Stars!"],
                    ["user2", "TOS", 5],
                    ["user3", "TOS", 5],
                ]
            else:
                return [
                    ["user1", "TNG", 5],
                    ["user1", "DS9", 5],
                    ["user1", "VOY", 5],
                    ["user2", "TOS", 5],
                    ["user2", "TNG", 5],
                    ["user2", "ENT", 5],
                    ["user3", "TOS", 5],
                    ["user3", "TNG", 5],
                    ["user3", "DS9", 5],
                    ["user3", "VOY", 5],
                ]

        @setup
        def insert_ratings_data(ratings_data, **context):
            folder, db_name_temp, db_name_perm = get_params_helper(**context)
            conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
            c = conn.cursor()
            c.executemany("INSERT INTO ratings VALUES (?,?,?)", ratings_data)
            conn.commit()
            conn.close()

        insert_ratings_data_obj = insert_ratings_data(fetch_data())

        @task
        def update_star_trek_series(**context):
            folder, db_name_temp, db_name_perm = get_params_helper(**context)
            conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
            c = conn.cursor()
            c.execute(
                """
                    INSERT INTO star_trek_series (series, average_rating, number_of_ratings)
                    SELECT series, AVG(rating), COUNT(rating) FROM ratings GROUP BY series
                    """
            )
            conn.commit()
            conn.close()

        update_star_trek_series_obj = update_star_trek_series()

        @task
        def insert_most_rated_series(**context):
            folder, db_name_temp, db_name_perm = get_params_helper(**context)
            conn_temp = sqlite3.connect(f"{folder}/{db_name_temp}.db")
            conn_perm = sqlite3.connect(f"{folder}/{db_name_perm}.db")

            c_temp = conn_temp.cursor()
            c_perm = conn_perm.cursor()

            c_temp.execute(
                "SELECT series, AVG(rating), COUNT(rating) FROM ratings GROUP BY series"
            )
            ratings_data = c_temp.fetchall()

            c_perm.executemany(
                "INSERT INTO most_rated (series, average_rating, number_of_ratings) VALUES (?,?,?)",
                ratings_data,
            )

            conn_temp.commit()
            conn_perm.commit()

            conn_temp.close()
            conn_perm.close()

        insert_most_rated_series_obj = insert_most_rated_series()

        @teardown
        def empty_ratings_table(**context):
            folder, db_name_temp, db_name_perm = get_params_helper(**context)
            conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
            c = conn.cursor()

            c.execute(f"DELETE FROM ratings")

            conn.commit()
            conn.close()

        empty_ratings_table_obj = empty_ratings_table()

        @teardown
        def empty_series_table(**context):
            folder, db_name_temp, db_name_perm = get_params_helper(**context)
            conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
            c = conn.cursor()

            c.execute(f"DELETE FROM star_trek_series")

            conn.commit()
            conn.close()

        empty_series_table_obj = empty_series_table()

        chain(
            insert_ratings_data_obj,
            update_star_trek_series_obj,
            insert_most_rated_series_obj,
            [empty_ratings_table_obj, empty_series_table_obj],
        )
        insert_ratings_data_obj >> [empty_ratings_table_obj, empty_series_table_obj]

    data_transformation_tg_obj = data_transformation()

    @teardown
    def delete_temp_tables(**context):
        folder, db_name_temp, db_name_perm = get_params_helper(**context)
        conn = sqlite3.connect(f"{folder}/{db_name_temp}.db")
        c = conn.cursor()
        c.execute("DROP TABLE star_trek_series")
        c.execute("DROP TABLE ratings")
        conn.commit()
        conn.close()

    delete_temp_tables_obj = delete_temp_tables()

    @teardown
    def delete_temp_db(**context):
        folder, db_name_temp, db_name_perm = get_params_helper(**context)
        os.remove(f"{folder}/{db_name_temp}.db")

    delete_temp_db_obj = delete_temp_db()

    chain(
        create_temp_db_obj,
        [create_table_ratings_obj, create_table_star_trek_series_obj],
        data_transformation_tg_obj,
        delete_temp_tables_obj,
        delete_temp_db_obj,
    )
    create_table_most_rated_obj >> data_transformation_tg_obj

    create_temp_db_obj >> delete_temp_db_obj


setup_teardown_complex_decorators()
