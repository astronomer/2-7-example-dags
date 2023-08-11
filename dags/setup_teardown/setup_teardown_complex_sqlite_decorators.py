"""
## Use setup/teardown in a complex example with three nested setup/teardown workflows

DAG that creates a permanent and temporary SQLite database, fetches data, 
inserts it and then cleans up after itself. Also shows that multiple setup/teardown tasks can be in a workflow.

Setup/teardown workflows:
1. Outer: Create temporary database -> delete temporary database (1 setup, 1 teardown)
2. Middle: Create tables in the temporary database -> delete tables in temporary database (2 setup, 1 teardown)
3. Inner: Insert data into tables in the temporary database -> empty tables in the temporary database (1 setup, 2 teardown)

This DAG needs the following SQLite connection with the id `sqlite_default`:

AIRFLOW_CONN_SQLITE_DEFAULT='{
    "conn_type": "sqlite",
    "host": "include/startrek_temp.db"
}'
"""

from airflow.decorators import dag, task_group, task, setup, teardown
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
import sqlite3
import os

STARTREK_TEMP_DB_PATH = "include/startrek_temp.db"
STARTREK_PERM_DB_PATH = "include/startrek_perm.db"


@dag(
    start_date=datetime(2023, 7, 1),
    schedule="@daily",
    catchup=False,
    params={
        "fail_fetch_data": Param(
            False,
            type="boolean",
            description="This param simulates a failure in fetching data, like an API being down in the middle setup/teardown workflow.",
        ),
        "fetch_bad_data": Param(
            False,
            type="boolean",
            description="This param simulates fetching bad quality data.",
        ),
        "fail_insert_into_perm_table": Param(
            False,
            type="boolean",
            description="This param simulates a failure in inserting data into the permanent ratings table in the inner setup/teardown workflow.",
        ),
    },
    tags=["@setup", "@teardown", "setup/teardown"],
)
def setup_teardown_complex_sqlite_decorators():
    @task
    def create_perm_db():
        conn = sqlite3.connect(STARTREK_PERM_DB_PATH)
        conn.close()

    create_perm_db_obj = create_perm_db()

    @task
    def create_table_perm_ratings():
        conn = sqlite3.connect(STARTREK_PERM_DB_PATH)
        c = conn.cursor()
        c.execute(
            """
                CREATE TABLE IF NOT EXISTS perm_ratings
                (series text PRIMARY KEY, average_rating integer, number_of_ratings integer)
                """
        )
        conn.commit()
        conn.close()

    create_table_perm_ratings_obj = create_table_perm_ratings()
    create_perm_db_obj >> create_table_perm_ratings_obj

    @setup
    def create_temp_db():
        conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
        conn.close()

    create_temp_db_obj = create_temp_db()

    @setup
    def create_table_star_trek_series(**context):
        conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
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
        conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
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
            fail_fetch_data = context["params"]["fail_fetch_data"]
            fetch_bad_data = context["params"]["fetch_bad_data"]
            if fail_fetch_data:
                raise Exception("Oh no! The API to get Star Trek ratings is down!")
            if fetch_bad_data:
                return [
                    ["user1", "TNG", 5],
                    ["user1", "VOY", "There is coffee in that nebula!"],
                    ["user2", "TOS", 5],
                    ["user2", "TNG", 5],
                ]
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
        def insert_ratings_data(ratings_data):
            conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
            c = conn.cursor()
            c.executemany("INSERT INTO ratings VALUES (?,?,?)", ratings_data)
            conn.commit()
            conn.close()

        insert_ratings_data_obj = insert_ratings_data(fetch_data())

        data_quality_check = SQLColumnCheckOperator(
            task_id="data_quality_check",
            conn_id="sqlite_default",
            table="ratings",
            column_mapping={
                "user": {"null_check": {"equal_to": 0}},
                "series": {"null_check": {"equal_to": 0}},
                "rating": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 5},
                },
            },
        )

        @task
        def update_star_trek_series():
            conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
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
        def insert_into_perm(**context):
            conn_temp = sqlite3.connect(STARTREK_TEMP_DB_PATH)
            conn_perm = sqlite3.connect(STARTREK_PERM_DB_PATH)

            c_temp = conn_temp.cursor()
            c_perm = conn_perm.cursor()

            c_temp.execute(
                """SELECT series, AVG(rating), COUNT(rating) 
                FROM ratings 
                GROUP BY series"""
            )
            ratings_data = c_temp.fetchall()

            fail_insert_perm_ratings_series = context["params"][
                "fail_insert_into_perm_table"
            ]
            if fail_insert_perm_ratings_series:
                raise Exception("Oh no! The data could not be inserted into the table!")

            c_perm.executemany(
                "INSERT OR REPLACE INTO perm_ratings (series, average_rating, number_of_ratings) VALUES (?,?,?)",
                ratings_data,
            )

            conn_temp.commit()
            conn_perm.commit()

            conn_temp.close()
            conn_perm.close()

        insert_into_perm_obj = insert_into_perm()

        @teardown
        def empty_ratings_table():
            conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
            c = conn.cursor()

            c.execute(f"DELETE FROM ratings")

            conn.commit()
            conn.close()

        empty_ratings_table_obj = empty_ratings_table()

        @teardown
        def empty_series_table():
            conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
            c = conn.cursor()

            c.execute(f"DELETE FROM star_trek_series")

            conn.commit()
            conn.close()

        empty_series_table_obj = empty_series_table()

        tables_empty = EmptyOperator(task_id="tables_empty")

        # setting dependencies within the task group
        chain(
            insert_ratings_data_obj,
            data_quality_check,
            update_star_trek_series_obj,
            insert_into_perm_obj,
            [empty_ratings_table_obj, empty_series_table_obj],
            tables_empty,
        )

        # setting dependencies between the setup task and its two teardown tasks
        # creating the inner setup/teardown pairing
        insert_ratings_data_obj >> [empty_ratings_table_obj, empty_series_table_obj]

    data_transformation_tg_obj = data_transformation()

    @teardown
    def delete_temp_tables():
        conn = sqlite3.connect(STARTREK_TEMP_DB_PATH)
        c = conn.cursor()
        c.execute("DROP TABLE star_trek_series")
        c.execute("DROP TABLE ratings")
        conn.commit()
        conn.close()

    delete_temp_tables_obj = delete_temp_tables()

    @teardown
    def delete_temp_db():
        os.remove(STARTREK_TEMP_DB_PATH)

    delete_temp_db_obj = delete_temp_db()

    @task
    def query_perm():
        conn = sqlite3.connect(STARTREK_PERM_DB_PATH)
        c = conn.cursor()
        r = c.execute(
            "SELECT AVG(average_rating), SUM(number_of_ratings) FROM perm_ratings"
        )
        re = r.fetchall()
        print(
            f"The average rating of all Star Trek series was {round(re[0][0])}/5 after {re[0][1]} ratings were cast!"
        )

    # setting dependencies outside the task group
    chain(
        create_temp_db_obj,
        [create_table_ratings_obj, create_table_star_trek_series_obj],
        data_transformation_tg_obj,
        delete_temp_tables_obj,
        delete_temp_db_obj,
        query_perm(),
    )
    create_table_perm_ratings_obj >> data_transformation_tg_obj

    [
        create_table_ratings_obj,
        create_table_star_trek_series_obj,
    ] >> delete_temp_tables_obj

    # setting dependencies between the three setup task and its teardown task
    # creating the outer setup/teardown workflow
    create_temp_db_obj >> delete_temp_db_obj


setup_teardown_complex_sqlite_decorators()
