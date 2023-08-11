"""
## Use the fail_stop parameter to stop a DAG run when any task in it fails

This DAG has fail stop enabled and a task that always fails showing how
tasks that are still running are marked as failed, tasks that have not run yet
are marked as skipped.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import time


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    fail_stop=True,
    tags=["fast_stop", "toy", "core"],
)
def toy_fail_stop():
    @task
    def waits_2s():
        time.sleep(2)
        print("I succeed!")

    @task
    def fails_after_10s():
        time.sleep(10)
        print("I fail! :(")
        raise Exception("I failed!")

    @task
    def waits_3s():
        time.sleep(3)
        print("I am fast enough to succeed!")

    @task
    def waits_60s():
        time.sleep(60)
        print("I am slow!")

    @task
    def waits_120s():
        time.sleep(120)
        print("I am even slower!")

    @task
    def waits_150s():
        time.sleep(150)
        print("I'm too slow as well!")

    @task
    def downstream_1():
        print("I'll never get a chance to run!")

    @task
    def downstream_2():
        print("I'll also never run!")

    @task
    def downstream_3():
        print("I'll also never run!")

    # When using fail_stop=True you cannot have tasks with a trigger rule other than
    # all_success in that DAG. Uncomment the following to see the import error:
    """@task(
        trigger_rule="all_done",
    )
    def downstream_4():
        print("I cause an import error due to my trigger rule!")

    downstream_4()"""

    # setting dependencies
    fails_after_10s_obj = fails_after_10s()
    waits_120s_obj = waits_120s()
    waits_3s_obj = waits_3s()
    waits_2s() >> [fails_after_10s_obj, waits_3s_obj, waits_60s(), waits_120s_obj]
    waits_3s_obj >> waits_150s()
    fails_after_10s_obj >> downstream_1()
    waits_120s_obj >> downstream_2() >> downstream_3()


toy_fail_stop()
