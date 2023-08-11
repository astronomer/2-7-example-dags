"""
## Nest an outer and inner setup/teardown workflow

Syntax DAG showing nested setup/teardown tasks.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models.param import Param


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    params={
        "fail_outer_worker_1": Param(False, type="boolean"),
        "fail_inner_worker_1": Param(False, type="boolean"),
    },
    tags=[".is_teardown()", "setup/teardown", "toy", "core"],
)
def toy_setup_teardown_nesting():
    @task
    def outer_setup():
        return "Setting up the big cluster!"

    @task
    def inner_setup():
        return "Setting up the environment on the cluster!"

    @task
    def inner_worker_1(**context):
        if context["params"]["fail_inner_worker_1"]:
            raise Exception("Inner worker failed!")
        return "Doing some work in the environment!"

    @task
    def inner_worker_2():
        return "Doing some more work in the environment!"

    @task
    def inner_teardown():
        return "Tearing down the environment on the cluster!"

    @task
    def outer_worker_1(**context):
        if context["params"]["fail_outer_worker_1"]:
            raise Exception("Outer worker failed!")
        return "Doing some other work on the cluster!"

    @task
    def outer_worker_2():
        return "Doing some more other work on the cluster!"

    @task
    def outer_worker_3():
        return (
            "Doing some work in parallel to the inner workers but I don't need the env!"
        )

    @task
    def outer_teardown():
        return "Tearing down the big cluster!"

    outer_setup_obj = outer_setup()
    inner_setup_obj = inner_setup()
    outer_teardown_obj = outer_teardown()

    (
        outer_setup_obj
        >> inner_setup_obj
        >> [inner_worker_1(), inner_worker_2()]
        >> inner_teardown().as_teardown(setups=inner_setup_obj)
        >> [outer_worker_1(), outer_worker_2()]
        >> outer_teardown_obj.as_teardown(setups=outer_setup_obj)
    )

    outer_setup_obj >> outer_worker_3() >> outer_teardown_obj


toy_setup_teardown_nesting()
