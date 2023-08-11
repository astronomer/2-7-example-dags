"""
## Toy DAG to selectively fail different elements in a setup/teardown workflow

Use params to fail tasks in this DAG and examine behavior. This DAG also demonstrates
task group teardown dependency behavior, how the DAG run is marked successful by
default even if the final teardown task fails and `on_failure_fail_dagrun`.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime
from airflow.models.param import Param


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    params={
        "fail_setup_1": Param(False, type="boolean"),
        "fail_setup_2": Param(False, type="boolean"),
        "fail_worker_1": Param(False, type="boolean"),
        "fail_worker_2": Param(False, type="boolean"),
        "fail_worker_3": Param(False, type="boolean"),
        "fail_worker_4": Param(False, type="boolean"),
        "fail_teardown_1": Param(False, type="boolean"),
        "fail_teardown_2": Param(False, type="boolean"),
        "fail_final_teardown": Param(False, type="boolean"),
    },
    tags=[".is_teardown()", "setup/teardown", "@task_group", "toy", "core"],
)
def toy_setup_teardown_task_group_and_failures():
    @task_group
    def work_in_the_cluster():
        @task
        def setup_1(**context):
            fail_setup_1 = context["params"]["fail_setup_1"]
            if fail_setup_1:
                raise Exception("Setup 1 failed!")
            return "Setting up the big cluster!"

        @task
        def setup_2(**context):
            fail_setup_2 = context["params"]["fail_setup_2"]
            if fail_setup_2:
                raise Exception("Setup 2 failed!")
            return "Configuring the big cluster!"

        @task
        def worker_1(**context):
            fail_worker_1 = context["params"]["fail_worker_1"]
            if fail_worker_1:
                raise Exception("Worker 1 failed!")
            return "Doing some work!"

        @task
        def worker_2(**context):
            fail_worker_2 = context["params"]["fail_worker_2"]
            if fail_worker_2:
                raise Exception("Worker 2 failed!")
            return "Doing more work!"

        @task
        def worker_3(**context):
            fail_worker_3 = context["params"]["fail_worker_3"]
            if fail_worker_3:
                raise Exception("Worker 3 failed!")
            return "Doing even more work!"

        @task
        def worker_4(**context):
            fail_worker_4 = context["params"]["fail_worker_4"]
            if fail_worker_4:
                raise Exception("Worker 4 failed!")
            return "Doing the last pices of the work!"

        @task
        def teardown_1(**context):
            fail_teardown_1 = context["params"]["fail_teardown_1"]
            if fail_teardown_1:
                raise Exception("Teardown 1 failed!")
            return "Tearing down the big cluster!"

        @task
        def teardown_2(**context):
            fail_teardown_2 = context["params"]["fail_teardown_2"]
            if fail_teardown_2:
                raise Exception("Teardown 2 failed!")
            return "Cleaning up after the big cluster!"

        setup_1_obj = setup_1()
        setup_2_obj = setup_2()
        teardown_1_obj = teardown_1()
        teardown_2_obj = teardown_2()

        (
            [setup_1_obj, setup_2_obj]
            >> worker_1()
            >> [worker_2(), worker_3()]
            >> worker_4()
            >> [
                teardown_1_obj.as_teardown(setups=[setup_1_obj, setup_2_obj]),
                teardown_2_obj.as_teardown(setups=[setup_1_obj, setup_2_obj]),
            ]
        )

    @task
    def run_after_taskgroup():
        return "I don't need the cluster!"

    @task
    def final_teardown(**context):
        fail_final_teardown = context["params"]["fail_final_teardown"]
        if fail_final_teardown:
            raise Exception("Final teardown failed!")
        return "Cleaning up after the DAG!"

    (
        work_in_the_cluster()
        >> run_after_taskgroup()
        >> final_teardown().as_teardown(
            # on_failure_fail_dagrun=True  # Uncomment to fail the DAG run if this task fails
        )
    )


toy_setup_teardown_task_group_and_failures()
