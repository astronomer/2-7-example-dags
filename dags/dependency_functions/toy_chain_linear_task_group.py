"""
## Use chain_linear() with tasks and task groups

This DAG shows how to use the dependency function chain_linear() with tasks and task groups.
chain_linear() was added in Airflow 2.7.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime
from airflow.models.baseoperator import chain_linear
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["TaskGroup", "@task_group", "chain_linear()", "dependency_functions", "toy"],
)
def toy_chain_linear_task_group():
    t1_traditional = EmptyOperator(task_id="t1_traditional")
    t2_traditional = EmptyOperator(task_id="t2_traditional")
    t3_traditional = EmptyOperator(task_id="t3_traditional")
    t4_traditional = EmptyOperator(task_id="t4_traditional")
    t5_traditional = EmptyOperator(task_id="t5_traditional")
    t6_traditional = EmptyOperator(task_id="t6_traditional")

    @task
    def t1_TF():
        print("hi")

    t1_TF_object = t1_TF()

    @task
    def t2_TF():
        print("hi")

    t2_TF_object = t2_TF()

    with TaskGroup(group_id="tg1_traditional") as tg1_traditional:
        t3_traditional_in_tg = EmptyOperator(task_id="t3_traditional_in_tg")

        @task
        def t3_TF_in_tg():
            print("t1")

        t3_traditional_in_tg >> t3_TF_in_tg()

    @task_group
    def tg1_TF():
        t4_traditional_in_tg = EmptyOperator(task_id="t4_traditional_in_tg")

        @task
        def t4_TF_in_tg():
            print("t1")

        t4_TF_in_tg()

    tg1_TF_object = tg1_TF()

    with TaskGroup(group_id="tg2_traditional") as tg2_traditional:
        t9 = EmptyOperator(task_id="t9")
        t10 = EmptyOperator(task_id="t10")
        t11 = EmptyOperator(task_id="t11")
        t12 = EmptyOperator(task_id="t12")
        t13 = EmptyOperator(task_id="t13")

        chain_linear([t9, t10, t11], [t12, t13])

    @task_group
    def tg2_TF():
        t4_traditional_in_tg = EmptyOperator(task_id="t4_traditional_in_tg")

        @task
        def t4_TF_in_tg():
            print("t1")

        t4_TF_in_tg()

    tg2_TF_object = tg2_TF()

    with TaskGroup(group_id="tg3_traditional") as tg3_traditional:
        t9 = EmptyOperator(task_id="t9")
        t10 = EmptyOperator(task_id="t10")
        t11 = EmptyOperator(task_id="t11")
        t12 = EmptyOperator(task_id="t12")
        t13 = EmptyOperator(task_id="t13")

        chain_linear([t9, t10, t11], [t12, t13])

    chain_linear(
        t1_traditional,
        t1_TF_object,
        [t2_traditional, t2_TF_object],
        [tg1_traditional, tg1_TF_object],
        [tg2_traditional, tg2_TF_object, tg3_traditional],
        [t3_traditional, t4_traditional, t5_traditional, t6_traditional],
    )


toy_chain_linear_task_group()
