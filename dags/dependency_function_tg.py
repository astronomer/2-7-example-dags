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
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["TaskGroup", "@task_group", "chain_linear"],
)
def dependency_function_tg():
    t1_traditional = EmptyOperator(task_id="t1_traditional")
    t2_traditional = EmptyOperator(task_id="t2_traditional")

    @task
    def t7_TF():
        print("t7")

    t7_TF_object = t7_TF()

    @task
    def t8_TF():
        print("t8")

    t8_TF_object = t8_TF()

    with TaskGroup(group_id="tg1_traditional") as tg1:
        t3_traditional_in_tg = EmptyOperator(task_id="t3_traditional_in_tg")

        @task
        def t4_TF_in_tg():
            print("t1")

        t3_traditional_in_tg >> t4_TF_in_tg()

    @task_group
    def tg2_TF():
        t5_traditional_in_tg = EmptyOperator(task_id="t5_traditional_in_tg")

        @task
        def t6_TF_in_tg():
            print("t1")

        t6_TF_in_tg()

    tg2_TF_object = tg2_TF()

    # use dependency function with tasks and task groups
    chain_linear(
        t1_traditional, t2_traditional, tg1, tg2_TF_object, t7_TF_object, t8_TF_object
    )

    with TaskGroup(group_id="tg3_traditional") as tg3:
        t9 = EmptyOperator(task_id="t9")
        t10 = EmptyOperator(task_id="t10")
        t11 = EmptyOperator(task_id="t11")
        t12 = EmptyOperator(task_id="t12")
        t13 = EmptyOperator(task_id="t13")

        chain_linear([t9, t10, t11], [t12, t13])

    with TaskGroup(group_id="tg4_traditional") as tg4:
        t14 = EmptyOperator(task_id="t14")
        t15 = EmptyOperator(task_id="t15")
        t16 = EmptyOperator(task_id="t16")

    with TaskGroup(group_id="tg5_traditional") as tg5:
        t17 = EmptyOperator(task_id="t17")
        t18 = EmptyOperator(task_id="t18")
        t19 = EmptyOperator(task_id="t19")

        t17 >> t18 >> t19

    with TaskGroup(group_id="tg6_traditional") as tg6:
        t20 = EmptyOperator(task_id="t20")

    with TaskGroup(group_id="tg7_traditional") as tg7:
        t21 = EmptyOperator(task_id="t21")

    t22 = EmptyOperator(task_id="t22")
    t23 = EmptyOperator(task_id="t23")

    chain_linear(t22, [tg3, tg4], [tg5, tg6, tg7], t23)


dependency_function_tg()
