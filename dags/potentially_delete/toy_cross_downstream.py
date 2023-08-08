"""
## Use cross_downstream() to set task dependencies

This DAG shows the use of `cross_downstream()` to set task dependencies.
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.models.baseoperator import cross_downstream
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["cross_downstream()", "dependency_functions", "toy"],
)
def toy_cross_downstream():
    t1_cross_downstream = EmptyOperator(task_id="t1_cross_downstream")
    t2_cross_downstream = EmptyOperator(task_id="t2_cross_downstream")
    t3_cross_downstream = EmptyOperator(task_id="t3_cross_downstream")
    t4_cross_downstream = EmptyOperator(task_id="t4_cross_downstream")
    t5_cross_downstream = EmptyOperator(task_id="t5_cross_downstream")

    # cross downstream only takes 2 arguments and both need to be lists
    # the lists can be of different lengths. All tasks in the second list will be
    # downstream of all tasks in the first list, creating a similar patter as
    # chain_linear() but only with 2 lists
    cross_downstream(
        [t1_cross_downstream, t2_cross_downstream],
        [t3_cross_downstream, t4_cross_downstream, t5_cross_downstream],
    )

# broken as of nightly 2023-07-28
# toy_cross_downstream()
