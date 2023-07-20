"""
## Compare the chain() and chain_linear() functions to set dependencies

This DAG shows the resulting dependencies form using chain() and chain_linear().
chain_linear() was added in Airflow 2.7.
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.models.baseoperator import chain, chain_linear, cross_downstream
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["chain_linear", "chain"],
)
def dependency_functions():
    # ------------ #
    # chain_linear #
    # ------------ #

    t1_chain_linear = EmptyOperator(task_id="t1_chain_linear")
    t2_chain_linear = EmptyOperator(task_id="t2_chain_linear")
    t3_chain_linear = EmptyOperator(task_id="t3_chain_linear")
    t4_chain_linear = EmptyOperator(task_id="t4_chain_linear")
    t5_chain_linear = EmptyOperator(task_id="t5_chain_linear")
    t6_chain_linear = EmptyOperator(task_id="t6_chain_linear")
    t7_chain_linear = EmptyOperator(task_id="t7_chain_linear")
    t8_chain_linear = EmptyOperator(task_id="t8_chain_linear")
    t9_chain_linear = EmptyOperator(task_id="t9_chain_linear")
    t10_chain_linear = EmptyOperator(task_id="t10_chain_linear")
    t11_chain_linear = EmptyOperator(task_id="t11_chain_linear")
    t12_chain_linear = EmptyOperator(task_id="t12_chain_linear")
    t13_chain_linear = EmptyOperator(task_id="t13_chain_linear")
    t14_chain_linear = EmptyOperator(task_id="t14_chain_linear")

    chain_linear(
        t1_chain_linear,
        t2_chain_linear,
        [t3_chain_linear, t4_chain_linear, t5_chain_linear],
        [t6_chain_linear, t7_chain_linear, t8_chain_linear],
        t9_chain_linear,
        [t10_chain_linear, t11_chain_linear],
        [t12_chain_linear, t13_chain_linear, t14_chain_linear],
    )

    # ----- #
    # chain #
    # ----- #

    t1_chain = EmptyOperator(task_id="t1_chain")
    t2_chain = EmptyOperator(task_id="t2_chain")
    t3_chain = EmptyOperator(task_id="t3_chain")
    t4_chain = EmptyOperator(task_id="t4_chain")
    t5_chain = EmptyOperator(task_id="t5_chain")
    t6_chain = EmptyOperator(task_id="t6_chain")
    t7_chain = EmptyOperator(task_id="t7_chain")
    t8_chain = EmptyOperator(task_id="t8_chain")
    t9_chain = EmptyOperator(task_id="t9_chain")
    t10_chain = EmptyOperator(task_id="t10_chain")
    t11_chain = EmptyOperator(task_id="t11_chain")

    chain(
        t1_chain,
        t2_chain,
        [t3_chain, t4_chain, t5_chain],
        [t6_chain, t7_chain, t8_chain],
        t9_chain,
        [t10_chain, t11_chain],
    )
    # the below code would error with `Chain not supported for different length Iterable. Got 3 and 2.`
    # chain([t1_chain, t2_chain], [t3_chain, t4_chain, t5_chain])

    # ---------------- #
    # cross_downstream #
    # ---------------- #

    # t1_cross_downstream = EmptyOperator(task_id="t1_cross_downstream")
    # t2_cross_downstream = EmptyOperator(task_id="t2_cross_downstream")
    # t3_cross_downstream = EmptyOperator(task_id="t3_cross_downstream")
    # t4_cross_downstream = EmptyOperator(task_id="t4_cross_downstream")
    # t5_cross_downstream = EmptyOperator(task_id="t5_cross_downstream")

    # cross downstream only takes 2 arguments and both need to be lists
    # the lists can be of different lengths. All tasks in the second list will be
    # downstream of all tasks in the first list.
    # cross_downstream(
    #     [t1_cross_downstream, t2_cross_downstream],
    #     [t3_cross_downstream, t4_cross_downstream, t5_cross_downstream],
    # )


dependency_functions()
