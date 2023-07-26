"""
## Use chain() and chain_linear() to set complex task dependencies

This DAG shows more complex use of `chain()` and `chain_linear()` to set task dependencies.
chain_linear() was added in Airflow 2.7.
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.models.baseoperator import chain_linear, chain
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["chain()", "chain_linear()", "dependency_functions"],
)
def chain_linear_vs_chain_complex():
    start_chain_linear = EmptyOperator(task_id="start_chain_linear")
    end_chain_linear = EmptyOperator(task_id="end_chain_linear")
    start_chain = EmptyOperator(task_id="start_chain")
    end_chain = EmptyOperator(task_id="end_chain")
    empty_1 = EmptyOperator(task_id="empty_1")
    empty_2 = EmptyOperator(task_id="empty_2")

    t0 = EmptyOperator(task_id="chain_linear_t0")
    t1 = EmptyOperator(task_id="chain_linear_t1")
    t2 = EmptyOperator(task_id="chain_linear_t2")
    t3 = EmptyOperator(task_id="chain_linear_t3")
    t4 = EmptyOperator(task_id="chain_linear_t4")
    t5 = EmptyOperator(task_id="chain_linear_t5")
    t6 = EmptyOperator(task_id="chain_linear_t6")
    t7 = EmptyOperator(task_id="chain_linear_t7")
    t8 = EmptyOperator(task_id="chain_linear_t8")
    t9 = EmptyOperator(task_id="chain_linear_t9")
    t10 = EmptyOperator(task_id="chain_t10")
    t11 = EmptyOperator(task_id="chain_t11")
    t12 = EmptyOperator(task_id="chain_t12")
    t13 = EmptyOperator(task_id="chain_t13")
    t14 = EmptyOperator(task_id="chain_t14")
    t15 = EmptyOperator(task_id="chain_t15")
    t16 = EmptyOperator(task_id="chain_t16")
    t17 = EmptyOperator(task_id="chain_t17")
    t18 = EmptyOperator(task_id="chain_t18")
    t19 = EmptyOperator(task_id="chain_t19")

    chain_linear(
        start_chain_linear,
        [t0, t1],
        [t2, t3, t4],
        [t5, t6, t7],
        [t8, t9],
        end_chain_linear,
    )

    # chain cannot set dependencies between lists of different lengths!
    chain(
        start_chain,
        [t10, t11],
        empty_1,
        [t12, t13, t14],
        [t15, t16, t17],
        empty_2,
        [t18, t19],
        end_chain,
    )

    end_chain_linear >> start_chain


chain_linear_vs_chain_complex()
