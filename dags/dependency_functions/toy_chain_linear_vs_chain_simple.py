"""
## Use chain() and chain_linear() to set task dependencies

This DAG shows simple use of `chain()` and `chain_linear()` to set task dependencies.
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
    tags=["chain()", "chain_linear()", "dependency_functions", "toy", "core"],
)
def toy_chain_linear_vs_chain_simple():
    start_chain_linear = EmptyOperator(task_id="start_chain_linear")
    end_chain_linear = EmptyOperator(task_id="end_chain_linear")
    start_chain = EmptyOperator(task_id="start_chain")
    end_chain = EmptyOperator(task_id="end_chain")

    t0 = EmptyOperator(task_id="chain_linear_t0")
    t1 = EmptyOperator(task_id="chain_linear_t1")
    t2 = EmptyOperator(task_id="chain_linear_t2")
    t3 = EmptyOperator(task_id="chain_linear_t3")
    t4 = EmptyOperator(task_id="chain_t4")
    t5 = EmptyOperator(task_id="chain_t5")
    t6 = EmptyOperator(task_id="chain_t6")
    t7 = EmptyOperator(task_id="chain_t7")

    chain_linear(start_chain_linear, [t0, t1], [t2, t3], end_chain_linear)
    chain(start_chain, [t4, t5], [t6, t7], end_chain)

    end_chain_linear >> start_chain


toy_chain_linear_vs_chain_simple()
