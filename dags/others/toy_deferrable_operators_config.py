"""
## Toy DAG showing how to turn on deferrable mode for an operator

The default value for many operators `deferrable` argument is:
conf.getboolean("operators", "default_deferrable", fallback=False),
"""

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["deferrable", "toy", "core"],
)
def toy_deferrable_operators_config():
    trigger_dag_run = TriggerDagRunOperator(
        task_id="trigger_dag_run",
        trigger_dag_id="helper_dag_wait_30_seconds",
        wait_for_completion=True,
        poke_interval=20,
        # deferrable=False,  # if not set at the DAG level, will use the config
    )

    @task
    def celebrate():
        print("The downstream DAG has finished running!")

    trigger_dag_run >> celebrate()


toy_deferrable_operators_config()
