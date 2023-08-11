"""
## Send notifications through the Apprise provider

This DAG shows how to use the Apprise provider to send notifications. Which services to 
send the notifications to is configured in the Airflow connection `apprise_default`.

Needs the Apprise provider, apprise package and the following connection:

    conn_id: apprise_default
    conn_type: apprise
    config: {"path": "your apprise URI"}

Learn more at: 
- https://github.com/caronc/apprise
- https://airflow.apache.org/docs/apache-airflow-providers-apprise/stable/index.html
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.apprise.notifications.apprise import send_apprise_notification
from apprise import NotifyType
from pendulum import datetime


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    on_failure_callback=[
        send_apprise_notification(
            apprise_conn_id="apprise_default",
            body="The dag {{ dag.dag_id }} failed",
            notify_type=NotifyType.FAILURE,
        )
    ],
    tags=["apprise", "toy"],
)
def toy_apprise_provider_example():
    BashOperator(
        task_id="bash_fail",
        on_failure_callback=[
            send_apprise_notification(
                apprise_conn_id="apprise_default",
                body="The task {{ ti.task_id }} in {{ run_id }} failed!",
                notify_type=NotifyType.FAILURE,
            )
        ],
        bash_command="fail",  # this task will fail
    )

    BashOperator(
        task_id="bash_success",
        on_success_callback=[
            send_apprise_notification(
                apprise_conn_id="apprise_default",
                body="The task {{ ti.task_id }} in {{ run_id }} failed!",
                notify_type=NotifyType.SUCCESS,
            )
        ],
        bash_command="echo hello",
    )

    @task(
        on_failure_callback=[
            send_apprise_notification(
                apprise_conn_id="apprise_default",
                body="The task {{ ti.task_id }} in {{ run_id }} failed!",
                notify_type=NotifyType.FAILURE,
            )
        ],
    )
    def python_fail():
        raise Exception("I failed! :(")  # this task will fail

    python_fail()


toy_apprise_provider_example()
