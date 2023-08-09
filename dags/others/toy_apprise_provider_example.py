from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.apprise.notifications.apprise import send_apprise_notification
from apprise import NotifyType
from pendulum import datetime


@dag(
    dag_id="apprise_notifier_testing",
    schedule_interval=None,
    start_date=datetime(2023, 8, 1),
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
                body="Hi",
                notify_type=NotifyType.FAILURE,
            )
        ],
        bash_command="fail",
    )

    BashOperator(
        task_id="bash_success",
        on_success_callback=[
            send_apprise_notification(
                apprise_conn_id="apprise_default",
                body="Hi",
                notify_type=NotifyType.FAILURE,
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
        raise Exception("I failed! :(")

    python_fail()


toy_apprise_provider_example()
