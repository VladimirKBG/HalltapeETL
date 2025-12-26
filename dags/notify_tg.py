import pendulum

from airflow.sdk import dag
from airflow.providers.telegram.operators.telegram import TelegramOperator

@dag(
    dag_id="notify_tg",
    start_date=pendulum.datetime(2025, 12, 25, 12, 00),
    schedule=None,
    description="Send message from params to telegram.",
    tags=["notification"],
)
def notify_tg():
    notification = TelegramOperator(
        task_id="send_notification",
        telegram_conn_id="notification_telegram",
        text="{{ dag_run.conf.get('message', 'No message provided from upstream') }} at {{ dag_run.conf.get('logical_date', ts) }}",
    )

notify_tg()
