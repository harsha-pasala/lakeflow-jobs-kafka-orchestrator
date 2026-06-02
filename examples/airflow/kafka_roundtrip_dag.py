"""Example Airflow DAG — the migration source for examples/airflow/README.md.

Waits for a Kafka message whose body contains "done", then notifies. Run the
converter in ``kafka_operator.airflow_migration`` over this file to produce the
equivalent Databricks bundle job (see kafka_roundtrip.generated.yml).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor


def await_done(message):
    """apply_function: match when the message body contains 'done'."""
    if b"done" in message.value():
        return message


def post_to_slack():
    """Downstream notifier (stub)."""
    ...


with DAG(
    dag_id="kafka_roundtrip",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    await_done_task = AwaitMessageSensor(
        task_id="await_done",
        kafka_config_id="kafka_default",
        topics=["rtm-source"],
        apply_function="dags.kafka_roundtrip_dag.await_done",
        poll_interval=30,
        poll_timeout=1,
    )
    notify = PythonOperator(task_id="notify", python_callable=post_to_slack)

    await_done_task >> notify
