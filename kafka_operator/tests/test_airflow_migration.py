"""Tests for the Airflow -> bundle converter."""

from kafka_operator.airflow_migration import (
    convert_await_message_sensor,
    extract_await_message_sensors,
    migrate_dag,
)

DAG_SOURCE = '''
from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

with DAG("kafka_roundtrip") as dag:
    await_done = AwaitMessageSensor(
        task_id="await_done",
        kafka_config_id="kafka_default",
        topics=["rtm-source"],
        apply_function="dags.fns.await_done",
        poll_interval=30,
    )
'''


def _params(task: dict) -> dict:
    return {p["name"]: p["value"] for p in task["python_operator_task"]["parameters"]}


def test_convert_maps_core_fields():
    task = convert_await_message_sensor(
        task_id="await_done", topics=["rtm-source"], match_value_contains="done", poll_interval=30
    )
    assert task["task_key"] == "await_done"
    assert task["python_operator_task"]["main"].endswith("KafkaMessageSensor")
    params = _params(task)
    assert params["topic"] == "rtm-source"
    assert params["match_value_contains"] == "done"
    assert params["defer_seconds"] == "30"  # poll_interval -> defer_seconds, stringified


def test_convert_empty_match_is_any_message():
    params = _params(convert_await_message_sensor(task_id="t", topics=["x"]))
    assert params["match_value_contains"] == ""


def test_convert_uses_first_topic_and_optional_timeout():
    params = _params(
        convert_await_message_sensor(task_id="t", topics=["a", "b"], timeout_seconds=1200)
    )
    assert params["topic"] == "a"
    assert params["timeout_seconds"] == "1200"


def test_extract_finds_sensor_kwargs():
    sensors = extract_await_message_sensors(DAG_SOURCE)
    assert len(sensors) == 1
    assert sensors[0]["task_id"] == "await_done"
    assert sensors[0]["topics"] == ["rtm-source"]
    assert sensors[0]["poll_interval"] == 30


def test_migrate_dag_produces_deployable_job():
    bundle = migrate_dag(
        DAG_SOURCE, job_name="kafka_roundtrip", matches={"await_done": "done"}, schedule_cron="0 0 0 * * ?"
    )
    job = bundle["resources"]["jobs"]["kafka_roundtrip"]
    assert job["schedule"]["quartz_cron_expression"] == "0 0 0 * * ?"
    assert len(job["tasks"]) == 1
    params = _params(job["tasks"][0])
    assert params["match_value_contains"] == "done"
    assert job["environments"][0]["spec"]["environment_version"] == "5"
