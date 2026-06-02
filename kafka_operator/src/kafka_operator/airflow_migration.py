"""Convert Airflow Kafka-sensor definitions into this bundle's tasks.

The Airflow ``AwaitMessageSensor`` is callable-driven (you pass an
``apply_function``); this bundle's ``KafkaMessageSensor`` is declarative (you
pass parameters). This module bridges the two:

- :func:`convert_await_message_sensor` — pure, agent-callable mapping from an
  Airflow sensor's parameters to a Databricks ``python_operator_task`` task dict.
- :func:`extract_await_message_sensors` — AST-scan a DAG source file for every
  ``AwaitMessageSensor(...)`` and pull its keyword arguments.
- :func:`migrate_dag` — combine the two: turn a DAG source string into a bundle
  ``resources.jobs`` definition ready to deploy.

Run as a CLI to migrate a DAG file:

    python -m kafka_operator.airflow_migration path/to/dag.py \\
        --job kafka_roundtrip --match await_done=done --schedule "0 0 0 * * ?"
"""

from __future__ import annotations

import argparse
import ast
import sys

# Bundle variable references emitted into the generated task. They line up with
# the variables declared in databricks.yml, so the migrated job is drop-in.
_CONNECTION_PARAMS = [
    ("bootstrap_servers", "${var.bootstrap_servers}"),
    ("secret_scope", "${var.secret_scope}"),
    ("secret_key", "${var.secret_key}"),
]
_SENSOR_MAIN = "kafka_operator.kafka_message_sensor.KafkaMessageSensor"


def convert_await_message_sensor(
    *,
    task_id: str,
    topics: list[str] | str,
    match_value_contains: str = "",
    poll_interval: int = 30,
    timeout_seconds: int | None = None,
    group_id: str | None = None,
) -> dict:
    """Map an Airflow ``AwaitMessageSensor`` to a ``KafkaMessageSensor`` task.

    Args:
        task_id: The Airflow ``task_id``; becomes the bundle ``task_key``.
        topics: The sensor's ``topics``. ``KafkaMessageSensor`` watches a single
            topic, so the first entry is used.
        match_value_contains: Substring the body must contain to complete. This
            is the human-readable intent of the Airflow ``apply_function`` — leave
            empty to complete on **any** message received on the topic.
        poll_interval: Airflow ``poll_interval`` seconds; becomes ``defer_seconds``.
        timeout_seconds: Optional hard wall-clock ceiling across deferrals.
        group_id: Kafka consumer group. Defaults to a per-run group so each job
            run gets a clean watch.

    Returns:
        A task dict shaped like a Databricks Jobs ``python_operator_task``,
        ready to drop under ``resources.jobs.<job>.tasks``.
    """
    topic = topics[0] if isinstance(topics, (list, tuple)) else topics
    group_id = group_id or "kafka-sensor-{{job.id}}-{{job.run_id}}"

    params = [
        ("task_key", "{{task.name}}"),
        ("topic", topic),
        ("group_id", group_id),
    ]
    params += _CONNECTION_PARAMS
    params.append(("match_value_contains", match_value_contains))
    params.append(("defer_seconds", str(int(poll_interval))))
    if timeout_seconds is not None:
        params.append(("timeout_seconds", str(int(timeout_seconds))))

    return {
        "task_key": task_id,
        "environment_key": "default",
        "python_operator_task": {
            "main": _SENSOR_MAIN,
            "parameters": [{"name": name, "value": value} for name, value in params],
        },
    }


def _call_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _literal(node: ast.expr):
    """Best-effort literal extraction; falls back to the unparsed expression."""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return f"<expr:{ast.unparse(node)}>"


def extract_await_message_sensors(dag_source: str) -> list[dict]:
    """Return the keyword arguments of every ``AwaitMessageSensor(...)`` call.

    Args:
        dag_source: The source code of an Airflow DAG file.

    Returns:
        One dict of ``{kwarg: value}`` per sensor instantiation found.
    """
    tree = ast.parse(dag_source)
    found: list[dict] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node.func) == "AwaitMessageSensor":
            found.append({kw.arg: _literal(kw.value) for kw in node.keywords if kw.arg})
    return found


def migrate_dag(
    dag_source: str,
    *,
    job_name: str,
    matches: dict[str, str] | None = None,
    schedule_cron: str | None = None,
) -> dict:
    """Convert an Airflow DAG source into a bundle ``resources.jobs`` definition.

    Args:
        dag_source: Source code of the Airflow DAG.
        job_name: Name for the generated Databricks job.
        matches: Optional ``{task_id: substring}`` map giving the match intent of
            each sensor's ``apply_function`` (which cannot be introspected from
            arbitrary Python). Omitted task_ids complete on any message.
        schedule_cron: Optional Quartz cron expression for the job schedule.

    Returns:
        A dict ready to serialize as a ``resources/<job>.yml`` bundle file.
    """
    matches = matches or {}
    tasks = []
    for sensor in extract_await_message_sensors(dag_source):
        task_id = sensor.get("task_id", "await_message")
        poll_interval = sensor.get("poll_interval", 30)
        tasks.append(
            convert_await_message_sensor(
                task_id=task_id,
                topics=sensor.get("topics", []),
                match_value_contains=matches.get(task_id, ""),
                poll_interval=poll_interval if isinstance(poll_interval, int) else 30,
            )
        )

    job: dict = {"name": job_name}
    if schedule_cron:
        job["schedule"] = {"quartz_cron_expression": schedule_cron, "timezone_id": "UTC"}
    job["tasks"] = tasks
    job["environments"] = [
        {
            "environment_key": "default",
            "spec": {"environment_version": "5", "dependencies": ["../dist/*.whl"]},
        }
    ]
    return {"resources": {"jobs": {job_name: job}}}


def to_yaml(obj: dict) -> str:
    """Serialize a converter result to YAML (requires PyYAML, a dev dependency)."""
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dev-only convenience
        raise ImportError("to_yaml requires PyYAML: pip install pyyaml") from exc
    return yaml.safe_dump(obj, sort_keys=False, width=100)


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dag", help="Path to the Airflow DAG source file.")
    parser.add_argument("--job", required=True, help="Name for the generated job.")
    parser.add_argument(
        "--match",
        action="append",
        default=[],
        metavar="TASK_ID=SUBSTRING",
        help="apply_function match intent per sensor; repeatable. Omit for any-message.",
    )
    parser.add_argument("--schedule", help="Quartz cron expression for the job schedule.")
    args = parser.parse_args(argv)

    matches = dict(pair.split("=", 1) for pair in args.match)
    with open(args.dag) as f:
        source = f.read()
    bundle = migrate_dag(source, job_name=args.job, matches=matches, schedule_cron=args.schedule)
    sys.stdout.write(to_yaml(bundle))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
