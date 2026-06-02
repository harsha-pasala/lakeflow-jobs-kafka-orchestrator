# Demonstrated migration: Airflow DAG → this bundle

A **real, reproducible** migration of an Airflow Kafka sensor to this bundle's
`KafkaMessageSensor` — not a hand-written claim.

| File | Role |
| --- | --- |
| `kafka_roundtrip_dag.py` | The original Airflow DAG (`AwaitMessageSensor` → notify). |
| `kafka_roundtrip.generated.yml` | The bundle job, **generated** from the DAG by the converter. |

## Reproduce it

```bash
cd kafka_operator
uv run --group dev python -m kafka_operator.airflow_migration \
    ../examples/airflow/kafka_roundtrip_dag.py \
    --job kafka_roundtrip --match await_done=done --schedule "0 0 0 * * ?" \
    > ../examples/airflow/kafka_roundtrip.generated.yml
```

The converter (`kafka_operator.airflow_migration`) AST-scans the DAG for
`AwaitMessageSensor(...)` calls and maps each to a `KafkaMessageSensor`
`python_operator_task`. `--match` supplies the substring intent of the Airflow
`apply_function` (omit it to complete on any message).

## Verified deployable

The generated file validates as a Databricks Asset Bundle resource
(`databricks bundle validate` → `Validation OK!`). To deploy it, copy it into
`kafka_operator/resources/` and add it to the `include:` list in
`databricks.yml`.

## What maps to what

| Airflow | Generated bundle task |
| --- | --- |
| `task_id="await_done"` | `task_key: await_done` |
| `topics=["rtm-source"]` | `topic: rtm-source` |
| `apply_function` (matches `"done"`) | `match_value_contains: done` |
| `poll_interval=30` | `defer_seconds: "30"` |
| `schedule="@daily"` | `schedule.quartz_cron_expression: "0 0 0 * * ?"` |
| `kafka_config_id` | `bootstrap_servers` / `secret_scope` / `secret_key` vars |

The converter is also agent-callable — see `kafka_operator/agent_tool_spec.json`
for the tool schema an AI agent or Genie Code skill can register.
