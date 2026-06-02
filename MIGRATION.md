# Migrating an Airflow Kafka sensor to this bundle

Porting an Airflow `AwaitMessageSensor` to the `KafkaMessageSensor` Python
Operator task in this bundle. Airflow's sensor is **callable-driven**
(`apply_function`); ours is **declarative** (a parameter). Migration is three
moves: connection → secret scope, `apply_function` → `match_value_contains`,
DAG edges → job `depends_on`.

## Mapping

| Airflow | This bundle |
| --- | --- |
| `AwaitMessageSensor` | `KafkaMessageSensor` (`Sensor.poll`) |
| `apply_function` (Python callable) | `match_value_contains` param (empty = match any message) |
| `kafka_config_id` (Connection) | `bootstrap_servers` var + `secret_scope`/`secret_key` (UC reads are blocked in `python_operator_task`, so the SASL password lives in a secret scope) |
| `poll_interval` | `defer_seconds` (compute released between polls) |
| `xcom_push_key` | `dbutils.jobs.taskValues` |
| DAG `a >> b` | task `depends_on: [{ task_key: a }]` |
| Triggerer (async deferral) | `SensorResult.deferred(duration=...)` |

## 1. Connection → secret scope

```bash
databricks secrets create-scope kafka-eh
databricks secrets put-secret kafka-eh sas_connection_string --string-value "$EH_SAS"
```
```yaml
# databricks.yml
variables:
  bootstrap_servers: { default: <ns>.servicebus.windows.net:9093 }  # from connection's bootstrap.servers
  secret_scope:      { default: kafka-eh }                          # holds sasl.password
  secret_key:        { default: sas_connection_string }
```

## 2. `apply_function` → `match_value_contains`

| Airflow `apply_function` | Port to |
| --- | --- |
| `return message` (unconditional) | leave `match_value_contains` empty |
| `if "done" in message.value(): return message` | `match_value_contains: "done"` |
| `'"status": "done"' in body` | `match_value_contains: '"status": "done"'` |

The matcher tests a **substring of the decoded UTF-8 body**. For key/header or
structural matching, subclass `KafkaMessageSensor` and override the check in
`poll()`.

## 3. Worked migration — Airflow DAG → bundle job

**Before** (Airflow):
```python
with DAG("kafka_roundtrip", schedule="@daily") as dag:
    await_done = AwaitMessageSensor(task_id="await_done", topics=["rtm-source"],
                                    apply_function="dags.fns.await_done",  # "done" in body
                                    kafka_config_id="kafka_default", poll_interval=30)
    notify = PythonOperator(task_id="notify", python_callable=post_to_slack)
    await_done >> notify
```

**After** (`resources/kafka_roundtrip.yml`):
```yaml
resources:
  jobs:
    kafka_roundtrip:
      name: kafka_roundtrip
      schedule: { quartz_cron_expression: "0 0 0 * * ?", timezone_id: UTC }   # @daily
      tasks:
        - task_key: await_done
          environment_key: default
          python_operator_task:
            main: kafka_operator.kafka_message_sensor.KafkaMessageSensor
            parameters:
              - { name: task_key,             value: "{{task.name}}" }
              - { name: topic,                value: ${var.sensor_topic} }
              - { name: group_id,             value: "kafka-sensor-{{job.id}}-{{job.run_id}}" }
              - { name: bootstrap_servers,    value: ${var.bootstrap_servers} }
              - { name: secret_scope,         value: ${var.secret_scope} }
              - { name: secret_key,           value: ${var.secret_key} }
              - { name: match_value_contains, value: done }   # apply_function → param
              - { name: defer_seconds,        value: "30" }   # poll_interval → defer_seconds
        - task_key: notify
          depends_on: [ { task_key: await_done } ]            # await_done >> notify
          # reads {{tasks.await_done.values.*}} and posts to Slack
      environments:
        - environment_key: default
          spec: { environment_version: "5", dependencies: [ ../dist/*.whl ] }
```

| Airflow | Bundle |
| --- | --- |
| `schedule="@daily"` | `schedule.quartz_cron_expression` |
| `a >> b` | `depends_on` on `b` |
| `poll_interval=30` | `defer_seconds: "30"` |
| `apply_function` ("done" in body) | `match_value_contains: done` |

## Checklist

1. Move the Kafka connection into a secret scope + bundle vars.
2. Translate each `apply_function` to `match_value_contains` (empty = any message), or subclass for key/header/structural matches.
3. Rewrite DAG edges as `depends_on`; map `schedule` to `quartz_cron_expression`.
4. `databricks bundle validate` → deploy to `dev` → smoke-test with a real message.
