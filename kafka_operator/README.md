# kafka_operator

Databricks Asset Bundle that defines two Python Operator job tasks for orchestrating Kafka work from Lakeflow Jobs:

| Resource | Class | Purpose |
| --- | --- | --- |
| `kafka_producer_operator` | `kafka_operator.kafka_producer_operator.KafkaProducerOperator` | Produces a single message to a Kafka topic. Synchronous; reports the partition/offset via task values. |
| `kafka_message_sensor` | `kafka_operator.kafka_message_sensor.KafkaMessageSensor` | Watches a Kafka topic and completes when a message body contains a configured substring. Uses a Kafka consumer group to checkpoint position across deferrals. |

Both run as Python Operator job tasks (`python_operator_task`) — no Spark, no notebook — and authenticate to the Kafka broker via SASL_SSL / PLAIN using a SAS connection string read from a Databricks secret scope.

## Prerequisites

- Databricks CLI **v1.1.0 or later** (older versions silently strip the `python_operator_task` field on deploy).
- A target Kafka broker. The defaults target the Event Hubs Kafka surface (`*.servicebus.windows.net:9093`), but any SASL_PLAIN Kafka broker works.
- A Databricks secret scope holding the broker's SAS connection string:

  ```bash
  databricks secrets create-scope kafka-eh
  export EH_SAS='Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>'
  databricks secrets put-secret kafka-eh sas_connection_string --string-value "$EH_SAS"
  unset EH_SAS
  ```

## Configuration

All environment-specific values live in bundle variables defined in `databricks.yml`:

| Variable | Default | Purpose |
| --- | --- | --- |
| `bootstrap_servers` | `dlt-eventhub.servicebus.windows.net:9093` | Kafka brokers (`host:port`). |
| `secret_scope` | `kafka-eh` | Databricks secret scope name. |
| `secret_key` | `sas_connection_string` | Key in that scope holding the SAS connection string. |
| `producer_topic` | `rtm-dest` | Topic the producer writes to. |
| `sensor_topic` | `rtm-source` | Topic the sensor watches. |
| `sensor_match_value` | `done` | Substring the sensor looks for in the message body. |

Override per target by adding a `variables:` block under that target in `databricks.yml`, or per command via `--var "name=value"`.

## Deploy and run

```bash
# Default target (dev → staging Azure workspace)
databricks bundle deploy
databricks bundle run kafka_producer_operator
databricks bundle run kafka_message_sensor

# Target a higher environment
databricks bundle deploy -t staging
databricks bundle deploy -t prod
```

The wheel is rebuilt automatically before each deploy via the `artifacts.python_artifact.build` command.

## Operator semantics

`KafkaProducerOperator` (lifecycle: `open` → `poll` → `close`):

- `open()` builds a `confluent_kafka.Producer`, calls `produce()` followed by `flush(timeout=30)`, and writes the resulting `{partition, offset}` (or error string) to the task value `produce_result`.
- `poll()` reads `produce_result` and returns `SensorResult.completed()`, or raises on failure.
- `close()` is a no-op (produce + flush is synchronous in `open`).

## Sensor semantics

`KafkaMessageSensor` (lifecycle: `poll` only):

- Each `poll()` creates a fresh `confluent_kafka.Consumer` with a stable `group.id` derived from the job/run IDs.
- New groups start at `auto.offset.reset=latest`, so only messages produced after the sensor joins are visible.
- Inner read loop runs for ~25s per `poll()` call. On a body containing `match_value_contains`, the offset is committed and the sensor completes.
- On every `poll()` (match or no match) the current consumer position is committed synchronously, so the next deferral resumes from where the last poll left off — this is what allows the sensor to see messages produced *during* deferral.
- A hard `timeout_seconds` ceiling (default 900) is tracked across deferrals using a task value (`started_at`) and raises if exceeded.

## Layout

```
kafka_operator/
├── databricks.yml                          # bundle config, variables, targets
├── pyproject.toml                          # confluent-kafka dependency
├── README.md
├── REFERENCE.md                            # Sensor / OperatorV0 / SensorResult API
├── resources/
│   ├── kafka_producer_operator.yml         # producer job definition
│   └── kafka_message_sensor.yml            # sensor job definition
└── src/
    ├── kafka_operator/
    │   ├── kafka_producer_operator.py      # KafkaProducerOperator
    │   └── kafka_message_sensor.py         # KafkaMessageSensor
    └── python_operator_task/               # local type stubs matching the workspace runtime
```

## Notes

- UC reads are blocked from `python_operator_task` (per FEIP-6709), so credentials are sourced from a Databricks secret scope rather than from a Unity Catalog connection.
- Consumer group IDs include `{{job.id}}` and `{{job.run_id}}` so each run gets an isolated group and starts cleanly at `latest`. Drop `{{job.run_id}}` if you want runs to resume from a shared offset.

## References

- [REFERENCE.md](./REFERENCE.md) — `Sensor`, `OperatorV0`, and `SensorResult` API.
- [Databricks Asset Bundles configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
- [Apache Kafka authentication on Databricks](https://docs.databricks.com/aws/en/connect/streaming/kafka/authentication)
