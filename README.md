# lakeflow-jobs-kafka-orchestrator

A Databricks Asset Bundle that ships two **Python Operator** job tasks for orchestrating Kafka work from Lakeflow Jobs:

| Task | What it does |
| --- | --- |
| **Producer operator** (`KafkaProducerOperator`) | Synchronously produces a single message to a Kafka topic, reports the partition/offset (or error) via task values. |
| **Message sensor** (`KafkaMessageSensor`) | Watches a Kafka topic and completes when a message body contains a configured substring. Releases compute between polls and resumes via Kafka consumer-group offsets. |

Both run on the Python Operator task runtime (a lightweight REPL VM — **no Spark, no notebook**) and authenticate with **SASL_SSL / PLAIN** against the broker. The default target is the Event Hubs Kafka surface, but any SASL_PLAIN-capable Kafka cluster works.

```
lakeflow-jobs-kafka-orchestrator/
└── kafka_operator/                       # the bundle root (deploy from here)
    ├── databricks.yml                    # bundle config, variables, targets
    ├── pyproject.toml                    # confluent-kafka dependency
    ├── README.md                         # short pointer back to this file
    ├── REFERENCE.md                      # Sensor / OperatorV0 / SensorResult API
    ├── resources/
    │   ├── kafka_producer_operator.yml   # producer job definition
    │   └── kafka_message_sensor.yml      # sensor job definition
    └── src/
        ├── kafka_operator/
        │   ├── kafka_producer_operator.py
        │   └── kafka_message_sensor.py
        └── python_operator_task/         # local runtime stubs for IDE/type-checking
```

---

## Prerequisites

- **Databricks CLI v1.1.0 or later.** Older CLIs (≤ v0.299.0) silently drop the `python_operator_task` field on deploy — symptom is that your wheel uploads fine but task parameters never update.
  ```bash
  brew upgrade databricks   # or: brew install databricks/tap/databricks
  databricks --version       # confirm >= 1.1.0
  ```
- **A SASL_PLAIN-capable Kafka broker.** For Event Hubs, the host looks like `<namespace>.servicebus.windows.net:9093` and the SAS connection string is the password.
- **A Databricks secret scope** holding the SAS connection string. **UC reads are blocked from `python_operator_task`** (per FEIP-6709), so credentials cannot come from a Unity Catalog connection at runtime — they must live in a workspace secret scope.

  ```bash
  databricks secrets create-scope kafka-eh

  # Paste in your terminal, don't put it in shell history:
  export EH_SAS='Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>'
  databricks secrets put-secret kafka-eh sas_connection_string --string-value "$EH_SAS"
  unset EH_SAS

  databricks secrets list-secrets kafka-eh
  ```

---

## Configuration knobs

All env-specific values flow through **bundle variables** declared in `kafka_operator/databricks.yml`. Override per target with a `variables:` block, or per command with `--var`.

### Bundle variables

| Variable | Default | What it controls |
| --- | --- | --- |
| `bootstrap_servers` | `dlt-eventhub.servicebus.windows.net:9093` | Kafka broker `host:port`. Used by both producer and sensor. |
| `secret_scope` | `kafka-eh` | Databricks secret scope name holding the SAS connection string. |
| `secret_key` | `sas_connection_string` | Key inside `secret_scope` that maps to the SAS connection string. |
| `producer_topic` | `rtm-dest` | Topic (event hub) the producer writes to. |
| `sensor_topic` | `rtm-source` | Topic (event hub) the sensor watches. |
| `sensor_match_value` | `done` | Substring the sensor looks for inside the decoded UTF-8 message body. |

Override examples:

```bash
# One-off override at deploy
databricks bundle deploy -t prod --var "secret_scope=kafka-eh-prod"

# Durable override per target — add to databricks.yml under that target:
targets:
  prod:
    variables:
      bootstrap_servers: prod-kafka.example.com:9093
      secret_scope: kafka-eh-prod
      producer_topic: prod-events
      sensor_topic: prod-signals
```

### Task-level parameters (not variables, but tweakable in YAML)

These live inside each `resources/*.yml` and are not promoted as variables because they're typically use-case-specific rather than environment-specific.

**Producer** (`resources/kafka_producer_operator.yml`):

| Parameter | Default in YAML | Notes |
| --- | --- | --- |
| `key` | `test-key` | UTF-8 string used as the Kafka record key. |
| `value` | `{"msg": "hello from operator"}` | UTF-8 string used as the Kafka record value. |
| `task_key` | `{{task.name}}` | Resolves dynamically to this task's key; needed so `poll()` can read its own task values. |

**Sensor** (`resources/kafka_message_sensor.yml`):

| Parameter | Default | Notes |
| --- | --- | --- |
| `group_id` | `kafka-sensor-{{job.id}}-{{job.run_id}}` | Unique-per-run consumer group, so each run starts cleanly at `latest`. Drop `{{job.run_id}}` if you want runs to resume from a shared committed offset. |
| `defer_seconds` | `30` (Python default; commented out in YAML) | How long the sensor releases compute between polls when no match is found. |
| `timeout_seconds` | `900` (Python default; commented out in YAML) | Hard wall-clock ceiling across deferrals. Raises if exceeded. |
| `task_key` | `{{task.name}}` | Same dynamic resolution as the producer. |

### Bundle targets

Defined in `kafka_operator/databricks.yml`:

| Target | Mode | Workspace | Notes |
| --- | --- | --- | --- |
| `dev` | development | staging Azure workspace | Default target. Jobs prefixed with `[dev <user>]`, schedules paused. |
| `staging` | production | staging Azure workspace | Single-user `CAN_MANAGE` permission. Add `variables:` overrides as needed. |
| `prod` | production | staging Azure workspace (placeholder) | Same shape as `staging`. Swap the `host:` for the real prod workspace when one exists. |

---

## How the orchestrator works

The Python Operator runtime instantiates the configured `main:` class, calls its lifecycle methods, and either completes the task or defers compute. Two protocols are in play:

- **`OperatorV0`** — `open()` creates external work, `poll()` checks on it (can defer), `close()` tears it down on termination.
- **`Sensor`** — `poll()` only; returns `SensorResult.completed()` or `SensorResult.deferred(duration)`. Releases compute between polls.

Full API reference in [`kafka_operator/REFERENCE.md`](./kafka_operator/REFERENCE.md).

### Producer operator (`KafkaProducerOperator`)

A thin synchronous wrapper around `confluent_kafka.Producer.produce()`.

| Phase | Behavior |
| --- | --- |
| `open()` | Reads the SAS connection string from `dbutils.secrets.get(secret_scope, secret_key)`. Builds a `Producer` configured for `SASL_SSL` / `PLAIN` with username `$ConnectionString` (the Event Hubs sentinel). Calls `produce()` then `flush(timeout=30)`. Writes `{partition, offset}` (or `{"error": "..."}`) into the task value `produce_result`. |
| `poll()` | Reads `produce_result`. Returns `SensorResult.completed()` on a successful entry, raises on error. |
| `close()` | No-op — the produce was already synchronous (`flush` blocked until the broker ack'd or timed out). |

**Failure modes:**

- `flush(timeout=30)` returning non-zero → broker unreachable or timing out; result entry contains `"error": "flush timed out..."` and `poll()` raises.
- Secret read fails → exception in `open()`, task fails immediately.
- Delivery callback receives an error (e.g. topic doesn't exist, auth failure) → captured in `delivery["error"]` and surfaced through `poll()`.

The operator deliberately produces **a single message per task invocation**. For higher-throughput patterns, instantiate the underlying `Producer` directly inside a function-style operator and loop.

### Message sensor (`KafkaMessageSensor`)

The sensor instance is **recreated on every poll** — the runtime literally constructs a new `KafkaMessageSensor` object for each `poll()` call. That means any state has to live outside the Python object. The design uses two stores:

- **Kafka itself** (via the consumer group) — holds "where the sensor got to" on each partition.
- **One task value** (`started_at`) — wall-clock anchor for the hard timeout.

Per-`poll()` flow:

1. **Deadline check.** First call writes `started_at = now()` to a task value. Every subsequent call computes `elapsed = now - started_at` and raises if it exceeds `timeout_seconds`. This is the only piece of external state the sensor manages itself.
2. **Build a fresh `Consumer`** with `SASL_SSL` / `PLAIN`, a stable `group.id`, `enable.auto.commit=False`, and `auto.offset.reset=latest`.
3. **Subscribe** to `topic`. The first call triggers a group join and partition assignment.
4. **Inner read loop, ~25 seconds wall clock.** Each iteration calls `consumer.poll(timeout=5.0)`.
   - On any message, decode the body as UTF-8 and check whether `match_value_contains` is a substring of it.
   - On match: synchronously commit through the matched message's offset (`consumer.commit(message=msg, asynchronous=False)`), `close()`, return `SensorResult.completed()`.
   - On non-match: keep reading until the inner-loop deadline.
5. **Position checkpoint, always.** Before closing the consumer (whether matched or not), get the current `position()` on each assigned partition and commit those offsets synchronously. **This is what allows messages produced during a deferral to still be visible to the next poll** — without it, each new consumer with `auto.offset.reset=latest` would re-pin to the current end of the partition and skip past anything that arrived while we were deferred.
6. **No match → defer** for `defer_seconds`. The runtime releases compute; on the next poll a brand-new `KafkaMessageSensor` is constructed, builds a new `Consumer`, joins the group, finds the committed offsets from the previous poll, and resumes from there.

#### Match semantics

The sensor matches on **substring of the decoded body**. So all of these complete the sensor when `sensor_match_value` is `done`:

```
done
{"marker": "done"}
{"status": "all done"}
```

The Kafka **key** is not considered. This was a deliberate choice for two reasons: (1) the Azure Event Hubs portal Data Explorer can't set Kafka keys (it sends via the AMQP path, where Kafka keys live in the message-annotation `x-opt-kafka-key` and aren't exposed in the send-events UI), and (2) the body substring is a cleanly UI-driveable predicate for ops/test.

#### Why `auto.offset.reset=latest`

A brand-new consumer group is pinned to the current end-of-partition on first join. Messages produced **before** the sensor's first `poll()` are invisible. This is the right default for orchestration gating (you want "did the upstream signal happen after I started watching"). Switch to `earliest` if you want the sensor to detect historical sentinel messages — be aware that the unique `{{job.run_id}}` in `group_id` means each run still gets its own group.

---

## Deployment

```bash
cd kafka_operator

# Default (dev → staging workspace)
databricks bundle deploy
databricks bundle run kafka_producer_operator
databricks bundle run kafka_message_sensor

# Higher environments
databricks bundle deploy -t staging
databricks bundle deploy -t prod
```

The wheel is rebuilt and re-uploaded automatically on every deploy via the `artifacts.python_artifact.build` command (`rm -rf dist/*.whl && uv build --wheel`).

To validate the rendered plan before deploying:

```bash
databricks bundle validate -t prod -o json | jq '.resources.jobs'
```

---

## Local testing

The fastest way to drive the sensor end-to-end:

1. **Deploy and start the sensor.** `databricks bundle run kafka_message_sensor` and wait ~15s for the first poll to subscribe and pin to `latest`.
2. **Send a marker message.** Either:
   - **kcat** (one-liner):
     ```bash
     SAS='<your SAS connection string>'
     printf 'k:{"marker":"done"}\n' | kcat -P \
       -b dlt-eventhub.servicebus.windows.net:9093 \
       -t rtm-source \
       -K : \
       -X security.protocol=SASL_SSL \
       -X sasl.mechanism=PLAIN \
       -X sasl.username='$ConnectionString' \
       -X sasl.password="$SAS"
     ```
   - **Azure portal Data Explorer**: paste `{"marker":"done"}` as the body (no Custom Properties needed — sensor matches on body substring).
3. **Next poll picks it up and completes.**

---

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `Warning: unknown field: python_operator_task` during `bundle validate` | CLI < v1.1.0 | `brew upgrade databricks` |
| Task fails with `TypeError: __init__() missing N required positional arguments` | YAML parameter list out of sync with class signature, often from an old CLI deploy that stripped `python_operator_task` and never updated it | Upgrade CLI, redeploy. To force-overwrite a stuck job, also `bundle destroy` first. |
| Sensor runs to `timeout_seconds` and fails even though the message was sent | The position-checkpoint commit didn't run (older sensor code, or the consumer never got partition assignments) | Confirm the sensor logs print `Checkpointed position: [...]` after the no-match branch. If not, increase the inner-loop budget or check broker reachability. |
| `dbutils.secrets.get(...)` raises `Secret does not exist` | Secret scope or key name doesn't match `secret_scope`/`secret_key` bundle variables | `databricks secrets list-scopes` / `list-secrets <scope>` |
| Producer hangs in `flush()` | Broker unreachable or wrong port (9093 for Kafka surface on Event Hubs, not 5671/9092) | Check `bootstrap_servers` host:port and any firewall/private-endpoint policy. |

---

## References

- [`kafka_operator/REFERENCE.md`](./kafka_operator/REFERENCE.md) — API reference for `Sensor`, `OperatorV0`, `SensorResult`.
- [Databricks Asset Bundles configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
- [Apache Kafka authentication on Databricks](https://docs.databricks.com/aws/en/connect/streaming/kafka/authentication)
- [Event Hubs Kafka protocol overview](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-kafka-overview)
