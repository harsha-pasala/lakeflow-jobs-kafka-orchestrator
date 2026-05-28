# kafka_operator

This directory is the **Databricks Asset Bundle** for the `lakeflow-jobs-kafka-orchestrator` project.

For a full walkthrough — configuration knobs, how the producer operator and message sensor work, deployment workflow, and troubleshooting — see [`../README.md`](../README.md) at the repo root.

For the runtime API reference (`Sensor`, `OperatorV0`, `SensorResult`), see [`REFERENCE.md`](./REFERENCE.md).

## Quick deploy

```bash
databricks bundle deploy             # default target: dev
databricks bundle run kafka_producer_operator
databricks bundle run kafka_message_sensor
```
