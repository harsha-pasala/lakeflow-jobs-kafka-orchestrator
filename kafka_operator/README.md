# kafka_operator

A Databricks Asset Bundle that demonstrates the **Python Operator** job task —
a lightweight alternative to notebook tasks for functions, sensors, and
operators. Code runs in a REPL VM (no Spark), starts faster than a cluster, and
can defer execution to release compute while waiting on external work.

This bundle ships with runnable examples under `src/kafka_operator/examples/` and
corresponding job definitions under `resources/`:

| Example              | Type      | What it does                                          |
| -------------------- | --------- | ----------------------------------------------------- |
| `math.sum_task`      | Function  | Adds two integers and writes the result to a task value. |
| `slack.send_slack_message` | Function | Posts a message to Slack through a Unity Catalog connection. |
| `counting_sensor.CountingSensor` | Sensor | Defers itself a fixed number of times, then completes. |
| `wait_for_run_sensor.WaitForRunSensor` | Sensor | Polls a job run until it reaches a terminal state. |
| `run_job_operator.RunJobOperator` | Operator | Starts a job run, waits for it to finish, and cancels it on failure. |

## Getting started

1. Open this bundle in a Databricks workspace.
2. Click the **deployments rocket** in the left sidebar to open the
   **Deployments** panel, then click **Deploy**.
3. To run a deployed job, hover over the resource in the **Deployments** panel
   and click **Run**.

Use the **Add** dropdown in the Deployments panel to add resources to the
bundle.

## Documentation

* [USER_GUIDE.md](./USER_GUIDE.md) — how to configure the Python Operator task,
  worked examples for functions, sensors, and operators, and notes on task
  values.
* [REFERENCE.md](./REFERENCE.md) — API reference for `Sensor`, `SensorResult`,
  and `OperatorV0`.
* [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
* [Databricks Asset Bundles configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
