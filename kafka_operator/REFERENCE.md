# Reference

This document describes the runtime classes exposed by the `python_operator_task`
package. For end-to-end examples and configuration, see [USER_GUIDE.md](./USER_GUIDE.md).

## `SensorResult`

```py
from python_operator_task import SensorResult
```

A frozen dataclass returned by `Sensor.poll` and `OperatorV0.poll` to indicate
whether the task has completed or should be deferred.

| Field        | Type                  | Description                                                    |
| ------------ | --------------------- | -------------------------------------------------------------- |
| `status`     | `"completed"` \| `"deferred"` | Outcome of the poll.                                   |
| `defer_for`  | `datetime.timedelta` \| `None` | How long to defer before the next poll. Only set when deferred. |

### Constructors

* `SensorResult.completed()` — the condition was met; the task finishes
  successfully.
* `SensorResult.deferred(duration: datetime.timedelta)` — the condition was not
  met. The task is re-scheduled after `duration` and compute resources are
  released in the meantime.

> **Note:** there is currently no minimum deferral threshold. Compute resources
> are always released, even when `defer_for` is zero. A future version may
> introduce a minimum threshold or skip releasing compute for very small
> durations.

## `Sensor`

```py
from python_operator_task import Sensor, SensorResult
```

Protocol for objects that poll an external condition and either complete or
defer. A `Sensor` is re-created between each poll call, so any state that must
survive deferral has to be persisted externally (task values, workspace files,
Lakebase, etc.).

### Methods

* `poll(self) -> SensorResult` — called once per attempt. Return
  `SensorResult.completed()` when the condition is met, or
  `SensorResult.deferred(duration)` to release compute and try again later.

## `OperatorV0`

```py
from python_operator_task import OperatorV0, SensorResult
```

Protocol for orchestrating external work through an API. An operator's lifecycle
is `open` → repeated `poll` → `close`. Unlike sensors, operators are intended to
own external state (for example, a submitted run on another system) and to
clean it up when the task ends.

### Methods

* `open(self)` — called once at the start of the task. Create the external work
  here (for example, submit a job) and persist any handles you need via task
  values.
* `poll(self) -> SensorResult` — called after `open` to check progress. Return
  `SensorResult.completed()` when the external work is done, or
  `SensorResult.deferred(duration)` to check again later.
* `close(self)` — called when the task finishes (success, failure, or
  cancellation). Implementations should terminate any external work created in
  `open`; otherwise that work will continue running after the Python Operator
  task ends.

> **Known limitation:** `close` is not currently invoked when the job task is
> cancelled by the user or by a timeout. External work submitted in `open` may
> leak in that case.
