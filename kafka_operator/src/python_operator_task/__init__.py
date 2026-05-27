# THE CODE BELOW IS A PART OF RUNTIME AND NOT INTENDED
# TO BE MODIFIED

import python_operator_task.runtime as _runtime
from databricks.sdk.runtime import dbutils as _dbutils

Sensor = _runtime.Sensor
SensorResult = _runtime.SensorResult
OperatorV0 = _runtime.OperatorV0

__all__ = [
    "run_function",
    "Sensor",
    "SensorResult",
    "OperatorV0",
]


def run_function():
    try:
        bindings = _dbutils.widgets.getAll()

        return _runtime._run_function(bindings)
    except:
        import traceback

        traceback.print_exc()
        raise
