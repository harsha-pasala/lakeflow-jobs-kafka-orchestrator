# THE CODE BELOW IS A PART OF RUNTIME AND NOT INTENDED
# TO BE MODIFIED

import inspect
import importlib
import json
from enum import Enum
import datetime
from typing import Literal, Optional, Protocol, runtime_checkable
from dataclasses import dataclass

__all__ = [
    "OperatorV0",
    "Sensor",
    "SensorResult",
    "_run_function",
]


@dataclass(frozen=True)
class SensorResult:
    """
    SensorResult is returned by Sensor.poll method to indicate whether
    the sensor has completed or should be deferred.  sensor is deferred,
    compute resources are released.

    NOTE: currently there is no threshold, compute resources are ALWAYS
    released, even if defer_for is set to zero. Future version may introduce
    a minimum threshold, or skip releasing compute resources if defer_for is set to
    zero or very small value.
    """

    status: Literal["completed", "deferred"]
    defer_for: datetime.timedelta | None = None

    @classmethod
    def completed(cls) -> "SensorResult":
        return cls(status="completed")

    @classmethod
    def deferred(cls, duration: datetime.timedelta) -> "SensorResult":
        return cls(status="deferred", defer_for=duration)


@runtime_checkable
class Sensor(Protocol):
    """
    Sensor is responsible for polling a condition and returning a SensorResult
    indicating whether the condition has been met or if the sensor should be deferred.
    """

    def poll(self) -> SensorResult:
        """
        Poll method is called to check the status of the condition. It should return
        SensorResult.completed() if the condition has been met, or SensorResult.deferred() if the
        sensor should be deferred.
        """
        ...


@runtime_checkable
class OperatorV0(Protocol):
    """
    OperatorV0 is responsible for orchestrating external tasks through API calls.
    """

    def open(self):
        """
        Open method is called once at the beginning of the task run. This is where
        operators should create external tasks through API.
        """
        ...

    def poll(self) -> SensorResult:
        """
        Poll method is called after open to check the status of the task. It should return
        SensorResult.completed() if the task has completed, or SensorResult.deferred() if the
        task should be deferred.
        """
        ...

    def close(self):
        """
        Close operator when job task fails, terminates successfuly, or is cancelled
        due to timeout or user request.

        Implementations should terminate any tasks created in 'open' method. For example,
        when orchestrating tasks through API, 'close' method should ensure that submitted
        tasks are completed, failed or cancelled. Otherwise, external tasks will continue
        running after Python Operator task run is terminated.

        NOTE: OperatorV0 doesn't call 'close' method when job task is cancelled.
        """
        ...


class _Outcome(Enum):
    COMPLETED = "COMPLETED"
    DEFERRED = "DEFERRED"


class _PythonOperatorClientMessage:
    MIME_TYPE = "application/vnd.databricks.pythonoperatortask+json"

    def __init__(self, outcome: _Outcome, deferred_duration_millis: Optional[int]):
        self.payload: dict = {"outcome": outcome.name}
        if deferred_duration_millis is not None:
            self.payload["deferred_duration_millis"] = deferred_duration_millis

    def _repr_mimebundle_(self, **kwargs):
        return {self.MIME_TYPE: json.dumps(self.payload)}, {}

    def __repr__(self):
        return f"PythonOperatorClientMessage({self.payload})"


def _run_function(bindings: dict[str, str]):
    main = bindings["__databricks_main"]

    if not main:
        raise ValueError("__databricks_main is not set")

    parameters = {k: v for k, v in bindings.items() if not k.startswith("__databricks")}

    # main is a string like "a.b.c.function"
    module, func = main.rsplit(".", 1)

    if not module or not func:
        raise ValueError("__databricks_main is not a valid qualified name")

    module = importlib.import_module(module)
    obj = getattr(module, func)

    if not func:
        raise ValueError(f"Function {func} not found in module {module}")

    if inspect.isfunction(obj):
        result = _run_callable(obj, parameters)

        return _PythonOperatorClientMessage(
            outcome=_Outcome.COMPLETED,
            deferred_duration_millis=None,
        )
    elif _is_operator_v0_class(obj):
        instance = _run_callable(obj, parameters)

        from databricks.sdk.runtime import dbutils

        task_key = _get_current_task_key()
        opened = (
            dbutils.jobs.taskValues.get(task_key, "__operator_opened", default="false")
            == "true"
        )

        if not isinstance(instance, OperatorV0):
            raise ValueError(f"{func} is not an OperatorV0")

        if not opened:
            instance.open()
            dbutils.jobs.taskValues.set("__operator_opened", "true")

        try:
            result = instance.poll()
        except:
            instance.close()
            raise

        if result.status == "completed":
            instance.close()

        return _client_message_from_result(result)
    elif _is_sensor_class(obj):
        instance = _run_callable(obj, parameters)

        if not isinstance(instance, Sensor):
            raise ValueError(f"{func} is not a Sensor")

        result = instance.poll()

        return _client_message_from_result(result)
    else:
        raise ValueError("{func} is not a function, Sensor or Operator")


def _client_message_from_result(result: SensorResult) -> _PythonOperatorClientMessage:
    if result.status == "completed":
        return _PythonOperatorClientMessage(
            outcome=_Outcome.COMPLETED,
            deferred_duration_millis=None,
        )
    else:
        deferred_duration_millis = None
        if result.defer_for:
            deferred_duration_millis = int(result.defer_for.total_seconds() * 1000)

        return _PythonOperatorClientMessage(
            outcome=_Outcome.DEFERRED,
            deferred_duration_millis=deferred_duration_millis,
        )


def _get_current_task_key() -> str:
    # specific sensors should add "{{task_key}}" as parameter
    # we add _get_current_task_key only to implement OperatorV0
    # until we create builtin support for operators
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.runtime import dbutils

    run_id = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().runId().get()
    )
    w = WorkspaceClient()
    run = w.jobs.get_run(run_id)

    for task in run.tasks:
        if str(task.run_id) == run_id:
            return task.task_key

    raise Exception("failed to resolve task_key")


def _is_sensor_class(obj: object) -> bool:
    return inspect.isclass(obj) and issubclass(obj, Sensor)


def _is_operator_v0_class(obj: object) -> bool:
    return inspect.isclass(obj) and issubclass(obj, OperatorV0)


def _run_callable(obj, parameters: dict[str, str]):
    signature = inspect.signature(obj)
    converted_kwargs: dict[str, object] = {}

    has_kwargs = any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in signature.parameters.values()
    )
    if has_kwargs:
        # capture all parameters as kwargs if signature has kwargs
        converted_kwargs = {**parameters}

    # inspect func signature and validate parameters
    for param in signature.parameters.values():
        if param.kind in [
            inspect.Parameter.VAR_POSITIONAL,  # non-deterministic, don't pass anything
            inspect.Parameter.VAR_KEYWORD,  # already captured as kwargs
            inspect.Parameter.POSITIONAL_ONLY,  # this will result in error when we call the function
        ]:
            continue

        if param.kind in [
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ]:
            if param.name in parameters:
                converted_kwargs[param.name] = _convert_parameter(
                    param, parameters[param.name]
                )

    return obj(**converted_kwargs)


def _convert_parameter(param: inspect.Parameter, value: str):
    if param.annotation == str:
        return value
    if param.annotation == int:
        return int(value)
    if param.annotation == float:
        return float(value)
    if param.annotation == bool:
        if value.lower() not in ["true", "false"]:
            raise ValueError(f"Parameter {param.name}: invalid boolean value '{value}'")
        return value.lower() == "true"
    if param.annotation == list:
        result = json.loads(value)
        if not isinstance(result, list):
            raise ValueError(f"Parameter {param.name}: invalid list value '{value}'")
        return result
    if param.annotation == dict:
        result = json.loads(value)
        if not isinstance(result, dict):
            raise ValueError(f"Parameter {param.name}: invalid dict value '{value}'")
        return result

    # TODO: support more types, including generics

    raise ValueError(
        f"Parameter {param.name}: unsupported type '{param.annotation}',"
        f"only str, int, float, bool, list, and dict are supported."
    )
