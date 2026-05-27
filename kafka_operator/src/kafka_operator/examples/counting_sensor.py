import datetime

from python_operator_task import Sensor, SensorResult
from databricks.sdk.runtime import dbutils


class CountingSensor(Sensor):
    def __init__(self, task_key: str, limit: int = 3):
        self.limit = limit
        self.task_key = task_key

    def poll(self) -> SensorResult:
        count = int(dbutils.jobs.taskValues.get(self.task_key, "count", default="0")) + 1
        dbutils.jobs.taskValues.set("count", str(count))

        print(f"Counting count={count}")

        if count < self.limit:
            return SensorResult.deferred(duration=datetime.timedelta(minutes=1))
        else:
            return SensorResult.completed()
