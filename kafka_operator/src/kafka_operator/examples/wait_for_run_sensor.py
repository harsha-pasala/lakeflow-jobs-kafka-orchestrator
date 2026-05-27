from python_operator_task import Sensor, SensorResult
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
import datetime


class WaitForRunSensor(Sensor):
    run_id: str

    def __init__(self, run_id: str):
        self.run_id = run_id

    def poll(self) -> SensorResult:
        w = WorkspaceClient()

        run = w.jobs.get_run(run_id=self.run_id)
        result_state = run.state.result_state

        if result_state is not None:
            dbutils.jobs.taskValues.set("result_state", result_state.name)

            return SensorResult.completed()
        else:
            print("Running", run.run_page_url)
            print("Waiting for a minute...")

            return SensorResult.deferred(duration=datetime.timedelta(minutes=1))
