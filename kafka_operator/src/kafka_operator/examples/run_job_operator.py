import datetime

from python_operator_task import OperatorV0, SensorResult
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
import databricks.sdk.service.jobs as jobs


class RunJobOperator(OperatorV0):
    def __init__(self, job_id: str, task_key: str):
        self.job_id = job_id
        self.task_key = task_key
        self.w = WorkspaceClient()

    def open(self):
        wait = self.w.jobs.run_now(self.job_id)
        run_id = wait.response.run_id

        dbutils.jobs.taskValues.set("run_id", str(run_id))

        print("Submitted a new task run")

    def poll(self) -> SensorResult:
        run_id = dbutils.jobs.taskValues.get(self.task_key, "run_id")
        run = self.w.jobs.get_run(run_id)
        result_state = run.state.result_state

        if result_state is None:
            defer_for = datetime.timedelta(minutes=5)
            next_check = datetime.datetime.now() + defer_for

            print("Running", run.run_page_url)
            print(f"Next check is scheduled for {next_check.isoformat()}")

            return SensorResult.deferred(defer_for)

        if result_state == jobs.RunResultState.SUCCESS:
            print("Run has succeeded", run.run_page_url)
            return SensorResult.completed()
        else:
            print("Run has failed or was cancelled", run.run_page_url)
            raise Exception(f"run has failed or was cancelled ({result_state})")

    def close(self):
        run_id = dbutils.jobs.taskValues.get(self.task_key, "run_id", default="")

        if run_id:
            run = self.w.jobs.get_run(run_id)

            if run.state.result_state is None:
                self.w.jobs.cancel_run(run_id)
