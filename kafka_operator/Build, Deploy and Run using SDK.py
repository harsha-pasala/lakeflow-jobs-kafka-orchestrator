# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk==0.110.0
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %sh bash -c "rm -rf dist/*.whl; uv build --wheel"

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import databricks.sdk.service.jobs as jobs
import databricks.sdk.service.compute as compute
import os

w = WorkspaceClient()

wait = w.jobs.submit(
    tasks=[
        jobs.Task(
            task_key="my_python_operator",
            environment_key="Default",
            python_operator_task=jobs.PythonOperatorTask(
                main="kafka_operator.examples.math.sum_task",
                parameters=[
                    jobs.PythonOperatorTaskParameter(
                        name="a",
                        value="100",
                    ),
                    jobs.PythonOperatorTaskParameter(
                        name="b",
                        value="200",
                    ),
                ],
            ),
        ),
    ],
    environments=[
        jobs.JobEnvironment(
            environment_key="Default",
            spec=compute.Environment.from_dict(
                {
                    "environment_version": "5",
                    "dependencies": [
                        os.getcwd() + "/dist/kafka_operator-0.0.1-py3-none-any.whl",
                    ],
                }
            ),
        )
    ],
)

run = w.jobs.get_run(wait.response.run_id)

print("Running at " + run.run_page_url)
_ = wait.result()
