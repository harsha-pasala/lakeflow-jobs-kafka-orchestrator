from databricks.sdk.runtime import dbutils


def sum_task(a: int, b: int):
    print(f"Running sum task with a={a} b={b}")

    dbutils.jobs.taskValues.set("return_value", str(a + b))
