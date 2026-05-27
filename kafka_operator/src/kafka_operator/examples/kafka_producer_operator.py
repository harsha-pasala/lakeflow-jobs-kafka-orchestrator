import datetime
import json

import requests
from python_operator_task import OperatorV0, SensorResult
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils


class KafkaProducerOperator(OperatorV0):
    """Produces a message to a Kafka topic via a Unity Catalog connection proxy.

    Lifecycle:
        open  -> POST the record to Kafka; persist the response in task values.
        poll  -> Verify the produce succeeded; complete or raise.
        close -> No-op (produce is synchronous through the REST proxy).
    """

    def __init__(self, topic: str, key: str, value: str, task_key: str, conn_id: str = "lakeflow-orchestrator-kafka-connection"):
        self.topic = topic
        self.key = key
        self.value = value
        self.conn_id = conn_id
        self.task_key = task_key
        self.w = WorkspaceClient()

    def open(self):
        response = requests.post(
            f"{self.w.config.host}/api/2.0/unity-catalog/connections/{self.conn_id}/proxy/topics/{self.topic}",
            headers={
                **self.w.config.authenticate(),
                "Accept": "application/vnd.kafka.v2+json",
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "Accept-Encoding": "identity",
            },
            json={
                "records": [
                    {
                        "key": self.key,
                        "value": self.value,
                    }
                ]
            },
        )

        dbutils.jobs.taskValues.set("produce_status", str(response.status_code))
        dbutils.jobs.taskValues.set("produce_response", json.dumps(response.json()))

        print(f"Produced to topic='{self.topic}' key='{self.key}' status={response.status_code}")
        print(f"Response: {response.json()}")

    def poll(self) -> SensorResult:
        status = dbutils.jobs.taskValues.get(self.task_key, "produce_status", default="")

        if not status:
            return SensorResult.deferred(duration=datetime.timedelta(seconds=10))

        if int(status) == 200:
            return SensorResult.completed()
        else:
            response_body = dbutils.jobs.taskValues.get(self.task_key, "produce_response", default="{}")
            raise Exception(f"Kafka produce failed: status={status} response={response_body}")

    def close(self):
        # Produce via REST proxy is synchronous — nothing to cancel.
        pass
