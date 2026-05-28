import datetime
import json

from python_operator_task import OperatorV0, SensorResult
from databricks.sdk.runtime import dbutils
from confluent_kafka import Producer


class KafkaProducerOperator(OperatorV0):
    """Produces a single message to a Kafka topic (Event Hubs Kafka surface).

    Auth: SASL_SSL / PLAIN. Password is a SAS connection string read from a
    Databricks secret scope at task start. UC reads are blocked from
    python_operator_task, so credentials cannot come from a UC connection here.

    Lifecycle:
        open  -> build Producer, produce(), flush(), record outcome in task values.
        poll  -> read outcome and either complete or raise.
        close -> no-op (open is synchronous through flush()).
    """

    def __init__(
        self,
        topic: str,
        key: str,
        value: str,
        task_key: str,
        bootstrap_servers: str,
        secret_scope: str,
        secret_key: str,
    ):
        self.topic = topic
        self.key = key
        self.value = value
        self.task_key = task_key
        self.bootstrap_servers = bootstrap_servers
        self.secret_scope = secret_scope
        self.secret_key = secret_key

    def open(self):
        sas_connection_string = dbutils.secrets.get(self.secret_scope, self.secret_key)

        producer = Producer({
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": sas_connection_string,
        })

        delivery: dict = {}

        def on_delivery(err, msg):
            if err is not None:
                delivery["error"] = str(err)
            else:
                delivery["partition"] = msg.partition()
                delivery["offset"] = msg.offset()

        producer.produce(
            topic=self.topic,
            key=self.key.encode("utf-8"),
            value=self.value.encode("utf-8"),
            on_delivery=on_delivery,
        )
        remaining = producer.flush(timeout=30)

        if remaining > 0:
            delivery["error"] = f"flush timed out with {remaining} message(s) still in queue"

        dbutils.jobs.taskValues.set("produce_result", json.dumps(delivery))
        print(f"Produced to topic='{self.topic}' key='{self.key}' result={delivery}")

    def poll(self) -> SensorResult:
        result_json = dbutils.jobs.taskValues.get(self.task_key, "produce_result", default="")

        if not result_json:
            return SensorResult.deferred(duration=datetime.timedelta(seconds=10))

        result = json.loads(result_json)
        if "error" in result:
            raise Exception(f"Kafka produce failed: {result['error']}")
        return SensorResult.completed()

    def close(self):
        # produce() + flush() in open() is synchronous — nothing to cancel.
        pass
