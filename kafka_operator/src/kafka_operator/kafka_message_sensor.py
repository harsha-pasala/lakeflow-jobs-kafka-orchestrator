import datetime
import time

from python_operator_task import Sensor, SensorResult
from databricks.sdk.runtime import dbutils
from confluent_kafka import Consumer


class KafkaMessageSensor(Sensor):
    """Waits for a message whose body contains a target substring.

    State across deferrals is held in Kafka itself via a consumer group:
    each poll creates a fresh Consumer with a stable group.id, reads any
    new messages, and synchronously commits the offset of a matched record.

    Auth: SASL_SSL / PLAIN against the Event Hubs Kafka surface. The SAS
    connection string lives in a Databricks secret scope; UC reads are
    blocked from python_operator_task so credentials cannot come from a
    UC connection here.
    """

    def __init__(
        self,
        task_key: str,
        topic: str,
        match_value_contains: str,
        group_id: str,
        bootstrap_servers: str,
        secret_scope: str,
        secret_key: str,
        defer_seconds: int = 30,
        timeout_seconds: int = 900,
    ):
        self.task_key = task_key
        self.topic = topic
        self.match_value_contains = match_value_contains
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.secret_scope = secret_scope
        self.secret_key = secret_key
        self.defer_seconds = defer_seconds
        self.timeout_seconds = timeout_seconds

    def poll(self) -> SensorResult:
        now = time.time()
        started_at = dbutils.jobs.taskValues.get(self.task_key, "started_at", default="")
        if not started_at:
            dbutils.jobs.taskValues.set("started_at", str(now))
            started_at = str(now)
        elapsed = now - float(started_at)
        if elapsed > self.timeout_seconds:
            raise Exception(
                f"KafkaMessageSensor timed out after {elapsed:.0f}s "
                f"waiting for body containing {self.match_value_contains!r} on topic='{self.topic}'"
            )

        sas_connection_string = dbutils.secrets.get(self.secret_scope, self.secret_key)
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": sas_connection_string,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        })
        consumer.subscribe([self.topic])

        try:
            deadline = now + 25.0
            while time.time() < deadline:
                msg = consumer.poll(timeout=5.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                value_bytes = msg.value()
                if value_bytes is None:
                    continue
                try:
                    value = value_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    print(f"Skipping non-utf8 message at partition={msg.partition()} offset={msg.offset()}")
                    continue

                print(f"Read message partition={msg.partition()} offset={msg.offset()} value={value!r}")

                if self.match_value_contains in value:
                    consumer.commit(message=msg, asynchronous=False)
                    print(
                        f"Matched body containing {self.match_value_contains!r} "
                        f"at partition={msg.partition()} offset={msg.offset()}"
                    )
                    return SensorResult.completed()
        finally:
            try:
                assigned = consumer.assignment()
                if assigned:
                    positions = consumer.position(assigned)
                    valid = [tp for tp in positions if tp.offset >= 0]
                    if valid:
                        consumer.commit(offsets=valid, asynchronous=False)
                        print(f"Checkpointed position: {[(tp.partition, tp.offset) for tp in valid]}")
            except Exception as e:
                print(f"Position checkpoint failed (non-fatal): {e}")
            consumer.close()

        print(
            f"No match yet for body containing {self.match_value_contains!r}; "
            f"deferring {self.defer_seconds}s"
        )
        return SensorResult.deferred(duration=datetime.timedelta(seconds=self.defer_seconds))
