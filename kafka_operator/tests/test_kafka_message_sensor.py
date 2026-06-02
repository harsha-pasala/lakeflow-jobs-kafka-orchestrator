"""Unit tests for KafkaMessageSensor.poll() with a mocked Kafka consumer.

The sensor's `time`, `dbutils`, and `Consumer` are replaced so poll() runs
deterministically with no broker, no Databricks runtime, and no real clock.
"""

import datetime

import pytest

from kafka_operator import kafka_message_sensor as kms


class FakeMsg:
    def __init__(self, value, partition=0, offset=0, err=None):
        self._v = None if value is None else value.encode("utf-8")
        self._p, self._o, self._e = partition, offset, err

    def value(self):
        return self._v

    def error(self):
        return self._e

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class FakeTP:
    def __init__(self, partition, offset):
        self.partition, self.offset = partition, offset


class FakeConsumer:
    def __init__(self, messages):
        self._messages = list(messages)
        self.committed = []
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout=0.0):
        return self._messages.pop(0) if self._messages else None

    def commit(self, message=None, offsets=None, asynchronous=True):
        self.committed.append({"message": message, "offsets": offsets})

    def assignment(self):
        return [FakeTP(0, 0)]

    def position(self, tps):
        return [FakeTP(0, 5)]

    def close(self):
        self.closed = True


class FakeClock:
    """Returns scripted time.time() values; repeats the last one when drained."""

    def __init__(self, values):
        self._values = list(values)

    def time(self):
        return self._values.pop(0) if len(self._values) > 1 else self._values[0]


class FakeTaskValues:
    def __init__(self, initial=None):
        self._store = dict(initial or {})

    def get(self, task_key, key, default=""):
        return self._store.get(key, default)

    def set(self, key, value):
        self._store[key] = value


class FakeDbutils:
    def __init__(self, task_values):
        self.jobs = types_ns(taskValues=task_values)
        self.secrets = types_ns(get=lambda scope, key: "fake-sas-connection-string")


def types_ns(**kwargs):
    ns = type("NS", (), {})()
    for k, v in kwargs.items():
        setattr(ns, k, v)
    return ns


def make_sensor(**overrides):
    kwargs = dict(
        task_key="t",
        topic="rtm-source",
        group_id="g",
        bootstrap_servers="b:9093",
        secret_scope="s",
        secret_key="k",
    )
    kwargs.update(overrides)
    return kms.KafkaMessageSensor(**kwargs)


def wire(monkeypatch, *, messages, clock, task_values=None):
    consumer = FakeConsumer(messages)
    monkeypatch.setattr(kms, "Consumer", lambda cfg: consumer)
    monkeypatch.setattr(kms, "time", FakeClock(clock))
    monkeypatch.setattr(kms, "dbutils", FakeDbutils(FakeTaskValues(task_values)))
    return consumer


def test_substring_match_completes(monkeypatch):
    consumer = wire(monkeypatch, messages=[FakeMsg('{"status":"done"}', offset=7)], clock=[1000, 1001, 2000])
    result = make_sensor(match_value_contains="done").poll()
    assert result.status == "completed"
    assert consumer.closed
    assert any(c["message"] is not None for c in consumer.committed)


def test_empty_match_completes_on_any_message(monkeypatch):
    wire(monkeypatch, messages=[FakeMsg("anything at all")], clock=[1000, 1001, 2000])
    result = make_sensor(match_value_contains="").poll()
    assert result.status == "completed"


def test_no_match_defers(monkeypatch):
    consumer = wire(monkeypatch, messages=[FakeMsg("nope")], clock=[1000, 1001, 1002, 2000])
    result = make_sensor(match_value_contains="done", defer_seconds=42).poll()
    assert result.status == "deferred"
    assert result.defer_for == datetime.timedelta(seconds=42)
    # Even without a match, the position is checkpointed before closing.
    assert any(c["offsets"] is not None for c in consumer.committed)
    assert consumer.closed


def test_timeout_raises(monkeypatch):
    wire(monkeypatch, messages=[], clock=[10_000.0], task_values={"started_at": "1.0"})
    with pytest.raises(Exception, match="timed out"):
        make_sensor(match_value_contains="done", timeout_seconds=900).poll()


def test_non_matching_message_does_not_complete_then_defers(monkeypatch):
    wire(monkeypatch, messages=[FakeMsg("still working")], clock=[1000, 1001, 1002, 2000])
    result = make_sensor(match_value_contains="done").poll()
    assert result.status == "deferred"
