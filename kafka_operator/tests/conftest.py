"""Test fixtures.

Stub the runtime-only imports (`confluent_kafka`, `databricks.sdk.runtime`) so
the operator/sensor modules import without the native Kafka lib or a live
Databricks runtime. Tests then monkeypatch the bound names per case.
"""

import sys
import types

if "confluent_kafka" not in sys.modules:
    ck = types.ModuleType("confluent_kafka")
    ck.Consumer = object
    ck.Producer = object
    sys.modules["confluent_kafka"] = ck

if "databricks.sdk.runtime" not in sys.modules:
    databricks = sys.modules.setdefault("databricks", types.ModuleType("databricks"))
    sdk = sys.modules.setdefault("databricks.sdk", types.ModuleType("databricks.sdk"))
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.dbutils = None
    sys.modules["databricks.sdk.runtime"] = runtime
    databricks.sdk = sdk
    sdk.runtime = runtime
