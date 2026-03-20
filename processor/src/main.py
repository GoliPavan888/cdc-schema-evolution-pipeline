import argparse
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from kafka import KafkaConsumer

from connector import ensure_connector
from lineage_report import generate_lineage_report
from parquet_writer import ParquetPartitionWriter
from schema_store import SchemaStore, utc_now_iso

RUNNING = True


def handle_shutdown(_signum: int, _frame: Optional[object]) -> None:
    global RUNNING
    RUNNING = False


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def infer_type(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"


def build_schema(record: Dict[str, object]) -> Dict[str, str]:
    schema = {}
    for key, value in record.items():
        schema[key] = infer_type(value)
    return schema


def iso_from_ts_ms(ts_ms: Optional[int]) -> str:
    if not ts_ms:
        return utc_now_iso()
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def event_date(ts_ms: Optional[int]) -> str:
    if not ts_ms:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def parse_table_name(topic: str) -> str:
    parts = topic.split(".")
    return parts[-1] if parts else topic


def build_connector_config() -> Dict[str, str]:
    topic_prefix = env("TOPIC_PREFIX", "dbserver1")
    database_name = env("MYSQL_DATABASE", "inventory")
    table_include_list = env(
        "TABLE_INCLUDE_LIST",
        "inventory.customers,inventory.orders,inventory.products",
    )
    return {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": env("DEBEZIUM_DB_USER", "debezium_user"),
        "database.password": env("DEBEZIUM_DB_PASSWORD", "debezium_pw"),
        "database.server.id": "184054",
        "topic.prefix": topic_prefix,
        "database.include.list": database_name,
        "table.include.list": table_include_list,
        "schema.history.internal.kafka.bootstrap.servers": env(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
        ),
        "schema.history.internal.kafka.topic": "schema-changes.inventory",
        "include.schema.changes": "true",
        "decimal.handling.mode": "string",
        "snapshot.mode": "initial",
    }


def run() -> int:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    kafka_bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    group_id = env("PROCESSOR_GROUP_ID", "cdc-processor-group")
    batch_size = int(env("PROCESSOR_BATCH_SIZE", "1000"))
    flush_records = int(env("PROCESSOR_FLUSH_RECORDS", "5000"))
    flush_seconds = int(env("PROCESSOR_FLUSH_SECONDS", "20"))
    topic_prefix = env("TOPIC_PREFIX", "dbserver1")
    connect_rest_url = env("CONNECT_REST_URL", "http://connect:8083")

    state_db_path = env("STATE_DB_PATH", "/app/state/schemas.db")
    data_lake_root = env("DATA_LAKE_ROOT", "/app/data_lake")
    lineage_output_path = env("LINEAGE_OUTPUT_PATH", "/app/output/lineage_report.json")

    schema_store = SchemaStore(state_db_path)
    writer = ParquetPartitionWriter(
        root_dir=data_lake_root,
        schema_store=schema_store,
        flush_record_limit=flush_records,
        flush_interval_seconds=flush_seconds,
    )

    auto_register = env("AUTO_REGISTER_CONNECTOR", "true").lower() == "true"
    if auto_register:
        ensure_connector(
            connect_rest_url=connect_rest_url,
            connector_name="inventory-connector",
            connector_config=build_connector_config(),
        )

    table_filter_regex = re.compile(
        rf"^{re.escape(topic_prefix)}\\.inventory\\.(customers|products|orders)$"
    )

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
        max_poll_records=batch_size,
    )
    consumer.subscribe(pattern=table_filter_regex.pattern)

    last_lineage_write = 0.0

    while RUNNING:
        polled = consumer.poll(timeout_ms=1000, max_records=batch_size)
        has_records = False

        for _tp, messages in polled.items():
            for message in messages:
                payload = message.value
                if not isinstance(payload, dict):
                    continue

                op = payload.get("op")
                if op not in ("c", "u", "d", "r"):
                    continue

                table_name = parse_table_name(message.topic)
                row_data = payload.get("after") if op in ("c", "u", "r") else payload.get("before")
                if not row_data:
                    continue

                ts_ms = payload.get("ts_ms") or payload.get("source", {}).get("ts_ms")
                seen_at = iso_from_ts_ms(ts_ms)
                op_type = "c" if op == "r" else op

                schema = build_schema(row_data)
                schema_version = schema_store.get_or_register_schema(
                    table_name=table_name,
                    schema=schema,
                    seen_at=seen_at,
                )

                enriched = dict(row_data)
                enriched["op_type"] = op_type
                enriched["event_timestamp"] = seen_at
                enriched["schema_version"] = schema_version
                enriched["source_table"] = f"inventory.{table_name}"

                writer.add_record(
                    table_name=table_name,
                    event_date=event_date(ts_ms),
                    op_type=op_type,
                    schema_version=schema_version,
                    record=enriched,
                )
                has_records = True

        # Offsets are committed only after writes complete to guarantee at-least-once semantics.
        if has_records:
            writer.flush(force=True)
            consumer.commit()

        if writer.should_flush():
            writer.flush(force=True)

        now = time.time()
        if now - last_lineage_write > 30:
            generate_lineage_report(schema_store, lineage_output_path)
            last_lineage_write = now

    writer.flush(force=True)
    generate_lineage_report(schema_store, lineage_output_path)
    consumer.commit()
    consumer.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="CDC processor")
    parser.add_argument("command", nargs="?", default="run", choices=["run", "lineage"])
    args = parser.parse_args()

    state_db_path = env("STATE_DB_PATH", "/app/state/schemas.db")
    lineage_output_path = env("LINEAGE_OUTPUT_PATH", "/app/output/lineage_report.json")

    schema_store = SchemaStore(state_db_path)

    if args.command == "lineage":
        generate_lineage_report(schema_store, lineage_output_path)
        return 0

    return run()


if __name__ == "__main__":
    sys.exit(main())
