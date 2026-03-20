import json
from pathlib import Path

from schema_store import SchemaStore


def generate_lineage_report(schema_store: SchemaStore, output_path: str) -> None:
    output = []
    for row in schema_store.get_lineage_snapshot():
        output.append(
            {
                "source_table": f"inventory.{row['table_name']}",
                "schema_version": int(row["schema_version"]),
                "active_from": row["created_at"],
                "active_to": row["last_seen_at"],
                "transformation_logic": "Debezium CDC event processing, direct mapping",
                "output_partitions": row["output_partitions"],
                "output_schema": row["schema_definition"],
            }
        )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, indent=2), encoding="utf-8")
