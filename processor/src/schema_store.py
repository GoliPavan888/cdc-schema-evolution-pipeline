import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


class SchemaStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS table_schemas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    schema_definition TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    UNIQUE(table_name, schema_version),
                    UNIQUE(table_name, schema_definition)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_partitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    partition_path TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    UNIQUE(table_name, schema_version, partition_path)
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _canonical_schema(schema: Dict[str, str]) -> str:
        return json.dumps(schema, sort_keys=True, separators=(",", ":"))

    def get_or_register_schema(
        self,
        table_name: str,
        schema: Dict[str, str],
        seen_at: str,
    ) -> int:
        canonical = self._canonical_schema(schema)
        conn = self._connect()
        try:
            existing = conn.execute(
                """
                SELECT schema_version FROM table_schemas
                WHERE table_name = ? AND schema_definition = ?
                """,
                (table_name, canonical),
            ).fetchone()

            if existing:
                schema_version = int(existing["schema_version"])
                conn.execute(
                    """
                    UPDATE table_schemas
                    SET last_seen_at = ?
                    WHERE table_name = ? AND schema_version = ?
                    """,
                    (seen_at, table_name, schema_version),
                )
                conn.commit()
                return schema_version

            row = conn.execute(
                """
                SELECT COALESCE(MAX(schema_version), 0) AS max_version
                FROM table_schemas
                WHERE table_name = ?
                """,
                (table_name,),
            ).fetchone()
            next_version = int(row["max_version"]) + 1

            conn.execute(
                """
                INSERT INTO table_schemas (
                    table_name,
                    schema_version,
                    schema_definition,
                    created_at,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (table_name, next_version, canonical, seen_at, seen_at),
            )
            conn.commit()
            return next_version
        finally:
            conn.close()

    def track_partition(
        self,
        table_name: str,
        schema_version: int,
        partition_path: str,
        seen_at: str,
    ) -> None:
        conn = self._connect()
        try:
            existing = conn.execute(
                """
                SELECT id FROM schema_partitions
                WHERE table_name = ?
                  AND schema_version = ?
                  AND partition_path = ?
                """,
                (table_name, schema_version, partition_path),
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE schema_partitions
                    SET last_seen_at = ?
                    WHERE id = ?
                    """,
                    (seen_at, existing["id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO schema_partitions (
                        table_name,
                        schema_version,
                        partition_path,
                        first_seen_at,
                        last_seen_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (table_name, schema_version, partition_path, seen_at, seen_at),
                )
            conn.commit()
        finally:
            conn.close()

    def get_lineage_snapshot(self) -> List[dict]:
        conn = self._connect()
        try:
            schema_rows = conn.execute(
                """
                SELECT table_name, schema_version, schema_definition, created_at, last_seen_at
                FROM table_schemas
                ORDER BY table_name, schema_version
                """
            ).fetchall()

            partition_rows = conn.execute(
                """
                SELECT table_name, schema_version, partition_path
                FROM schema_partitions
                ORDER BY table_name, schema_version, partition_path
                """
            ).fetchall()
        finally:
            conn.close()

        partitions_map = {}
        for row in partition_rows:
            key = (row["table_name"], int(row["schema_version"]))
            partitions_map.setdefault(key, []).append(row["partition_path"])

        snapshot = []
        for row in schema_rows:
            table_name = row["table_name"]
            schema_version = int(row["schema_version"])
            snapshot.append(
                {
                    "table_name": table_name,
                    "schema_version": schema_version,
                    "schema_definition": json.loads(row["schema_definition"]),
                    "created_at": row["created_at"],
                    "last_seen_at": row["last_seen_at"],
                    "output_partitions": partitions_map.get((table_name, schema_version), []),
                }
            )
        return snapshot


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
