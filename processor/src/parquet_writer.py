import os
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from schema_store import SchemaStore, utc_now_iso


class ParquetPartitionWriter:
    def __init__(
        self,
        root_dir: str,
        schema_store: SchemaStore,
        flush_record_limit: int,
        flush_interval_seconds: int,
    ) -> None:
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.schema_store = schema_store
        self.flush_record_limit = flush_record_limit
        self.flush_interval_seconds = flush_interval_seconds
        self.buffers: Dict[Tuple[str, str, str, int], List[dict]] = defaultdict(list)
        self.last_flush_at = time.time()

    def add_record(
        self,
        table_name: str,
        event_date: str,
        op_type: str,
        schema_version: int,
        record: dict,
    ) -> None:
        key = (table_name, event_date, op_type, schema_version)
        self.buffers[key].append(record)

    def should_flush(self) -> bool:
        if not self.buffers:
            return False

        if any(len(records) >= self.flush_record_limit for records in self.buffers.values()):
            return True

        return (time.time() - self.last_flush_at) >= self.flush_interval_seconds

    def flush(self, force: bool = False) -> List[str]:
        if not self.buffers:
            return []
        if not force and not self.should_flush():
            return []

        written_files = []
        now_iso = utc_now_iso()

        for (table_name, event_date, op_type, schema_version), records in list(self.buffers.items()):
            if not records:
                continue

            partition_dir = self.root / table_name / event_date / op_type
            partition_dir.mkdir(parents=True, exist_ok=True)

            file_name = f"part-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.parquet"
            file_path = partition_dir / file_name

            df = pd.DataFrame.from_records(records)
            df.to_parquet(file_path, index=False, engine="pyarrow")
            written_files.append(str(file_path))

            partition_path = f"/data_lake/{table_name}/{event_date}/{op_type}/"
            self.schema_store.track_partition(
                table_name=table_name,
                schema_version=schema_version,
                partition_path=partition_path,
                seen_at=now_iso,
            )

            del self.buffers[(table_name, event_date, op_type, schema_version)]

        self.last_flush_at = time.time()
        return written_files
