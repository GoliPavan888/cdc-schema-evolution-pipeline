# CDC Schema Evolution Pipeline

This project implements a fault-tolerant CDC pipeline from MySQL to partitioned Parquet files with schema version tracking.

## Architecture

- MySQL 8 with binlog + GTID enabled
- Kafka + Zookeeper
- Debezium Kafka Connect (MySQL connector)
- Python processor service:
  - Consumes Debezium CDC events from Kafka
  - Detects schema changes and versions them in SQLite
  - Writes partitioned Parquet files to `data_lake`
  - Generates `output/lineage_report.json`

## Partitioning Scheme

`data_lake/{table_name}/{event_date}/{op_type}/`

Examples:

- `data_lake/products/2026-03-19/c/`
- `data_lake/customers/2026-03-19/u/`
- `data_lake/orders/2026-03-19/d/`

## Schema Store

Persistent SQLite DB at `state/schemas.db` with:

- `table_schemas` (versioned schema definitions per table)
- `schema_partitions` (observed output partitions per table/schema version)

## Quick Start

1. Create a runtime env file:

```powershell
Copy-Item .env.example .env
```

2. Start everything:

```powershell
docker compose up -d --build
```

3. Confirm services:

```powershell
docker compose ps
```

## Debezium Connector

The processor auto-registers connector `inventory-connector` at startup via Kafka Connect REST.

## Seed Data

Database seed scripts are mounted from `mysql/init` and run automatically on first MySQL startup.

Tables seeded:

- `customers`: 100,000 rows
- `products`: 50,000 rows
- `orders`: 400,000 rows

Total rows: 550,000+

## Simulate Schema Evolution

Run:

```powershell
./scripts/simulate_schema_evolution.ps1
```

This applies:

1. `ALTER TABLE products RENAME COLUMN description TO product_description;`
2. Inserts a new products row
3. `ALTER TABLE customers ADD COLUMN country_code VARCHAR(3) NOT NULL DEFAULT 'USA';`
4. Inserts a new customers row

The processor should continue writing Parquet files without interruption.

## Validate Contract Items

Run:

```powershell
./scripts/verify_contract.ps1
```

## Generate lineage report manually

```powershell
docker compose exec processor python -m main lineage
```

Output file:

- `output/lineage_report.json`

## Notes on Fault Tolerance

- Consumer offsets are committed only after successful Parquet writes.
- On restart, the processor re-reads from last committed offsets (at-least-once processing).
- Schema store and data lake are persisted on bind mounts.
