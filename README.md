# SecurityMaster DuckDB Persistence

Lightweight DuckDB-based storage for SecurityMaster snapshots using SCD Type 2. Instead of saving identical CSVs day after day, only changes are stored.

**13 days of data: 65 MB CSV -> 6.3 MB DuckDB (~10x compression)**

## How it works

Each security is stored once with a `valid_from` / `valid_to` date range. When data doesn't change between days, no new rows are written. Only new, changed, or deleted securities generate records.

```
security_id | ticker | country | valid_from | valid_to
04MXXQYNG9  | AAPL   | US      | 2025-12-01 | NULL        # unchanged since Dec 1
K9RQE6VQGZ  | AAL    | US      | 2025-12-01 | 2025-12-05  # changed on Dec 5
K9RQE6VQGZ  | AAL    | US      | 2025-12-05 | NULL        # new version
```

Date range semantics: `valid_from` is **inclusive** (>=), `valid_to` is **exclusive** (<). NULL means current/forever.

## Usage

```python
from security_master_db import SecurityMasterDB

# Initialize (creates DB file if needed)
db = SecurityMasterDB("security_master.duckdb")

# Sync a daily snapshot (accepts pandas, polars, or pyarrow)
df = pd.read_csv("SecurityMaster_20251201.csv")
result = db.sync("2025-12-01", df)
# SyncResult(date='2025-12-01', rows_total=57798, rows_new=57798, ...)

# Reconstruct snapshot for any synced date
snapshot = db.get_data("2025-12-01")  # returns pyarrow Table

# Check what's been synced
db.get_synced_dates()  # ['2025-12-01', '2025-12-02', ...]

db.close()
```

### Out-of-order sync

Dates can be synced in any order. The SCD records are adjusted automatically:

```python
db.sync("2025-12-05", dec5_df)  # sync Dec 5 first
db.sync("2025-12-01", dec1_df)  # then backfill Dec 1
db.get_data("2025-12-01")       # returns correct snapshot
```

## Schema

```sql
CREATE TABLE security_master (
    security_id VARCHAR NOT NULL,
    ticker VARCHAR,
    mic VARCHAR,
    isin VARCHAR,
    description VARCHAR,
    sub_industry INTEGER,
    country VARCHAR,
    currency VARCHAR,
    country_risk VARCHAR,
    valid_from DATE NOT NULL,
    valid_to DATE,              -- NULL = current
    PRIMARY KEY (security_id, valid_from)
);

CREATE TABLE sync_metadata (
    as_of_date DATE PRIMARY KEY,
    synced_at TIMESTAMP,
    row_count INTEGER
);
```

## Setup

```bash
uv init && uv add pandas duckdb pyarrow polars
```

## Sync Logic

See [SYNC_LOGIC.md](SYNC_LOGIC.md) for detailed operation cases.
