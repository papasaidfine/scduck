# SecurityMaster DuckDB Persistence

Lightweight DuckDB-based storage for SecurityMaster snapshots using SCD Type 2 with strict coverage. Instead of saving identical CSVs day after day, only changes are stored.

**13 days of data: 65 MB CSV -> 44.5 MB DuckDB**

## How it works

Each security is stored once with a `valid_from` / `valid_to` date range. When data doesn't change between days, no new rows are written. Only new, changed, or deleted securities generate records.

**Strict coverage model**: `valid_to` is always set to the next business day (never NULL). This ensures we only claim coverage for dates we've actually synced.

```
security_id | ticker | country | valid_from | valid_to
04MXXQYNG9  | AAPL   | US      | 2025-12-01 | 2025-12-18  # unchanged Dec 1-17
K9RQE6VQGZ  | AAL    | US      | 2025-12-01 | 2025-12-05  # changed on Dec 5
K9RQE6VQGZ  | AAL    | US      | 2025-12-05 | 2025-12-18  # new version
```

Date range semantics: `valid_from` is **inclusive** (>=), `valid_to` is **exclusive** (<).

## Usage

```python
from security_master_db import SecurityMasterDB

# Business calendar is REQUIRED - list of valid trading dates
business_dates = ["2025-12-01", "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05",
                  "2025-12-08", "2025-12-09", ...]  # excludes weekends/holidays

# Initialize (creates DB file if needed)
db = SecurityMasterDB("security_master.duckdb", business_dates)

# Sync a daily snapshot (accepts pandas, polars, or pyarrow)
df = pd.read_csv("SecurityMaster_20251201.csv")
result = db.sync("2025-12-01", df)
# SyncResult(date='2025-12-01', rows_total=57798, rows_new=57798,
#            rows_changed=0, rows_deleted=0, rows_unchanged=0)

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
db.get_data("2025-12-01")       # still returns correct snapshot
```

## Sync operations by scenario

When `sync(date, df)` is called, the following logic applies:

### Security IS in incoming snapshot

| Scenario | Condition | Operation |
|----------|-----------|-----------|
| **Unchanged** | Record covers date, data identical | No change |
| **Changed** | Record covers date, data differs | Close old record (`valid_to = date`), insert new record |
| **Backfill (same)** | No covering record, later record exists with same data | Extend later record backwards (`valid_from = date`) |
| **Backfill (diff)** | No covering record, later record exists with different data | Insert new record (`valid_to = later.valid_from`) |
| **Extend forward** | No covering record, prev record ends at date, same data | Extend prev record forward (`valid_to = next_biz(date)`) |
| **Reappearance** | No covering record, prev record with gap or different data | Insert new record (`valid_to = next_biz(date)`) |
| **New** | No records exist for this security | Insert record (`valid_to = next_biz(date)`) |

### Security NOT in incoming snapshot (deletion)

| Scenario | Condition | Operation |
|----------|-----------|-----------|
| **Deleted** | Record covers date | Close record (`valid_to = date`). If security was present at later synced dates, reopen coverage from next such date. |
| **Already gone** | No record covers date | No change |

### Example walkthrough

```
Sync Dec 5: AAPL present with data A
  → No records exist
  → INSERT (valid_from=Dec5, valid_to=Dec8)

Sync Dec 1: AAPL present with data A
  → No record covers Dec 1, but Dec 5 record exists with same data
  → EXTEND Dec 5 record backwards: valid_from = Dec 1
  → Result: (valid_from=Dec1, valid_to=Dec8)

Sync Dec 3: AAPL present with data B (different!)
  → Record (Dec1, Dec8) covers Dec 3, data differs
  → CLOSE old: (Dec1, Dec3) with A
  → INSERT new: (Dec3, Dec8) with B
```

## Schema

```sql
-- SCD Type 2 records
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
    valid_from DATE NOT NULL,   -- inclusive (>=)
    valid_to DATE NOT NULL,     -- exclusive (<), always next business day
    PRIMARY KEY (security_id, valid_from)
);

-- Tracks which dates have been synced
CREATE TABLE sync_metadata (
    as_of_date DATE PRIMARY KEY,
    synced_at TIMESTAMP,
    row_count INTEGER
);

-- Tracks security presence for out-of-order sync support
CREATE TABLE security_presence (
    security_id VARCHAR NOT NULL,
    as_of_date DATE NOT NULL,
    PRIMARY KEY (security_id, as_of_date)
);
```

## Setup

```bash
uv init && uv add pandas duckdb pyarrow polars
```
