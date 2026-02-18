# scduck

SCD Type 2 tables with DuckDB. Track historical changes to slowly-changing data without storing redundant snapshots.

**13 days of data: 65 MB CSV -> 6.3 MB DuckDB (~10x compression)**

## How it works

Records are stored with `valid_from` / `valid_to` date ranges. When data doesn't change, no new rows are written. Only changes generate new records.

```
id   | name  | price | valid_from | valid_to
P001 | Widget| 9.99  | 2025-01-01 | 2025-03-15  # original price
P001 | Widget| 12.99 | 2025-03-15 | NULL        # price changed
P002 | Gadget| 4.99  | 2025-01-01 | NULL        # unchanged
```

- `valid_from`: inclusive (>=)
- `valid_to`: exclusive (<), NULL = current

## Usage

```python
from scduck import SCDTable

# Define your schema
with SCDTable(
    "products.duckdb",
    table="products",
    keys=["product_id"],
    values=["name", "price", "category"]
) as db:
    # Sync daily snapshots (pandas, polars, or pyarrow)
    result = db.sync("2025-01-01", df_jan1)  # returns SyncResult
    db.sync("2025-01-02", df_jan2)

    # Reconstruct any historical snapshot
    snapshot = db.get_data("2025-01-01")  # returns pyarrow Table

    # Check synced dates
    db.get_synced_dates()  # ['2025-01-01', '2025-01-02']
```

### Out-of-order sync

Dates can be synced in any order:

```python
db.sync("2025-01-15", df)  # sync Jan 15 first
db.sync("2025-01-01", df)  # backfill Jan 1
db.get_data("2025-01-01")  # returns correct snapshot
```

## Example: SecurityMaster

```python
import pandas as pd
from scduck import SCDTable

with SCDTable(
    "security_master.duckdb",
    table="securities",
    keys=["security_id"],
    values=["ticker", "mic", "isin", "description",
            "sub_industry", "country", "currency", "country_risk"]
) as db:
    df = pd.read_csv("SecurityMaster_20251201.csv")
    db.sync("2025-12-01", df)
```

## Installation

```bash
pip install scduck

# With pandas/polars support
pip install scduck[all]
```

## Sync Logic

See [SYNC_LOGIC.md](SYNC_LOGIC.md) for detailed operation cases.
