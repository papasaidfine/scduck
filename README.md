# scduck

[![PyPI version](https://img.shields.io/pypi/v/scduck.svg)](https://pypi.org/project/scduck/)
[![Python versions](https://img.shields.io/pypi/pyversions/scduck.svg)](https://pypi.org/project/scduck/)
[![License](https://img.shields.io/pypi/l/scduck.svg)](https://github.com/papasaidfine/scduck/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-repo-blue.svg)](https://github.com/papasaidfine/scduck)

Store time series of snapshots in a SCD Type 2 table. Only changes are stored — unchanged records are not duplicated.

---

## Features

- **Efficient storage** — identical records across days are not duplicated
- **Out-of-order sync** — backfill or sync dates in any order
- **Flexible input** — accepts pandas, polars, or pyarrow DataFrames
- **Simple API** — sync snapshots, retrieve any historical date

---

## Installation

```bash
pip install scduck

# With pandas/polars support
pip install scduck[all]
```

---

## Quick Start

```python
from scduck import SCDTable

with SCDTable(
    "products.duckdb",
    table="products",
    keys=["product_id"],
    values=["name", "price", "category"]
) as db:
    # Sync daily snapshots
    db.sync("2025-01-01", df_jan1)
    db.sync("2025-01-02", df_jan2)

    # Retrieve any historical snapshot
    db.get_data("2025-01-01")  # returns pyarrow Table

    # Check synced dates
    db.get_synced_dates()  # ['2025-01-01', '2025-01-02']
```

---

## How It Works

Records are stored with `valid_from` / `valid_to` date ranges:

| id   | name   | price | valid_from | valid_to   |
|------|--------|-------|------------|------------|
| P001 | Widget | 9.99  | 2025-01-01 | 2025-03-15 |
| P001 | Widget | 12.99 | 2025-03-15 | NULL       |
| P002 | Gadget | 4.99  | 2025-01-01 | NULL       |

- `valid_from`: inclusive, `valid_to`: exclusive
- `NULL` = current/active record

---

## Out-of-Order Sync

Dates can be synced in any order:

```python
db.sync("2025-01-15", df)  # sync Jan 15 first
db.sync("2025-01-01", df)  # backfill Jan 1 later
db.get_data("2025-01-01")  # works correctly
```

---

## Documentation

See [SYNC_LOGIC.md](SYNC_LOGIC.md) for detailed sync operation cases.
