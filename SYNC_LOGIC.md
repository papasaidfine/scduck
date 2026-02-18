# Sync Logic

This document describes how `SCDTable.sync()` handles each case when syncing a snapshot.

## Model

- `valid_from`: inclusive (>=), the date this record became effective
- `valid_to`: exclusive (<), NULL means current/forever
- Out-of-order sync supported
- No separate presence tracking table needed

## Sync Operations

When `sync(date, df)` is called, each record falls into one of these cases:

---

### Record IS in incoming snapshot

#### Case 1: Covered, same data
**Condition**: Record exists where `valid_from <= date` AND (`valid_to > date` OR `valid_to IS NULL`), data identical

**Operation**: No change

---

#### Case 2: Covered, different data
**Condition**: Record covers date, data differs

**Operation**:
1. Close old record: `valid_to = date`
2. Insert new record: `valid_from = date`, `valid_to = old_record.valid_to`

---

#### Case 3: Not covered, has NEXT record (valid_from > date)

**3a. Same data**:
- Extend next record backwards: `valid_from = date`

**3b. Different data**:
- Insert new record: `valid_from = date`, `valid_to = next.valid_from`

---

#### Case 4: Reappearance (not covered, no next, has prev record)

**Operation**: Insert new record with:
- `valid_from = date`
- `valid_to = earliest synced date after this date where record has no coverage, or NULL`

---

#### Case 5: New record (no existing SCD records)

**Operation**: Insert record with:
- `valid_from = date`
- `valid_to = earliest synced date after this date where record has no coverage, or NULL`

---

### Record NOT in incoming snapshot

#### Case 6: Has covering record

**Operation**:
1. Close record: `valid_to = date`
2. If there are synced dates after this date that were covered by the original record, re-open from the earliest such date

---

#### Case 7: No covering record

**Operation**: No change

---

## Key Insight: Inferring Presence

We don't need a separate presence tracking table because:

1. **sync_metadata** tells us which dates have been synced
2. **SCD records** tell us coverage ranges

When a record covers a synced date and wasn't closed at that date, the record must have been present when that date was synced.

Example:
- Record (Dec 1, NULL) exists
- sync_metadata shows Dec 5 was synced
- If Dec 5 was synced and record still covers it, record was present at Dec 5
- If record was absent at Dec 5, it would have been closed at Dec 5

This allows correct handling of deletions during out-of-order sync without storing per-record presence.

---

## Example: Out-of-order with deletion

```
Sync Dec 17: Record X present
  → Case 5: INSERT (Dec 17, NULL)

Sync Dec 1: Record X present, same data
  → Case 3a: Extend backwards (Dec 1, NULL)

Sync Dec 5: Record X present
  → Case 1: Covered, unchanged

Sync Dec 3: Record X NOT present
  → Case 6: Close at Dec 3: (Dec 1, Dec 3)
  → Re-open from next synced date (Dec 5): INSERT (Dec 5, NULL)

Result: (Dec 1, Dec 3), (Dec 5, NULL) ✓
```
