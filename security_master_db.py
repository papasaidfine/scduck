"""
SecurityMaster DuckDB persistence layer.

SCD Type 2 storage with strict coverage:
- valid_from: inclusive (>=)
- valid_to: exclusive (<), always next business day (no NULLs)
- Business calendar required
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa


DataFrameLike = Union[pd.DataFrame, pl.DataFrame, pa.Table]


@dataclass
class SyncResult:
    date: str
    rows_total: int
    rows_new: int
    rows_changed: int
    rows_deleted: int
    rows_unchanged: int
    rows_extended_back: int
    rows_extended_forward: int


class SecurityMasterDB:
    """DuckDB-based persistence for SecurityMaster using SCD Type 2 with strict coverage."""

    KEY_COLS = ["security_id"]
    VALUE_COLS = ["ticker", "mic", "isin", "description", "sub_industry",
                  "country", "currency", "country_risk"]
    ALL_COLS = KEY_COLS + VALUE_COLS

    def __init__(self, db_path: str | Path, business_dates: list[str]):
        """
        Initialize connection to DuckDB file.

        Args:
            db_path: Path to the DuckDB file
            business_dates: Ordered list of valid business dates (yyyy-mm-dd). Required.
        """
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

        if not business_dates:
            raise ValueError("business_dates is required for strict coverage model")
        self._business_dates = list(business_dates)
        self._biz_index = {d: i for i, d in enumerate(self._business_dates)}

    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS security_master (
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
                valid_to DATE NOT NULL,
                PRIMARY KEY (security_id, valid_from)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_metadata (
                as_of_date DATE PRIMARY KEY,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER
            )
        """)
        # Track which securities were confirmed present at each synced date
        # This enables correct handling of deletions during out-of-order sync
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS security_presence (
                security_id VARCHAR NOT NULL,
                as_of_date DATE NOT NULL,
                PRIMARY KEY (security_id, as_of_date)
            )
        """)

    def next_biz(self, date: str) -> str:
        """Get next business day after date."""
        idx = self._biz_index.get(date)
        if idx is None:
            raise ValueError(f"{date} not in business calendar")
        if idx + 1 >= len(self._business_dates):
            raise ValueError(f"No business day after {date}")
        return self._business_dates[idx + 1]

    def _to_arrow(self, df: DataFrameLike) -> pa.Table:
        """Convert any supported DataFrame type to pyarrow Table."""
        if isinstance(df, pa.Table):
            return df
        elif isinstance(df, pd.DataFrame):
            return pa.Table.from_pandas(df)
        elif isinstance(df, pl.DataFrame):
            return df.to_arrow()
        else:
            raise TypeError(f"Unsupported DataFrame type: {type(df)}")

    def _normalize_columns(self, table: pa.Table) -> pa.Table:
        """Normalize column names to match schema."""
        def normalize_name(name: str) -> str:
            return name.lower().replace("-", "").replace("_", "")

        col_map = {normalize_name(c): c for c in table.column_names}
        arrays = []
        names = []
        for schema_col in self.ALL_COLS:
            normalized = normalize_name(schema_col)
            if normalized in col_map:
                actual_col = col_map[normalized]
                arrays.append(table.column(actual_col))
                names.append(schema_col)
        return pa.table(dict(zip(names, arrays)))

    def sync(self, date: str, df: DataFrameLike) -> SyncResult:
        """
        Sync a snapshot for the given date using SCD Type 2 with strict coverage.
        Uses batch SQL operations for performance.
        """
        next_biz_date = self.next_biz(date)

        table = self._to_arrow(df)
        table = self._normalize_columns(table)
        row_count = table.num_rows

        # Register incoming data
        self.conn.register("_incoming", table)

        self.conn.execute("BEGIN TRANSACTION")
        try:
            # Record presence of all incoming securities for this date
            # This enables correct out-of-order sync with deletions
            self.conn.execute(f"""
                INSERT OR REPLACE INTO security_presence (security_id, as_of_date)
                SELECT security_id, DATE '{date}' FROM _incoming
            """)

            # Create temp table with comparison results using SQL JOINs
            # This avoids Python loops over 50K+ rows

            value_cols_sql = ", ".join(self.VALUE_COLS)

            # Compare incoming with covering records (valid_from <= date < valid_to)
            self.conn.execute(f"""
                CREATE TEMP TABLE _covering AS
                SELECT
                    i.security_id,
                    {', '.join(f'i.{c} as i_{c}' for c in self.VALUE_COLS)},
                    sm.valid_from as sm_valid_from,
                    sm.valid_to as sm_valid_to,
                    {', '.join(f'sm.{c} as sm_{c}' for c in self.VALUE_COLS)}
                FROM _incoming i
                LEFT JOIN security_master sm
                    ON i.security_id = sm.security_id
                    AND sm.valid_from <= DATE '{date}'
                    AND sm.valid_to > DATE '{date}'
            """)

            # Find next records (valid_from > date) for non-covered securities
            self.conn.execute(f"""
                CREATE TEMP TABLE _next AS
                SELECT DISTINCT ON (i.security_id)
                    i.security_id,
                    {', '.join(f'i.{c} as i_{c}' for c in self.VALUE_COLS)},
                    sm.valid_from as sm_valid_from,
                    sm.valid_to as sm_valid_to,
                    {', '.join(f'sm.{c} as sm_{c}' for c in self.VALUE_COLS)}
                FROM _incoming i
                JOIN security_master sm
                    ON i.security_id = sm.security_id
                    AND sm.valid_from > DATE '{date}'
                WHERE i.security_id NOT IN (
                    SELECT security_id FROM _covering WHERE sm_valid_from IS NOT NULL
                )
                ORDER BY i.security_id, sm.valid_from
            """)

            # Find prev records (valid_to <= date) for non-covered, non-next securities
            self.conn.execute(f"""
                CREATE TEMP TABLE _prev AS
                SELECT DISTINCT ON (i.security_id)
                    i.security_id,
                    {', '.join(f'i.{c} as i_{c}' for c in self.VALUE_COLS)},
                    sm.valid_from as sm_valid_from,
                    sm.valid_to as sm_valid_to,
                    {', '.join(f'sm.{c} as sm_{c}' for c in self.VALUE_COLS)}
                FROM _incoming i
                JOIN security_master sm
                    ON i.security_id = sm.security_id
                    AND sm.valid_to <= DATE '{date}'
                WHERE i.security_id NOT IN (
                    SELECT security_id FROM _covering WHERE sm_valid_from IS NOT NULL
                )
                AND i.security_id NOT IN (SELECT security_id FROM _next)
                ORDER BY i.security_id, sm.valid_to DESC
            """)

            # Build comparison expression for value columns
            same_expr = " AND ".join(
                f"((i_{c} IS NULL AND sm_{c} IS NULL) OR i_{c} = sm_{c})"
                for c in self.VALUE_COLS
            )

            # Case 2a: Covered, same data -> no change (just count)
            unchanged = self.conn.execute(f"""
                SELECT COUNT(*) FROM _covering
                WHERE sm_valid_from IS NOT NULL AND ({same_expr})
            """).fetchone()[0]

            # Case 2b: Covered, different data -> close old, insert new
            changed_covered = self.conn.execute(f"""
                SELECT security_id, sm_valid_from, sm_valid_to,
                       {', '.join(f'i_{c}' for c in self.VALUE_COLS)}
                FROM _covering
                WHERE sm_valid_from IS NOT NULL AND NOT ({same_expr})
            """).fetchdf()

            # Case 3a: Has next record, same data -> extend next backwards
            extend_back = self.conn.execute(f"""
                SELECT security_id, sm_valid_from
                FROM _next WHERE {same_expr}
            """).fetchdf()

            # Case 3b: Has next record, different data -> insert before next
            insert_before_next = self.conn.execute(f"""
                SELECT security_id, sm_valid_from as next_valid_from,
                       {', '.join(f'i_{c}' for c in self.VALUE_COLS)}
                FROM _next WHERE NOT ({same_expr})
            """).fetchdf()

            # Case 4a: Has prev record ending exactly at sync date, same data -> extend prev forward
            # IMPORTANT: Only extend if there's NO GAP (sm_valid_to == date)
            extend_forward = self.conn.execute(f"""
                SELECT security_id, sm_valid_from
                FROM _prev WHERE sm_valid_to = DATE '{date}' AND ({same_expr})
            """).fetchdf()

            # Case 4b: Has prev record but gap exists OR different data -> insert new record
            # (security reappeared after a gap, or data changed)
            insert_after_prev = self.conn.execute(f"""
                SELECT security_id,
                       {', '.join(f'i_{c}' for c in self.VALUE_COLS)}
                FROM _prev WHERE sm_valid_to < DATE '{date}' OR NOT ({same_expr})
            """).fetchdf()

            # Case 1: Completely new securities
            new_securities = self.conn.execute(f"""
                SELECT i.security_id, {', '.join(f'i.{c}' for c in self.VALUE_COLS)}
                FROM _incoming i
                WHERE i.security_id NOT IN (
                    SELECT security_id FROM _covering WHERE sm_valid_from IS NOT NULL
                )
                AND i.security_id NOT IN (SELECT security_id FROM _next)
                AND i.security_id NOT IN (SELECT security_id FROM _prev)
            """).fetchdf()

            # Case 6: Deletions - securities with covering record but not in incoming
            # Also capture original valid_to and data for potential re-opening
            deletions = self.conn.execute(f"""
                SELECT sm.security_id, sm.valid_from, sm.valid_to,
                       {', '.join(f'sm.{c}' for c in self.VALUE_COLS)}
                FROM security_master sm
                WHERE sm.valid_from <= DATE '{date}'
                  AND sm.valid_to > DATE '{date}'
                  AND sm.security_id NOT IN (SELECT security_id FROM _incoming)
            """).fetchdf()

            # Execute batch updates and inserts (no Python loops)

            # Case 2b: Close covered records that changed
            if len(changed_covered) > 0:
                self.conn.execute(f"""
                    UPDATE security_master sm SET valid_to = DATE '{date}'
                    FROM _covering c
                    WHERE sm.security_id = c.security_id
                      AND sm.valid_from = c.sm_valid_from
                      AND c.sm_valid_from IS NOT NULL
                      AND NOT ({same_expr})
                """)
                # Insert new versions
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT security_id, {', '.join(f'i_{c}' for c in self.VALUE_COLS)},
                           DATE '{date}', sm_valid_to
                    FROM _covering
                    WHERE sm_valid_from IS NOT NULL AND NOT ({same_expr})
                """)

            # Case 3a: Extend next records backwards
            if len(extend_back) > 0:
                self.conn.execute(f"""
                    UPDATE security_master sm SET valid_from = DATE '{date}'
                    FROM _next n
                    WHERE sm.security_id = n.security_id
                      AND sm.valid_from = n.sm_valid_from
                      AND ({same_expr.replace('i_', 'n.i_').replace('sm_', 'n.sm_')})
                """)

            # Case 3b: Insert before next
            if len(insert_before_next) > 0:
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT security_id, {', '.join(f'i_{c}' for c in self.VALUE_COLS)},
                           DATE '{date}', sm_valid_from
                    FROM _next
                    WHERE NOT ({same_expr})
                """)

            # Case 4a: Extend prev records forward (only if no gap)
            if len(extend_forward) > 0:
                self.conn.execute(f"""
                    UPDATE security_master sm SET valid_to = DATE '{next_biz_date}'
                    FROM _prev p
                    WHERE sm.security_id = p.security_id
                      AND sm.valid_from = p.sm_valid_from
                      AND p.sm_valid_to = DATE '{date}'
                      AND ({same_expr.replace('i_', 'p.i_').replace('sm_', 'p.sm_')})
                """)

            # Case 4b: Insert after prev (gap exists OR different data)
            if len(insert_after_prev) > 0:
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT security_id, {', '.join(f'i_{c}' for c in self.VALUE_COLS)},
                           DATE '{date}', DATE '{next_biz_date}'
                    FROM _prev
                    WHERE sm_valid_to < DATE '{date}' OR NOT ({same_expr})
                """)

            # Case 1: Insert new securities
            if len(new_securities) > 0:
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT i.security_id, {', '.join(f'i.{c}' for c in self.VALUE_COLS)},
                           DATE '{date}', DATE '{next_biz_date}'
                    FROM _incoming i
                    WHERE i.security_id NOT IN (
                        SELECT security_id FROM _covering WHERE sm_valid_from IS NOT NULL
                    )
                    AND i.security_id NOT IN (SELECT security_id FROM _next)
                    AND i.security_id NOT IN (SELECT security_id FROM _prev)
                """)

            # Case 6: Close deleted records and potentially re-open at next synced date
            if len(deletions) > 0:
                # First, close the deleted records
                self.conn.execute(f"""
                    UPDATE security_master sm SET valid_to = DATE '{date}'
                    WHERE sm.valid_from <= DATE '{date}'
                      AND sm.valid_to > DATE '{date}'
                      AND sm.security_id NOT IN (SELECT security_id FROM _incoming)
                """)

                # For out-of-order sync support: if the original record extended beyond
                # the current date and the security was confirmed present at a later date,
                # we need to re-open coverage starting from that later date.
                self.conn.register("_deletions", deletions)
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT
                        d.security_id,
                        {', '.join(f'd.{c}' for c in self.VALUE_COLS)},
                        sp.as_of_date as valid_from,
                        d.valid_to
                    FROM _deletions d
                    -- Join with security_presence to find dates where this security was confirmed
                    JOIN security_presence sp ON sp.security_id = d.security_id
                                             AND sp.as_of_date > DATE '{date}'
                                             AND sp.as_of_date < d.valid_to
                    WHERE NOT EXISTS (
                        -- Don't insert if there's already a record covering that date
                        SELECT 1 FROM security_master s
                        WHERE s.security_id = d.security_id
                          AND s.valid_from <= sp.as_of_date
                          AND s.valid_to > sp.as_of_date
                    )
                    AND NOT EXISTS (
                        -- Don't insert if there's already a record starting at that date
                        SELECT 1 FROM security_master s
                        WHERE s.security_id = d.security_id
                          AND s.valid_from = sp.as_of_date
                    )
                    -- Pick the earliest confirmed date after the deletion
                    AND sp.as_of_date = (
                        SELECT MIN(sp2.as_of_date)
                        FROM security_presence sp2
                        WHERE sp2.security_id = d.security_id
                          AND sp2.as_of_date > DATE '{date}'
                          AND sp2.as_of_date < d.valid_to
                    )
                """)
                self.conn.unregister("_deletions")

            # Update sync metadata
            self.conn.execute("""
                INSERT OR REPLACE INTO sync_metadata (as_of_date, synced_at, row_count)
                VALUES (?, ?, ?)
            """, [date, datetime.now(), row_count])

            # Cleanup temp tables
            self.conn.execute("DROP TABLE IF EXISTS _covering")
            self.conn.execute("DROP TABLE IF EXISTS _next")
            self.conn.execute("DROP TABLE IF EXISTS _prev")

            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        finally:
            self.conn.unregister("_incoming")

        return SyncResult(
            date=date,
            rows_total=row_count,
            rows_new=len(new_securities),
            rows_changed=len(changed_covered) + len(insert_before_next) + len(insert_after_prev),
            rows_deleted=len(deletions),
            rows_unchanged=unchanged,
            rows_extended_back=len(extend_back),
            rows_extended_forward=len(extend_forward)
        )

    def get_data(self, date: str) -> pa.Table:
        """Reconstruct snapshot for a date."""
        return self.conn.execute("""
            SELECT security_id, ticker, mic, isin, description,
                   sub_industry, country, currency, country_risk
            FROM security_master
            WHERE valid_from <= ?
              AND valid_to > ?
        """, [date, date]).fetch_arrow_table()

    def get_synced_dates(self) -> list[str]:
        """Return list of synced dates."""
        result = self.conn.execute(
            "SELECT as_of_date FROM sync_metadata ORDER BY as_of_date"
        ).fetchall()
        return [str(row[0])[:10] for row in result]

    def get_record_count(self) -> int:
        """Return total number of records."""
        return self.conn.execute("SELECT COUNT(*) FROM security_master").fetchone()[0]

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
