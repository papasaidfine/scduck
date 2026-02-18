"""
SecurityMaster DuckDB persistence layer.

SCD Type 2 storage:
- valid_from: inclusive (>=)
- valid_to: exclusive (<), NULL means current/forever
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
    rows_reappeared: int


class SecurityMasterDB:
    """DuckDB-based persistence for SecurityMaster using SCD Type 2."""

    KEY_COLS = ["security_id"]
    VALUE_COLS = ["ticker", "mic", "isin", "description", "sub_industry",
                  "country", "currency", "country_risk"]
    ALL_COLS = KEY_COLS + VALUE_COLS

    def __init__(self, db_path: str | Path):
        """
        Initialize connection to DuckDB file.

        Args:
            db_path: Path to the DuckDB file
        """
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

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
                valid_to DATE,
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
        Sync a snapshot for the given date using SCD Type 2.
        Uses batch SQL operations for performance.
        """
        table = self._to_arrow(df)
        table = self._normalize_columns(table)
        row_count = table.num_rows

        # Register incoming data
        self.conn.register("_incoming", table)

        self.conn.execute("BEGIN TRANSACTION")
        try:
            value_cols_sql = ", ".join(self.VALUE_COLS)

            # Case 1 & 2: Find covering records (valid_from <= date AND (valid_to > date OR valid_to IS NULL))
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
                    AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
            """)

            # Case 3: Find next records (valid_from > date) for non-covered securities
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

            # Case 4: Find prev records (valid_to <= date) for non-covered, non-next securities
            # These are reappearances after deletion
            self.conn.execute(f"""
                CREATE TEMP TABLE _prev AS
                SELECT DISTINCT ON (i.security_id)
                    i.security_id,
                    {', '.join(f'i.{c} as i_{c}' for c in self.VALUE_COLS)}
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

            # Case 1: Covered, same data -> no change (just count)
            unchanged = self.conn.execute(f"""
                SELECT COUNT(*) FROM _covering
                WHERE sm_valid_from IS NOT NULL AND ({same_expr})
            """).fetchone()[0]

            # Case 2: Covered, different data -> close old, insert new
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

            # Case 4: Reappearance after deletion -> always insert new record
            reappeared = self.conn.execute(f"""
                SELECT security_id, {', '.join(f'i_{c}' for c in self.VALUE_COLS)}
                FROM _prev
            """).fetchdf()

            # Case 5: Completely new securities
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
            # Capture original valid_to and data for potential re-opening
            deletions = self.conn.execute(f"""
                SELECT sm.security_id, sm.valid_from, sm.valid_to,
                       {', '.join(f'sm.{c}' for c in self.VALUE_COLS)}
                FROM security_master sm
                WHERE sm.valid_from <= DATE '{date}'
                  AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
                  AND sm.security_id NOT IN (SELECT security_id FROM _incoming)
            """).fetchdf()

            # Execute batch updates and inserts

            # Case 2: Close covered records that changed, insert new versions
            if len(changed_covered) > 0:
                self.conn.execute(f"""
                    UPDATE security_master sm SET valid_to = DATE '{date}'
                    FROM _covering c
                    WHERE sm.security_id = c.security_id
                      AND sm.valid_from = c.sm_valid_from
                      AND c.sm_valid_from IS NOT NULL
                      AND NOT ({same_expr})
                """)
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

            # Case 4: Insert reappeared securities
            # valid_to = earliest synced date after current where security has no coverage, or NULL
            if len(reappeared) > 0:
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT p.security_id, {', '.join(f'p.i_{c}' for c in self.VALUE_COLS)},
                           DATE '{date}',
                           (SELECT MIN(sm.as_of_date)
                            FROM sync_metadata sm
                            WHERE sm.as_of_date > DATE '{date}'
                              AND NOT EXISTS (
                                  SELECT 1 FROM security_master s
                                  WHERE s.security_id = p.security_id
                                    AND s.valid_from <= sm.as_of_date
                                    AND (s.valid_to > sm.as_of_date OR s.valid_to IS NULL)
                              ))
                    FROM _prev p
                """)

            # Case 5: Insert new securities
            # valid_to = earliest synced date after current where security has no coverage, or NULL
            if len(new_securities) > 0:
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT i.security_id, {', '.join(f'i.{c}' for c in self.VALUE_COLS)},
                           DATE '{date}',
                           (SELECT MIN(sm.as_of_date)
                            FROM sync_metadata sm
                            WHERE sm.as_of_date > DATE '{date}'
                              AND NOT EXISTS (
                                  SELECT 1 FROM security_master s
                                  WHERE s.security_id = i.security_id
                                    AND s.valid_from <= sm.as_of_date
                                    AND (s.valid_to > sm.as_of_date OR s.valid_to IS NULL)
                              ))
                    FROM _incoming i
                    WHERE i.security_id NOT IN (
                        SELECT security_id FROM _covering WHERE sm_valid_from IS NOT NULL
                    )
                    AND i.security_id NOT IN (SELECT security_id FROM _next)
                    AND i.security_id NOT IN (SELECT security_id FROM _prev)
                """)

            # Case 6: Close deleted records and re-open from next synced date if needed
            if len(deletions) > 0:
                # Close the records
                self.conn.execute(f"""
                    UPDATE security_master sm SET valid_to = DATE '{date}'
                    WHERE sm.valid_from <= DATE '{date}'
                      AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
                      AND sm.security_id NOT IN (SELECT security_id FROM _incoming)
                """)

                # Re-open from the next synced date that was covered by the original record
                # If a synced date was covered and the record survived, the security was present
                self.conn.register("_deletions", deletions)
                self.conn.execute(f"""
                    INSERT INTO security_master
                    SELECT
                        d.security_id,
                        {', '.join(f'd.{c}' for c in self.VALUE_COLS)},
                        sm.as_of_date,
                        d.valid_to
                    FROM _deletions d
                    JOIN sync_metadata sm
                        ON sm.as_of_date > DATE '{date}'
                        AND (d.valid_to IS NULL OR sm.as_of_date < d.valid_to)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM security_master s
                        WHERE s.security_id = d.security_id
                          AND s.valid_from = sm.as_of_date
                    )
                    AND sm.as_of_date = (
                        SELECT MIN(sm2.as_of_date)
                        FROM sync_metadata sm2
                        WHERE sm2.as_of_date > DATE '{date}'
                          AND (d.valid_to IS NULL OR sm2.as_of_date < d.valid_to)
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
            rows_changed=len(changed_covered) + len(insert_before_next),
            rows_deleted=len(deletions),
            rows_unchanged=unchanged,
            rows_extended_back=len(extend_back),
            rows_reappeared=len(reappeared)
        )

    def get_data(self, date: str) -> pa.Table:
        """Reconstruct snapshot for a date."""
        return self.conn.execute("""
            SELECT security_id, ticker, mic, isin, description,
                   sub_industry, country, currency, country_risk
            FROM security_master
            WHERE valid_from <= ?
              AND (valid_to > ? OR valid_to IS NULL)
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
