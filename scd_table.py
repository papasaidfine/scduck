"""
Generic SCD Type 2 DuckDB persistence layer.

- valid_from: inclusive (>=)
- valid_to: exclusive (<), NULL means current/forever
- Out-of-order sync supported
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


class SCDTable:
    """Generic SCD Type 2 table with DuckDB backend."""

    def __init__(
        self,
        db_path: str | Path,
        table: str,
        keys: list[str],
        values: list[str],
    ):
        """
        Initialize SCD table.

        Args:
            db_path: Path to DuckDB file
            table: Table name
            keys: Primary key column(s) for identifying records
            values: Value columns to track for changes
        """
        self.db_path = Path(db_path)
        self.table = table
        self.keys = list(keys)
        self.values = list(values)
        self.all_cols = self.keys + self.values

        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        # Build column definitions (all VARCHAR for simplicity)
        key_cols = ", ".join(f"{col} VARCHAR NOT NULL" for col in self.keys)
        value_cols = ", ".join(f"{col} VARCHAR" for col in self.values)
        pk_cols = ", ".join(self.keys)

        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                {key_cols},
                {value_cols},
                valid_from DATE NOT NULL,
                valid_to DATE,
                PRIMARY KEY ({pk_cols}, valid_from)
            )
        """)
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table}_sync_metadata (
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
        for schema_col in self.all_cols:
            normalized = normalize_name(schema_col)
            if normalized in col_map:
                actual_col = col_map[normalized]
                arrays.append(table.column(actual_col))
                names.append(schema_col)
        return pa.table(dict(zip(names, arrays)))

    def sync(self, date: str, df: DataFrameLike) -> SyncResult:
        """
        Sync a snapshot for the given date using SCD Type 2.
        """
        tbl = self.table
        meta = f"{self.table}_sync_metadata"

        table = self._to_arrow(df)
        table = self._normalize_columns(table)
        row_count = table.num_rows

        self.conn.register("_incoming", table)

        self.conn.execute("BEGIN TRANSACTION")
        try:
            # Case 1 & 2: Find covering records
            self.conn.execute(f"""
                CREATE TEMP TABLE _covering AS
                SELECT
                    {', '.join(f'i.{c} as i_{c}' for c in self.all_cols)},
                    sm.valid_from as sm_valid_from,
                    sm.valid_to as sm_valid_to,
                    {', '.join(f'sm.{c} as sm_{c}' for c in self.values)}
                FROM _incoming i
                LEFT JOIN {tbl} sm
                    ON {' AND '.join(f'i.{k} = sm.{k}' for k in self.keys)}
                    AND sm.valid_from <= DATE '{date}'
                    AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
            """)

            # Case 3: Find next records for non-covered
            key_cols_i = ', '.join(f'i.{k}' for k in self.keys)
            self.conn.execute(f"""
                CREATE TEMP TABLE _next AS
                SELECT DISTINCT ON ({key_cols_i})
                    {', '.join(f'i.{c} as i_{c}' for c in self.all_cols)},
                    sm.valid_from as sm_valid_from,
                    sm.valid_to as sm_valid_to,
                    {', '.join(f'sm.{c} as sm_{c}' for c in self.values)}
                FROM _incoming i
                JOIN {tbl} sm
                    ON {' AND '.join(f'i.{k} = sm.{k}' for k in self.keys)}
                    AND sm.valid_from > DATE '{date}'
                WHERE ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                    SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _covering WHERE sm_valid_from IS NOT NULL
                )
                ORDER BY {key_cols_i}, sm.valid_from
            """)

            # Case 4: Find prev records (reappearance)
            self.conn.execute(f"""
                CREATE TEMP TABLE _prev AS
                SELECT DISTINCT ON ({key_cols_i})
                    {', '.join(f'i.{c} as i_{c}' for c in self.all_cols)}
                FROM _incoming i
                JOIN {tbl} sm
                    ON {' AND '.join(f'i.{k} = sm.{k}' for k in self.keys)}
                    AND sm.valid_to <= DATE '{date}'
                WHERE ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                    SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _covering WHERE sm_valid_from IS NOT NULL
                )
                AND ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                    SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _next
                )
                ORDER BY {key_cols_i}, sm.valid_to DESC
            """)

            # Build comparison expression
            same_expr = " AND ".join(
                f"((i_{c} IS NULL AND sm_{c} IS NULL) OR i_{c} = sm_{c})"
                for c in self.values
            )

            # Case 1: Unchanged
            unchanged = self.conn.execute(f"""
                SELECT COUNT(*) FROM _covering
                WHERE sm_valid_from IS NOT NULL AND ({same_expr})
            """).fetchone()[0]

            # Case 2: Changed
            changed_covered = self.conn.execute(f"""
                SELECT {', '.join(f'i_{k}' for k in self.keys)}, sm_valid_from, sm_valid_to
                FROM _covering
                WHERE sm_valid_from IS NOT NULL AND NOT ({same_expr})
            """).fetchdf()

            # Case 3a: Extend back
            extend_back = self.conn.execute(f"""
                SELECT {', '.join(f'i_{k}' for k in self.keys)}, sm_valid_from
                FROM _next WHERE {same_expr}
            """).fetchdf()

            # Case 3b: Insert before next
            insert_before_next = self.conn.execute(f"""
                SELECT {', '.join(f'i_{k}' for k in self.keys)}, sm_valid_from as next_valid_from
                FROM _next WHERE NOT ({same_expr})
            """).fetchdf()

            # Case 4: Reappearance
            reappeared = self.conn.execute(f"""
                SELECT {', '.join(f'i_{k}' for k in self.keys)}
                FROM _prev
            """).fetchdf()

            # Case 5: New
            new_records = self.conn.execute(f"""
                SELECT {', '.join(f'i.{c}' for c in self.keys)}
                FROM _incoming i
                WHERE ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                    SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _covering WHERE sm_valid_from IS NOT NULL
                )
                AND ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                    SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _next
                )
                AND ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                    SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _prev
                )
            """).fetchdf()

            # Case 6: Deletions
            deletions = self.conn.execute(f"""
                SELECT {', '.join(f'sm.{c}' for c in self.keys)}, sm.valid_from, sm.valid_to,
                       {', '.join(f'sm.{c}' for c in self.values)}
                FROM {tbl} sm
                WHERE sm.valid_from <= DATE '{date}'
                  AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
                  AND ({', '.join(f'sm.{k}' for k in self.keys)}) NOT IN (
                      SELECT {', '.join(k for k in self.keys)} FROM _incoming
                  )
            """).fetchdf()

            # Execute updates/inserts

            # Case 2: Close and insert new version
            if len(changed_covered) > 0:
                key_join = ' AND '.join(f'sm.{k} = c.i_{k}' for k in self.keys)
                self.conn.execute(f"""
                    UPDATE {tbl} sm SET valid_to = DATE '{date}'
                    FROM _covering c
                    WHERE {key_join}
                      AND sm.valid_from = c.sm_valid_from
                      AND c.sm_valid_from IS NOT NULL
                      AND NOT ({same_expr})
                """)
                self.conn.execute(f"""
                    INSERT INTO {tbl}
                    SELECT {', '.join(f'i_{c}' for c in self.all_cols)},
                           DATE '{date}', sm_valid_to
                    FROM _covering
                    WHERE sm_valid_from IS NOT NULL AND NOT ({same_expr})
                """)

            # Case 3a: Extend backwards
            if len(extend_back) > 0:
                key_join = ' AND '.join(f'sm.{k} = n.i_{k}' for k in self.keys)
                self.conn.execute(f"""
                    UPDATE {tbl} sm SET valid_from = DATE '{date}'
                    FROM _next n
                    WHERE {key_join}
                      AND sm.valid_from = n.sm_valid_from
                      AND ({same_expr.replace('i_', 'n.i_').replace('sm_', 'n.sm_')})
                """)

            # Case 3b: Insert before next
            if len(insert_before_next) > 0:
                self.conn.execute(f"""
                    INSERT INTO {tbl}
                    SELECT {', '.join(f'i_{c}' for c in self.all_cols)},
                           DATE '{date}', sm_valid_from
                    FROM _next
                    WHERE NOT ({same_expr})
                """)

            # Case 4: Insert reappeared
            if len(reappeared) > 0:
                key_match = ' AND '.join(f's.{k} = p.i_{k}' for k in self.keys)
                self.conn.execute(f"""
                    INSERT INTO {tbl}
                    SELECT {', '.join(f'p.i_{c}' for c in self.all_cols)},
                           DATE '{date}',
                           (SELECT MIN(sm.as_of_date)
                            FROM {meta} sm
                            WHERE sm.as_of_date > DATE '{date}'
                              AND NOT EXISTS (
                                  SELECT 1 FROM {tbl} s
                                  WHERE {key_match}
                                    AND s.valid_from <= sm.as_of_date
                                    AND (s.valid_to > sm.as_of_date OR s.valid_to IS NULL)
                              ))
                    FROM _prev p
                """)

            # Case 5: Insert new
            if len(new_records) > 0:
                key_match = ' AND '.join(f's.{k} = i.{k}' for k in self.keys)
                self.conn.execute(f"""
                    INSERT INTO {tbl}
                    SELECT {', '.join(f'i.{c}' for c in self.all_cols)},
                           DATE '{date}',
                           (SELECT MIN(sm.as_of_date)
                            FROM {meta} sm
                            WHERE sm.as_of_date > DATE '{date}'
                              AND NOT EXISTS (
                                  SELECT 1 FROM {tbl} s
                                  WHERE {key_match}
                                    AND s.valid_from <= sm.as_of_date
                                    AND (s.valid_to > sm.as_of_date OR s.valid_to IS NULL)
                              ))
                    FROM _incoming i
                    WHERE ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                        SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _covering WHERE sm_valid_from IS NOT NULL
                    )
                    AND ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                        SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _next
                    )
                    AND ({', '.join(f'i.{k}' for k in self.keys)}) NOT IN (
                        SELECT {', '.join(f'i_{k}' for k in self.keys)} FROM _prev
                    )
                """)

            # Case 6: Close deletions and re-open if needed
            if len(deletions) > 0:
                self.conn.execute(f"""
                    UPDATE {tbl} sm SET valid_to = DATE '{date}'
                    WHERE sm.valid_from <= DATE '{date}'
                      AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
                      AND ({', '.join(f'sm.{k}' for k in self.keys)}) NOT IN (
                          SELECT {', '.join(k for k in self.keys)} FROM _incoming
                      )
                """)

                # Re-open from next synced date if covered
                self.conn.register("_deletions", deletions)
                key_match = ' AND '.join(f's.{k} = d.{k}' for k in self.keys)
                self.conn.execute(f"""
                    INSERT INTO {tbl}
                    SELECT
                        {', '.join(f'd.{c}' for c in self.all_cols)},
                        sm.as_of_date,
                        d.valid_to
                    FROM _deletions d
                    JOIN {meta} sm
                        ON sm.as_of_date > DATE '{date}'
                        AND (d.valid_to IS NULL OR sm.as_of_date < d.valid_to)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {tbl} s
                        WHERE {key_match}
                          AND s.valid_from = sm.as_of_date
                    )
                    AND sm.as_of_date = (
                        SELECT MIN(sm2.as_of_date)
                        FROM {meta} sm2
                        WHERE sm2.as_of_date > DATE '{date}'
                          AND (d.valid_to IS NULL OR sm2.as_of_date < d.valid_to)
                    )
                """)
                self.conn.unregister("_deletions")

            # Update metadata
            self.conn.execute(f"""
                INSERT OR REPLACE INTO {meta} (as_of_date, synced_at, row_count)
                VALUES (?, ?, ?)
            """, [date, datetime.now(), row_count])

            # Cleanup
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
            rows_new=len(new_records),
            rows_changed=len(changed_covered) + len(insert_before_next),
            rows_deleted=len(deletions),
            rows_unchanged=unchanged,
            rows_extended_back=len(extend_back),
            rows_reappeared=len(reappeared)
        )

    def get_data(self, date: str) -> pa.Table:
        """Get snapshot for a date."""
        cols = ", ".join(self.all_cols)
        return self.conn.execute(f"""
            SELECT {cols}
            FROM {self.table}
            WHERE valid_from <= ?
              AND (valid_to > ? OR valid_to IS NULL)
        """, [date, date]).fetch_arrow_table()

    def get_synced_dates(self) -> list[str]:
        """Return list of synced dates."""
        result = self.conn.execute(f"""
            SELECT as_of_date FROM {self.table}_sync_metadata ORDER BY as_of_date
        """).fetchall()
        return [str(row[0])[:10] for row in result]

    def get_record_count(self) -> int:
        """Return total number of SCD records."""
        return self.conn.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()[0]

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
