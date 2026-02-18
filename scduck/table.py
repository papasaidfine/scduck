"""
Generic SCD Type 2 DuckDB persistence layer.

- valid_from: inclusive (>=)
- valid_to: exclusive (<), NULL means current/forever
- Out-of-order sync supported
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa


DataFrameLike = pd.DataFrame | pl.DataFrame | pa.Table


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
        self.db_path = Path(db_path)
        self.table = table
        self.keys = list(keys)
        self.values = list(values)
        self.all_cols = self.keys + self.values

        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

        # Pre-compute SQL fragments used throughout sync operations
        self._sql = self._build_sql_fragments()

    def _build_sql_fragments(self) -> dict[str, str]:
        """Pre-compute reusable SQL fragments for sync operations."""
        keys, values, all_cols = self.keys, self.values, self.all_cols
        return {
            # Column lists
            "i_cols": ", ".join(f"i.{c}" for c in all_cols),
            "i_keys": ", ".join(f"i.{k}" for k in keys),
            "i_col_aliases": ", ".join(f"i.{c} as i_{c}" for c in all_cols),
            "i_key_aliases": ", ".join(f"i_{k}" for k in keys),
            "sm_value_aliases": ", ".join(f"sm.{c} as sm_{c}" for c in values),
            "sm_keys": ", ".join(f"sm.{k}" for k in keys),
            "select_i_cols": ", ".join(f"i_{c}" for c in all_cols),
            # Join conditions
            "key_join_i_sm": " AND ".join(f"i.{k} = sm.{k}" for k in keys),
            # NULL-safe value comparison
            "same_values": " AND ".join(
                f"((i_{c} IS NULL AND sm_{c} IS NULL) OR i_{c} = sm_{c})"
                for c in values
            ),
        }

    def _not_in_covered(self, alias: str = "i") -> str:
        """SQL fragment: keys not in covered records."""
        keys = ", ".join(f"{alias}.{k}" for k in self.keys)
        i_keys = self._sql["i_key_aliases"]
        return f"({keys}) NOT IN (SELECT {i_keys} FROM _covering WHERE sm_valid_from IS NOT NULL)"

    def _not_in_next(self, alias: str = "i") -> str:
        """SQL fragment: keys not in next records."""
        keys = ", ".join(f"{alias}.{k}" for k in self.keys)
        i_keys = self._sql["i_key_aliases"]
        return f"({keys}) NOT IN (SELECT {i_keys} FROM _next)"

    def _not_in_prev(self, alias: str = "i") -> str:
        """SQL fragment: keys not in prev records."""
        keys = ", ".join(f"{alias}.{k}" for k in self.keys)
        i_keys = self._sql["i_key_aliases"]
        return f"({keys}) NOT IN (SELECT {i_keys} FROM _prev)"

    def _valid_to_subquery(self, date: str, key_alias: str, key_prefix: str) -> str:
        """SQL subquery to find valid_to date based on future synced dates."""
        meta = f"{self.table}_sync_metadata"
        key_match = " AND ".join(f"s.{k} = {key_alias}.{key_prefix}{k}" for k in self.keys)
        return f"""(SELECT MIN(sm.as_of_date)
            FROM {meta} sm
            WHERE sm.as_of_date > DATE '{date}'
              AND NOT EXISTS (
                  SELECT 1 FROM {self.table} s
                  WHERE {key_match}
                    AND s.valid_from <= sm.as_of_date
                    AND (s.valid_to > sm.as_of_date OR s.valid_to IS NULL)
              ))"""

    def _init_schema(self) -> None:
        """Create SCD table and metadata table if they don't exist."""
        key_defs = ", ".join(f"{col} VARCHAR NOT NULL" for col in self.keys)
        value_defs = ", ".join(f"{col} VARCHAR" for col in self.values)
        pk_cols = ", ".join(self.keys)

        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                {key_defs}, {value_defs},
                valid_from DATE NOT NULL, valid_to DATE,
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
        """Convert any supported DataFrame type to PyArrow Table."""
        if isinstance(df, pa.Table):
            return df
        if isinstance(df, pd.DataFrame):
            return pa.Table.from_pandas(df)
        if isinstance(df, pl.DataFrame):
            return df.to_arrow()
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
        """Sync a snapshot for the given date using SCD Type 2."""
        tbl = self.table
        meta = f"{self.table}_sync_metadata"
        sql = self._sql

        table = self._to_arrow(df)
        table = self._normalize_columns(table)
        row_count = table.num_rows

        self.conn.register("_incoming", table)

        self.conn.execute("BEGIN TRANSACTION")
        try:
            self._create_temp_tables(date, tbl, sql)
            stats = self._compute_sync_stats(date, tbl, sql)
            self._execute_sync_operations(date, tbl, meta, sql, stats)

            self.conn.execute(f"""
                INSERT OR REPLACE INTO {meta} (as_of_date, synced_at, row_count)
                VALUES (?, ?, ?)
            """, [date, datetime.now(), row_count])

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
            rows_new=stats["new_count"],
            rows_changed=stats["changed_count"] + stats["insert_before_next_count"],
            rows_deleted=stats["deletion_count"],
            rows_unchanged=stats["unchanged"],
            rows_extended_back=stats["extend_back_count"],
            rows_reappeared=stats["reappeared_count"],
        )

    def _create_temp_tables(self, date: str, tbl: str, sql: dict[str, str]) -> None:
        """Create temporary tables for sync categorization."""
        # _covering: records where existing SCD covers the sync date
        self.conn.execute(f"""
            CREATE TEMP TABLE _covering AS
            SELECT {sql["i_col_aliases"]},
                   sm.valid_from as sm_valid_from, sm.valid_to as sm_valid_to,
                   {sql["sm_value_aliases"]}
            FROM _incoming i
            LEFT JOIN {tbl} sm ON {sql["key_join_i_sm"]}
                AND sm.valid_from <= DATE '{date}'
                AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
        """)

        # _next: records with future SCD records (not covered)
        self.conn.execute(f"""
            CREATE TEMP TABLE _next AS
            SELECT DISTINCT ON ({sql["i_keys"]})
                {sql["i_col_aliases"]},
                sm.valid_from as sm_valid_from, sm.valid_to as sm_valid_to,
                {sql["sm_value_aliases"]}
            FROM _incoming i
            JOIN {tbl} sm ON {sql["key_join_i_sm"]} AND sm.valid_from > DATE '{date}'
            WHERE {self._not_in_covered()}
            ORDER BY {sql["i_keys"]}, sm.valid_from
        """)

        # _prev: records with past SCD records only (reappearance)
        self.conn.execute(f"""
            CREATE TEMP TABLE _prev AS
            SELECT DISTINCT ON ({sql["i_keys"]}) {sql["i_col_aliases"]}
            FROM _incoming i
            JOIN {tbl} sm ON {sql["key_join_i_sm"]} AND sm.valid_to <= DATE '{date}'
            WHERE {self._not_in_covered()} AND {self._not_in_next()}
            ORDER BY {sql["i_keys"]}, sm.valid_to DESC
        """)

    def _compute_sync_stats(self, date: str, tbl: str, sql: dict[str, str]) -> dict:
        """Compute counts for each sync case category."""
        same = sql["same_values"]
        sm_keys = sql["sm_keys"]
        all_keys = ", ".join(self.keys)

        def count(query: str) -> int:
            return self.conn.execute(query).fetchone()[0]

        unchanged = count(f"""
            SELECT COUNT(*) FROM _covering WHERE sm_valid_from IS NOT NULL AND ({same})
        """)
        changed_count = count(f"""
            SELECT COUNT(*) FROM _covering WHERE sm_valid_from IS NOT NULL AND NOT ({same})
        """)
        extend_back_count = count(f"SELECT COUNT(*) FROM _next WHERE {same}")
        insert_before_next_count = count(f"SELECT COUNT(*) FROM _next WHERE NOT ({same})")
        reappeared_count = count("SELECT COUNT(*) FROM _prev")
        new_count = count(f"""
            SELECT COUNT(*) FROM _incoming i
            WHERE {self._not_in_covered()} AND {self._not_in_next()} AND {self._not_in_prev()}
        """)

        deletions = self.conn.execute(f"""
            SELECT {sm_keys}, sm.valid_from, sm.valid_to,
                   {", ".join(f"sm.{c}" for c in self.values)}
            FROM {tbl} sm
            WHERE sm.valid_from <= DATE '{date}'
              AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
              AND ({sm_keys}) NOT IN (SELECT {all_keys} FROM _incoming)
        """).fetchdf()

        return {
            "unchanged": unchanged,
            "changed_count": changed_count,
            "extend_back_count": extend_back_count,
            "insert_before_next_count": insert_before_next_count,
            "reappeared_count": reappeared_count,
            "new_count": new_count,
            "deletions": deletions,
            "deletion_count": len(deletions),
        }

    def _execute_sync_operations(
        self, date: str, tbl: str, meta: str, sql: dict[str, str], stats: dict
    ) -> None:
        """Execute UPDATE and INSERT operations for each sync case."""
        same = sql["same_values"]
        select_cols = sql["select_i_cols"]

        # Case 2: Close old record and insert changed version
        if stats["changed_count"] > 0:
            key_join = " AND ".join(f"sm.{k} = c.i_{k}" for k in self.keys)
            self.conn.execute(f"""
                UPDATE {tbl} sm SET valid_to = DATE '{date}'
                FROM _covering c
                WHERE {key_join} AND sm.valid_from = c.sm_valid_from
                  AND c.sm_valid_from IS NOT NULL AND NOT ({same})
            """)
            self.conn.execute(f"""
                INSERT INTO {tbl}
                SELECT {select_cols}, DATE '{date}', sm_valid_to
                FROM _covering WHERE sm_valid_from IS NOT NULL AND NOT ({same})
            """)

        # Case 3a: Extend next record backwards
        if stats["extend_back_count"] > 0:
            key_join = " AND ".join(f"sm.{k} = n.i_{k}" for k in self.keys)
            same_prefixed = same.replace("i_", "n.i_").replace("sm_", "n.sm_")
            self.conn.execute(f"""
                UPDATE {tbl} sm SET valid_from = DATE '{date}'
                FROM _next n
                WHERE {key_join} AND sm.valid_from = n.sm_valid_from AND ({same_prefixed})
            """)

        # Case 3b: Insert before next record
        if stats["insert_before_next_count"] > 0:
            self.conn.execute(f"""
                INSERT INTO {tbl}
                SELECT {select_cols}, DATE '{date}', sm_valid_from
                FROM _next WHERE NOT ({same})
            """)

        # Case 4: Insert reappeared record
        if stats["reappeared_count"] > 0:
            valid_to = self._valid_to_subquery(date, "p", "i_")
            select_cols_p = ", ".join(f"p.i_{c}" for c in self.all_cols)
            self.conn.execute(f"""
                INSERT INTO {tbl}
                SELECT {select_cols_p}, DATE '{date}', {valid_to}
                FROM _prev p
            """)

        # Case 5: Insert new record
        if stats["new_count"] > 0:
            valid_to = self._valid_to_subquery(date, "i", "")
            self.conn.execute(f"""
                INSERT INTO {tbl}
                SELECT {sql["i_cols"]}, DATE '{date}', {valid_to}
                FROM _incoming i
                WHERE {self._not_in_covered()} AND {self._not_in_next()} AND {self._not_in_prev()}
            """)

        # Case 6: Close deletions and re-open from next synced date if needed
        if stats["deletion_count"] > 0:
            self._handle_deletions(date, tbl, meta, stats["deletions"])

    def _handle_deletions(
        self, date: str, tbl: str, meta: str, deletions: "pd.DataFrame"
    ) -> None:
        """Close deleted records and re-open from next synced date if applicable."""
        sm_keys = ", ".join(f"sm.{k}" for k in self.keys)
        all_keys = ", ".join(self.keys)

        self.conn.execute(f"""
            UPDATE {tbl} sm SET valid_to = DATE '{date}'
            WHERE sm.valid_from <= DATE '{date}'
              AND (sm.valid_to > DATE '{date}' OR sm.valid_to IS NULL)
              AND ({sm_keys}) NOT IN (SELECT {all_keys} FROM _incoming)
        """)

        self.conn.register("_deletions", deletions)
        key_match = " AND ".join(f"s.{k} = d.{k}" for k in self.keys)
        d_cols = ", ".join(f"d.{c}" for c in self.all_cols)
        self.conn.execute(f"""
            INSERT INTO {tbl}
            SELECT {d_cols}, sm.as_of_date, d.valid_to
            FROM _deletions d
            JOIN {meta} sm ON sm.as_of_date > DATE '{date}'
                AND (d.valid_to IS NULL OR sm.as_of_date < d.valid_to)
            WHERE NOT EXISTS (
                SELECT 1 FROM {tbl} s WHERE {key_match} AND s.valid_from = sm.as_of_date
            )
            AND sm.as_of_date = (
                SELECT MIN(sm2.as_of_date) FROM {meta} sm2
                WHERE sm2.as_of_date > DATE '{date}'
                  AND (d.valid_to IS NULL OR sm2.as_of_date < d.valid_to)
            )
        """)
        self.conn.unregister("_deletions")

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
