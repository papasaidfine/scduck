"""
scduck - SCD Type 2 tables with DuckDB.

Track historical changes to slowly-changing data without storing redundant snapshots.
"""

from .table import DataFrameLike, SCDTable, SyncResult

__all__ = ["SCDTable", "SyncResult", "DataFrameLike"]
__version__ = "0.1.0"
