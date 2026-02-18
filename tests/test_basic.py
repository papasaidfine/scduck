"""Basic functionality tests for SCDTable."""

import pandas as pd
import pytest

from scduck import SCDTable, SyncResult

from .conftest import make_df


class TestBasicSync:
    """Test basic sync operations."""

    def test_sync_single_date(self, scd_table):
        """Sync a single date and verify data."""
        df = make_df([
            {"id": "A", "name": "Widget", "price": "9.99"},
            {"id": "B", "name": "Gadget", "price": "4.99"},
        ])

        result = scd_table.sync("2025-01-01", df)

        assert isinstance(result, SyncResult)
        assert result.date == "2025-01-01"
        assert result.rows_total == 2
        assert result.rows_new == 2
        assert result.rows_changed == 0
        assert result.rows_deleted == 0

    def test_get_data(self, scd_table):
        """Verify get_data returns correct snapshot."""
        df = make_df([
            {"id": "A", "name": "Widget", "price": "9.99"},
            {"id": "B", "name": "Gadget", "price": "4.99"},
        ])
        scd_table.sync("2025-01-01", df)

        snapshot = scd_table.get_data("2025-01-01")

        assert snapshot.num_rows == 2
        ids = set(snapshot.column("id").to_pylist())
        assert ids == {"A", "B"}

    def test_get_synced_dates(self, scd_table):
        """Verify get_synced_dates returns all synced dates."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])

        scd_table.sync("2025-01-01", df)
        scd_table.sync("2025-01-02", df)
        scd_table.sync("2025-01-05", df)

        dates = scd_table.get_synced_dates()
        assert dates == ["2025-01-01", "2025-01-02", "2025-01-05"]

    def test_get_record_count(self, scd_table):
        """Verify get_record_count returns total SCD records."""
        df1 = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df2 = make_df([{"id": "A", "name": "Widget", "price": "12.99"}])  # price changed

        scd_table.sync("2025-01-01", df1)
        scd_table.sync("2025-01-02", df2)

        # Should have 2 records: original closed, new one opened
        assert scd_table.get_record_count() == 2


class TestIdempotency:
    """Test idempotent re-sync behavior."""

    def test_resync_same_data_no_change(self, scd_table):
        """Re-syncing same data should be a no-op."""
        df = make_df([
            {"id": "A", "name": "Widget", "price": "9.99"},
        ])

        result1 = scd_table.sync("2025-01-01", df)
        result2 = scd_table.sync("2025-01-01", df)

        assert result1.rows_new == 1
        assert result2.rows_unchanged == 1
        assert result2.rows_new == 0
        assert result2.rows_changed == 0

        # Still only 1 record
        assert scd_table.get_record_count() == 1

    def test_resync_different_data_updates(self, scd_table):
        """Re-syncing different data should update."""
        df1 = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df2 = make_df([{"id": "A", "name": "Widget Pro", "price": "9.99"}])

        scd_table.sync("2025-01-01", df1)
        result = scd_table.sync("2025-01-01", df2)

        assert result.rows_changed == 1

        # Snapshot should show updated name
        snapshot = scd_table.get_data("2025-01-01")
        names = snapshot.column("name").to_pylist()
        assert names == ["Widget Pro"]


class TestContextManager:
    """Test context manager behavior."""

    def test_context_manager(self, tmp_db):
        """SCDTable works as context manager."""
        with SCDTable(tmp_db, "test", ["id"], ["value"]) as db:
            df = make_df([{"id": "A", "value": "1"}])
            db.sync("2025-01-01", df)
            assert db.get_record_count() == 1

        # Connection closed, can reopen
        with SCDTable(tmp_db, "test", ["id"], ["value"]) as db:
            assert db.get_record_count() == 1

    def test_multiple_tables_same_db(self, tmp_db):
        """Multiple tables can coexist in same database."""
        with SCDTable(tmp_db, "table1", ["id"], ["val"]) as db1:
            db1.sync("2025-01-01", make_df([{"id": "A", "val": "1"}]))

        with SCDTable(tmp_db, "table2", ["id"], ["val"]) as db2:
            db2.sync("2025-01-01", make_df([{"id": "X", "val": "2"}]))

        # Verify both tables exist and are independent
        with SCDTable(tmp_db, "table1", ["id"], ["val"]) as db1:
            assert db1.get_record_count() == 1
            snapshot = db1.get_data("2025-01-01")
            assert snapshot.column("id").to_pylist() == ["A"]

        with SCDTable(tmp_db, "table2", ["id"], ["val"]) as db2:
            assert db2.get_record_count() == 1
            snapshot = db2.get_data("2025-01-01")
            assert snapshot.column("id").to_pylist() == ["X"]


class TestMultipleKeys:
    """Test composite key behavior."""

    def test_composite_key_sync(self, multi_key_table):
        """Composite keys work correctly."""
        df = make_df([
            {"category": "electronics", "product_id": "001", "name": "Phone", "price": "999"},
            {"category": "electronics", "product_id": "002", "name": "Tablet", "price": "499"},
            {"category": "clothing", "product_id": "001", "name": "Shirt", "price": "29"},
        ])

        result = multi_key_table.sync("2025-01-01", df)

        assert result.rows_total == 3
        assert result.rows_new == 3

        snapshot = multi_key_table.get_data("2025-01-01")
        assert snapshot.num_rows == 3

    def test_composite_key_change(self, multi_key_table):
        """Changes detected correctly with composite keys."""
        df1 = make_df([
            {"category": "electronics", "product_id": "001", "name": "Phone", "price": "999"},
        ])
        df2 = make_df([
            {"category": "electronics", "product_id": "001", "name": "Phone", "price": "899"},  # price changed
        ])

        multi_key_table.sync("2025-01-01", df1)
        result = multi_key_table.sync("2025-01-02", df2)

        assert result.rows_changed == 1

        # Jan 1 should show old price
        snap1 = multi_key_table.get_data("2025-01-01")
        assert snap1.column("price").to_pylist() == ["999"]

        # Jan 2 should show new price
        snap2 = multi_key_table.get_data("2025-01-02")
        assert snap2.column("price").to_pylist() == ["899"]
