"""Tests for all sync cases documented in SYNC_LOGIC.md."""

import pytest

from scduck import SCDTable

from .conftest import make_df


class TestCase1CoveredSameData:
    """Case 1: Record covered by existing SCD, data identical - no change."""

    def test_unchanged_record_no_new_rows(self, scd_table):
        """Unchanged records don't create new SCD rows."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])

        scd_table.sync("2025-01-01", df)
        result = scd_table.sync("2025-01-02", df)

        assert result.rows_unchanged == 1
        assert result.rows_new == 0
        assert result.rows_changed == 0

        # Still only 1 SCD record
        assert scd_table.get_record_count() == 1

    def test_multiple_unchanged_days(self, scd_table):
        """Multiple days of unchanged data = single SCD record."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])

        for day in range(1, 11):
            scd_table.sync(f"2025-01-{day:02d}", df)

        # Still just 1 record spanning all dates
        assert scd_table.get_record_count() == 1

        # All dates return same snapshot
        for day in range(1, 11):
            snap = scd_table.get_data(f"2025-01-{day:02d}")
            assert snap.num_rows == 1


class TestCase2CoveredDifferentData:
    """Case 2: Record covered, data differs - close old, insert new."""

    def test_value_change_creates_two_records(self, scd_table):
        """Value change creates a new SCD record."""
        df1 = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df2 = make_df([{"id": "A", "name": "Widget", "price": "12.99"}])

        scd_table.sync("2025-01-01", df1)
        result = scd_table.sync("2025-01-05", df2)

        assert result.rows_changed == 1
        assert scd_table.get_record_count() == 2

        # Jan 1 shows old price
        snap1 = scd_table.get_data("2025-01-01")
        assert snap1.column("price").to_pylist() == ["9.99"]

        # Jan 5 shows new price
        snap5 = scd_table.get_data("2025-01-05")
        assert snap5.column("price").to_pylist() == ["12.99"]

    def test_name_change(self, scd_table):
        """Name change is detected as different data."""
        df1 = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df2 = make_df([{"id": "A", "name": "Widget Pro", "price": "9.99"}])

        scd_table.sync("2025-01-01", df1)
        result = scd_table.sync("2025-01-02", df2)

        assert result.rows_changed == 1

        snap1 = scd_table.get_data("2025-01-01")
        assert snap1.column("name").to_pylist() == ["Widget"]

        snap2 = scd_table.get_data("2025-01-02")
        assert snap2.column("name").to_pylist() == ["Widget Pro"]


class TestCase3NotCoveredHasNext:
    """Case 3: Not covered, has future record."""

    def test_case3a_same_data_extends_back(self, scd_table):
        """Same data as next record - extend backwards."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])

        # Sync future date first
        scd_table.sync("2025-01-10", df)
        assert scd_table.get_record_count() == 1

        # Backfill with same data - should extend existing record
        result = scd_table.sync("2025-01-01", df)

        assert result.rows_extended_back == 1
        assert result.rows_new == 0

        # Still only 1 record
        assert scd_table.get_record_count() == 1

        # Both dates return data
        assert scd_table.get_data("2025-01-01").num_rows == 1
        assert scd_table.get_data("2025-01-10").num_rows == 1

    def test_case3b_different_data_inserts_before(self, scd_table):
        """Different data than next record - insert new with valid_to = next.valid_from."""
        df_old = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df_new = make_df([{"id": "A", "name": "Widget", "price": "12.99"}])

        # Sync future date with new price
        scd_table.sync("2025-01-10", df_new)

        # Backfill with old price
        result = scd_table.sync("2025-01-01", df_old)

        assert result.rows_changed == 1  # counted as changed because insert before next
        assert scd_table.get_record_count() == 2

        # Jan 1 shows old price
        snap1 = scd_table.get_data("2025-01-01")
        assert snap1.column("price").to_pylist() == ["9.99"]

        # Jan 10 shows new price
        snap10 = scd_table.get_data("2025-01-10")
        assert snap10.column("price").to_pylist() == ["12.99"]


class TestCase4Reappearance:
    """Case 4: Reappearance - not covered, no next, has prev record."""

    def test_reappearance_after_gap(self, scd_table):
        """Record reappears after being absent."""
        df_with_a = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df_empty = make_df([{"id": "B", "name": "Other", "price": "1.99"}])

        # Day 1: A present
        scd_table.sync("2025-01-01", df_with_a)

        # Day 5: A absent (deleted)
        scd_table.sync("2025-01-05", df_empty)

        # Day 10: A reappears
        result = scd_table.sync("2025-01-10", df_with_a)

        assert result.rows_reappeared == 1

        # Day 1: A present
        assert scd_table.get_data("2025-01-01").num_rows == 1

        # Day 5: only B present
        snap5 = scd_table.get_data("2025-01-05")
        assert snap5.num_rows == 1
        assert snap5.column("id").to_pylist() == ["B"]

        # Day 10: A present again
        snap10 = scd_table.get_data("2025-01-10")
        ids = set(snap10.column("id").to_pylist())
        assert "A" in ids


class TestCase5NewRecord:
    """Case 5: New record - no existing SCD records for this key."""

    def test_new_record_insert(self, scd_table):
        """New record is inserted with valid_from = date."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])

        result = scd_table.sync("2025-01-01", df)

        assert result.rows_new == 1
        assert scd_table.get_record_count() == 1

    def test_new_record_with_future_synced(self, scd_table):
        """New record when future dates already synced sets valid_to correctly."""
        df_b = make_df([{"id": "B", "name": "Gadget", "price": "4.99"}])
        df_ab = make_df([
            {"id": "A", "name": "Widget", "price": "9.99"},
            {"id": "B", "name": "Gadget", "price": "4.99"},
        ])

        # Day 10: only B
        scd_table.sync("2025-01-10", df_b)

        # Day 1: A and B (A is new, won't exist at day 10)
        result = scd_table.sync("2025-01-01", df_ab)

        assert result.rows_new == 1  # A is new

        # Day 1: both present
        snap1 = scd_table.get_data("2025-01-01")
        assert snap1.num_rows == 2

        # Day 10: only B (A was not present when day 10 was synced)
        snap10 = scd_table.get_data("2025-01-10")
        assert snap10.num_rows == 1
        assert snap10.column("id").to_pylist() == ["B"]


class TestCase6Deletion:
    """Case 6: Record not in incoming snapshot, has covering record - deletion."""

    def test_deletion_closes_record(self, scd_table):
        """Deleted record has valid_to set to sync date."""
        df_with_a = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df_without_a = make_df([{"id": "B", "name": "Other", "price": "1.99"}])

        scd_table.sync("2025-01-01", df_with_a)
        result = scd_table.sync("2025-01-05", df_without_a)

        assert result.rows_deleted == 1

        # Day 1: A present
        snap1 = scd_table.get_data("2025-01-01")
        assert snap1.column("id").to_pylist() == ["A"]

        # Day 5: A gone, B present
        snap5 = scd_table.get_data("2025-01-05")
        assert snap5.column("id").to_pylist() == ["B"]

    def test_deletion_reopens_for_future_synced_dates(self, scd_table):
        """Deletion re-opens record from next synced date if covered."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df_empty = make_df([{"id": "B", "name": "Other", "price": "1.99"}])

        # Sync out of order: day 17, day 1, day 5, day 3
        scd_table.sync("2025-01-17", df)  # A present
        scd_table.sync("2025-01-01", df)  # A present (extends back)
        scd_table.sync("2025-01-05", df)  # A present (unchanged)
        scd_table.sync("2025-01-03", df_empty)  # A absent - creates gap

        # Result should be: (Jan 1, Jan 3), (Jan 5, NULL)
        snap1 = scd_table.get_data("2025-01-01")
        assert snap1.column("id").to_pylist() == ["A"]

        snap3 = scd_table.get_data("2025-01-03")
        assert "A" not in snap3.column("id").to_pylist()

        snap5 = scd_table.get_data("2025-01-05")
        assert "A" in snap5.column("id").to_pylist()

        snap17 = scd_table.get_data("2025-01-17")
        assert "A" in snap17.column("id").to_pylist()


class TestCase7NoCoveringRecord:
    """Case 7: Record not in incoming, no covering record - no change."""

    def test_no_covering_no_change(self, scd_table):
        """Record never existed - nothing to delete."""
        df_a = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        df_b = make_df([{"id": "B", "name": "Gadget", "price": "4.99"}])

        scd_table.sync("2025-01-01", df_a)  # Only A
        result = scd_table.sync("2025-01-05", df_b)  # Only B (A not deleted, just not present)

        # A was never at day 5, B is new
        assert result.rows_new == 1
        assert result.rows_deleted == 1  # A is deleted from Jan 1 coverage


class TestOutOfOrderSync:
    """Test various out-of-order sync scenarios."""

    def test_backfill_same_data(self, scd_table):
        """Backfilling with same data extends record backwards."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])

        scd_table.sync("2025-01-10", df)
        scd_table.sync("2025-01-05", df)
        scd_table.sync("2025-01-01", df)

        # All dates show same data
        for day in ["2025-01-01", "2025-01-05", "2025-01-10"]:
            snap = scd_table.get_data(day)
            assert snap.num_rows == 1
            assert snap.column("name").to_pylist() == ["Widget"]

        # Only 1 SCD record
        assert scd_table.get_record_count() == 1

    def test_complex_out_of_order_scenario(self, scd_table):
        """Complex scenario from SYNC_LOGIC.md documentation."""
        df_x = make_df([{"id": "X", "name": "Item", "price": "10"}])
        df_empty = make_df([{"id": "Y", "name": "Other", "price": "5"}])

        # Sync Dec 17: X present
        scd_table.sync("2025-12-17", df_x)

        # Sync Dec 1: X present, same data -> extends backwards
        scd_table.sync("2025-12-01", df_x)

        # Sync Dec 5: X present -> unchanged
        scd_table.sync("2025-12-05", df_x)

        # Sync Dec 3: X NOT present -> creates gap
        scd_table.sync("2025-12-03", df_empty)

        # Result: X has (Dec 1, Dec 3) and (Dec 5, NULL)
        snap_dec1 = scd_table.get_data("2025-12-01")
        assert "X" in snap_dec1.column("id").to_pylist()

        snap_dec3 = scd_table.get_data("2025-12-03")
        assert "X" not in snap_dec3.column("id").to_pylist()

        snap_dec5 = scd_table.get_data("2025-12-05")
        assert "X" in snap_dec5.column("id").to_pylist()

        snap_dec17 = scd_table.get_data("2025-12-17")
        assert "X" in snap_dec17.column("id").to_pylist()


class TestNullHandling:
    """Test NULL value handling in data."""

    def test_null_values_tracked(self, scd_table):
        """NULL values are tracked correctly."""
        df1 = make_df([{"id": "A", "name": "Widget", "price": None}])
        df2 = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])

        scd_table.sync("2025-01-01", df1)
        result = scd_table.sync("2025-01-02", df2)

        # NULL to value is a change
        assert result.rows_changed == 1

        snap1 = scd_table.get_data("2025-01-01")
        assert snap1.column("price").to_pylist() == [None]

        snap2 = scd_table.get_data("2025-01-02")
        assert snap2.column("price").to_pylist() == ["9.99"]

    def test_null_to_null_unchanged(self, scd_table):
        """NULL to NULL is considered unchanged."""
        df = make_df([{"id": "A", "name": "Widget", "price": None}])

        scd_table.sync("2025-01-01", df)
        result = scd_table.sync("2025-01-02", df)

        assert result.rows_unchanged == 1
        assert scd_table.get_record_count() == 1
