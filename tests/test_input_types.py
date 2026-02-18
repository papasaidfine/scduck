"""Tests for different input DataFrame types."""

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from scduck import SCDTable

from .conftest import make_arrow_table, make_df, make_polars_df


class TestPandasInput:
    """Test pandas DataFrame input."""

    def test_pandas_sync(self, scd_table):
        """Pandas DataFrame syncs correctly."""
        df = pd.DataFrame([
            {"id": "A", "name": "Widget", "price": "9.99"},
            {"id": "B", "name": "Gadget", "price": "4.99"},
        ])

        result = scd_table.sync("2025-01-01", df)

        assert result.rows_total == 2
        assert result.rows_new == 2

        snapshot = scd_table.get_data("2025-01-01")
        assert snapshot.num_rows == 2

    def test_pandas_with_index(self, scd_table):
        """Pandas DataFrame with custom index works."""
        df = pd.DataFrame(
            [
                {"id": "A", "name": "Widget", "price": "9.99"},
            ],
            index=["custom_index"],
        )

        result = scd_table.sync("2025-01-01", df)
        assert result.rows_new == 1


class TestPolarsInput:
    """Test polars DataFrame input."""

    def test_polars_sync(self, scd_table):
        """Polars DataFrame syncs correctly."""
        df = pl.DataFrame([
            {"id": "A", "name": "Widget", "price": "9.99"},
            {"id": "B", "name": "Gadget", "price": "4.99"},
        ])

        result = scd_table.sync("2025-01-01", df)

        assert result.rows_total == 2
        assert result.rows_new == 2

        snapshot = scd_table.get_data("2025-01-01")
        assert snapshot.num_rows == 2

    def test_polars_lazy_not_supported(self, scd_table):
        """Polars LazyFrame should fail gracefully."""
        lf = pl.LazyFrame([{"id": "A", "name": "Widget", "price": "9.99"}])

        with pytest.raises(TypeError):
            scd_table.sync("2025-01-01", lf)


class TestPyArrowInput:
    """Test PyArrow Table input."""

    def test_pyarrow_sync(self, scd_table):
        """PyArrow Table syncs correctly."""
        table = pa.Table.from_pylist([
            {"id": "A", "name": "Widget", "price": "9.99"},
            {"id": "B", "name": "Gadget", "price": "4.99"},
        ])

        result = scd_table.sync("2025-01-01", table)

        assert result.rows_total == 2
        assert result.rows_new == 2

        snapshot = scd_table.get_data("2025-01-01")
        assert snapshot.num_rows == 2

    def test_pyarrow_output_type(self, scd_table):
        """get_data returns PyArrow Table."""
        df = make_df([{"id": "A", "name": "Widget", "price": "9.99"}])
        scd_table.sync("2025-01-01", df)

        snapshot = scd_table.get_data("2025-01-01")
        assert isinstance(snapshot, pa.Table)


class TestColumnNormalization:
    """Test column name normalization."""

    def test_case_insensitive_columns(self, tmp_db):
        """Column names are matched case-insensitively."""
        with SCDTable(
            tmp_db,
            table="test",
            keys=["product_id"],
            values=["name", "price"],
        ) as db:
            # Input has different casing
            df = pd.DataFrame([
                {"PRODUCT_ID": "A", "Name": "Widget", "PRICE": "9.99"},
            ])

            result = db.sync("2025-01-01", df)
            assert result.rows_new == 1

            snapshot = db.get_data("2025-01-01")
            assert snapshot.num_rows == 1

    def test_underscore_dash_normalized(self, tmp_db):
        """Underscores and dashes are normalized in column names."""
        with SCDTable(
            tmp_db,
            table="test",
            keys=["product_id"],
            values=["sub_industry"],
        ) as db:
            # Input uses dashes instead of underscores
            df = pd.DataFrame([
                {"product-id": "A", "sub-industry": "Tech"},
            ])

            result = db.sync("2025-01-01", df)
            assert result.rows_new == 1


class TestMixedInputTypes:
    """Test mixing different input types across syncs."""

    def test_pandas_then_polars(self, scd_table):
        """Can sync pandas then polars."""
        df_pandas = pd.DataFrame([{"id": "A", "name": "Widget", "price": "9.99"}])
        df_polars = pl.DataFrame([{"id": "A", "name": "Widget", "price": "12.99"}])

        scd_table.sync("2025-01-01", df_pandas)
        result = scd_table.sync("2025-01-02", df_polars)

        assert result.rows_changed == 1

    def test_polars_then_pyarrow(self, scd_table):
        """Can sync polars then pyarrow."""
        df_polars = pl.DataFrame([{"id": "A", "name": "Widget", "price": "9.99"}])
        table_arrow = pa.Table.from_pylist([{"id": "A", "name": "Widget", "price": "9.99"}])

        scd_table.sync("2025-01-01", df_polars)
        result = scd_table.sync("2025-01-02", table_arrow)

        assert result.rows_unchanged == 1

    def test_all_three_types(self, scd_table):
        """Can mix all three input types."""
        df_pandas = pd.DataFrame([{"id": "A", "name": "V1", "price": "1"}])
        df_polars = pl.DataFrame([{"id": "A", "name": "V2", "price": "2"}])
        table_arrow = pa.Table.from_pylist([{"id": "A", "name": "V3", "price": "3"}])

        scd_table.sync("2025-01-01", df_pandas)
        scd_table.sync("2025-01-02", df_polars)
        scd_table.sync("2025-01-03", table_arrow)

        # Should have 3 SCD records
        assert scd_table.get_record_count() == 3

        # Each date shows correct version
        assert scd_table.get_data("2025-01-01").column("name").to_pylist() == ["V1"]
        assert scd_table.get_data("2025-01-02").column("name").to_pylist() == ["V2"]
        assert scd_table.get_data("2025-01-03").column("name").to_pylist() == ["V3"]


class TestUnsupportedTypes:
    """Test that unsupported types fail gracefully."""

    def test_dict_not_supported(self, scd_table):
        """Raw dict is not supported."""
        data = [{"id": "A", "name": "Widget", "price": "9.99"}]

        with pytest.raises(TypeError):
            scd_table.sync("2025-01-01", data)

    def test_list_not_supported(self, scd_table):
        """Raw list is not supported."""
        data = [["A", "Widget", "9.99"]]

        with pytest.raises(TypeError):
            scd_table.sync("2025-01-01", data)
