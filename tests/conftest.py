"""Shared pytest fixtures for scduck tests."""

import tempfile
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from scduck import SCDTable


@pytest.fixture
def tmp_db(tmp_path):
    """Provide a temporary database path."""
    return tmp_path / "test.duckdb"


@pytest.fixture
def scd_table(tmp_db):
    """Provide an SCDTable instance with simple schema."""
    with SCDTable(
        tmp_db,
        table="items",
        keys=["id"],
        values=["name", "price"],
    ) as db:
        yield db


@pytest.fixture
def multi_key_table(tmp_db):
    """Provide an SCDTable with composite key."""
    with SCDTable(
        tmp_db,
        table="products",
        keys=["category", "product_id"],
        values=["name", "price"],
    ) as db:
        yield db


def make_df(data: list[dict]) -> pd.DataFrame:
    """Create a pandas DataFrame from list of dicts."""
    return pd.DataFrame(data)


def make_polars_df(data: list[dict]) -> pl.DataFrame:
    """Create a polars DataFrame from list of dicts."""
    return pl.DataFrame(data)


def make_arrow_table(data: list[dict]) -> pa.Table:
    """Create a pyarrow Table from list of dicts."""
    return pa.Table.from_pylist(data)
