from unittest.mock import MagicMock
from pathlib import Path
import pytest

from src.ingestion.snowflake_loader import (
    calculate_file_hash,
    file_already_loaded,
    load_csv,
)


def test_calculate_file_hash(tmp_path: Path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("col1,col2\n1,2\n", encoding="utf-8")
    hash1 = calculate_file_hash(test_file)
    assert len(hash1) == 64
    assert hash1 == calculate_file_hash(test_file)


def test_file_already_loaded_true():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [1]

    result = file_already_loaded(
        mock_cursor, "customer", "customers.csv", "abc123hash"
    )
    assert result is True
    assert mock_cursor.execute.called


def test_file_already_loaded_false():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [0]

    result = file_already_loaded(
        mock_cursor, "customer", "customers.csv", "abc123hash"
    )
    assert result is False


def test_load_csv_skips_when_already_loaded(tmp_path: Path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("id,name\n1,test\n", encoding="utf-8")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [1]  # already loaded
    mock_conn.cursor.return_value = mock_cursor

    rows = load_csv(
        connection=mock_conn,
        file_path=test_file,
        table_name="TEST_RAW",
        columns=["ID", "NAME"],
        source_name="test_src",
    )
    assert rows == 0


def test_load_csv_success(tmp_path: Path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("id,name\n1,test\n", encoding="utf-8")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [0]  # not loaded yet
    # Snowflake copy result format: [file, status, rows_parsed, rows_loaded, ...]
    mock_cursor.fetchall.return_value = [
        ("test.csv.gz", "LOADED", 1, 1, 0, 0, None, None, None, None)
    ]
    mock_conn.cursor.return_value = mock_cursor

    rows = load_csv(
        connection=mock_conn,
        file_path=test_file,
        table_name="TEST_RAW",
        columns=["ID", "NAME"],
        source_name="test_src",
    )
    assert rows == 1
    assert mock_conn.commit.called
