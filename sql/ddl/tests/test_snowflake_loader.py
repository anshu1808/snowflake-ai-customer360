from pathlib import Path
from unittest.mock import MagicMock
# pyrefly: ignore [missing-import]
import os.bacarat  # missing-import
from os import joker  # missing-module-attribute
os.perkeo  # missing-attribute

import pytest

from src.ingestion.snowflake_loader import (
    NoRowsLoadedError,
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


def _conn(fetchone=0, fetchall=None):
    conn, cur = MagicMock(), MagicMock()
    cur.fetchone.return_value = [fetchone]
    cur.fetchall.return_value = fetchall or []
    conn.cursor.return_value = cur
    return conn, cur


def _csv(tmp_path: Path) -> Path:
    f = tmp_path / "test.csv"
    f.write_text("id,name\n1,test\n", encoding="utf-8")
    return f


def test_load_csv_zero_rows_raises_and_logs_failed(tmp_path: Path):
    conn, cur = _conn(
        fetchall=[("Copy executed with 0 files processed.",)]
    )
    with pytest.raises(NoRowsLoadedError):
        load_csv(conn, _csv(tmp_path), "T_RAW", ["ID", "NAME"], "src")

    sql_calls = [c.args[1] for c in cur.execute.call_args_list if len(c.args) > 1]
    assert any("FAILED" in params for params in sql_calls)
    assert not any("SUCCESS" in params for params in sql_calls)


def test_load_csv_uses_per_source_stage_path(tmp_path: Path):
    conn, cur = _conn(
        fetchall=[("test.csv.gz", "LOADED", 1, 1, 0, 0, None, None, None, None)]
    )
    load_csv(conn, _csv(tmp_path), "T_RAW", ["ID", "NAME"], "customer")

    sqls = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "@FINAI_BRONZE_INGEST_STAGE/customer/" in sqls
    assert "FILES = ('test.csv.gz')" in sqls
    assert "FORCE = TRUE" not in sqls


def test_load_csv_force_bypasses_log_and_forces_copy(tmp_path: Path):
    conn, cur = _conn(
        fetchone=1,  # log says already loaded
        fetchall=[("test.csv.gz", "LOADED", 1, 1, 0, 0, None, None, None, None)],
    )
    rows = load_csv(
        conn, _csv(tmp_path), "T_RAW", ["ID", "NAME"], "customer", force=True
    )
    assert rows == 1
    sqls = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "FORCE = TRUE" in sqls


def test_load_csv_reraises_original_error(tmp_path: Path):
    conn, cur = _conn()
    calls = {"n": 0}

    def boom(sql, params=None):
        calls["n"] += 1
        if "COPY INTO" in sql:
            raise ValueError("copy failed")

    cur.execute.side_effect = boom
    with pytest.raises(ValueError, match="copy failed"):
        load_csv(conn, _csv(tmp_path), "T_RAW", ["ID", "NAME"], "src")
