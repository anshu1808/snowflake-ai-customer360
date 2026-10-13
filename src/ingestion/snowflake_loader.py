"""Bronze loader: PUT a local CSV to an internal stage, COPY INTO the RAW table,
and record the result in INGESTION_FILE_LOG.

Behaviour guarantees
--------------------
* A file is only logged as SUCCESS if at least one row was actually loaded.
* A COPY that processes 0 files / loads 0 rows raises, so it can never poison
  the log and cause permanent "SKIPPED" results.
* Failures are logged as FAILED and the original error is always re-raised.
* ``force=True`` bypasses the file-hash log and Snowflake's COPY load
  metadata (use after truncating a table).
"""

import hashlib
import logging
from pathlib import Path

import snowflake.connector

logger = logging.getLogger(__name__)

BRONZE_STAGE = "FINAI_BRONZE_INGEST_STAGE"
INGESTION_LOG = "INGESTION_FILE_LOG"

CSV_FORMAT_SQL = """(
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                NULL_IF = ('', 'NULL')
            )"""


class NoRowsLoadedError(RuntimeError):
    """COPY INTO finished without loading any rows."""


def get_connection(config: dict):
    return snowflake.connector.connect(
        account=config["SNOWFLAKE_ACCOUNT"],
        user=config["SNOWFLAKE_USER"],
        password=config["SNOWFLAKE_PASSWORD"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema=config["SNOWFLAKE_SCHEMA"],
        role=config["SNOWFLAKE_ROLE"],
    )


def calculate_file_hash(file_path: Path) -> str:
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            sha256.update(chunk)

    return sha256.hexdigest()


def file_already_loaded(
    cursor,
    source_name: str,
    file_name: str,
    file_hash: str,
) -> bool:
    """True only if this exact file version loaded at least one row before."""

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {INGESTION_LOG}
        WHERE SOURCE_NAME = %s
          AND FILE_NAME = %s
          AND FILE_HASH = %s
          AND LOAD_STATUS = 'SUCCESS'
          AND ROWS_LOADED > 0
        """,
        (source_name, file_name, file_hash),
    )

    return cursor.fetchone()[0] > 0


def _write_log(
    cursor,
    source_name: str,
    file_name: str,
    file_size: int,
    file_hash: str,
    rows_loaded: int,
    status: str,
) -> None:
    cursor.execute(
        f"""
        INSERT INTO {INGESTION_LOG}
        (SOURCE_NAME, FILE_NAME, FILE_SIZE, FILE_HASH, ROWS_LOADED, LOAD_STATUS)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source_name, file_name, file_size, file_hash, rows_loaded, status),
    )


def _count_loaded_rows(copy_results) -> int:
    """Sum ROWS_LOADED from COPY INTO output.

    Columns: file, status, rows_parsed, rows_loaded, ...
    When nothing is processed Snowflake returns a single one-column row
    ("Copy executed with 0 files processed."), which yields 0 here.
    """
    return sum(
        int(row[3])
        for row in copy_results
        if len(row) > 3 and str(row[1]).upper() == "LOADED"
    )


def load_csv(
    connection,
    file_path: Path,
    table_name: str,
    columns: list[str],
    source_name: str,
    force: bool = False,
) -> int:

    file_path = file_path.resolve()
    file_name = file_path.name
    file_size = file_path.stat().st_size
    file_hash = calculate_file_hash(file_path)

    cursor = connection.cursor()

    try:
        if not force and file_already_loaded(
            cursor, source_name, file_name, file_hash
        ):
            print(
                f"SKIPPED: {source_name} / {file_name} already loaded "
                f"(use force=True / --force to reload)."
            )
            return 0

        cursor.execute(
            f"""
            CREATE STAGE IF NOT EXISTS {BRONZE_STAGE}
            FILE_FORMAT = {CSV_FORMAT_SQL}
            """
        )

        # One sub-folder per source so COPY never picks up other sources' files.
        cursor.execute(
            f"""
            PUT 'file://{file_path.as_posix()}'
            @{BRONZE_STAGE}/{source_name}/
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE
            """
        )

        column_list = ", ".join(columns)
        force_clause = "FORCE = TRUE" if force else ""

        cursor.execute(
            f"""
            COPY INTO {table_name} ({column_list})
            FROM @{BRONZE_STAGE}/{source_name}/
            FILES = ('{file_name}.gz')
            FILE_FORMAT = {CSV_FORMAT_SQL}
            ON_ERROR = 'ABORT_STATEMENT'
            {force_clause}
            """
        )

        rows_loaded = _count_loaded_rows(cursor.fetchall())

        if rows_loaded == 0:
            raise NoRowsLoadedError(
                f"COPY INTO {table_name} loaded 0 rows from {file_name}. "
                "If the table was truncated, re-run with --force."
            )

        _write_log(
            cursor, source_name, file_name, file_size, file_hash,
            rows_loaded, "SUCCESS",
        )
        connection.commit()
        return rows_loaded

    except Exception:
        try:
            connection.rollback()
            _write_log(
                cursor, source_name, file_name, file_size, file_hash,
                0, "FAILED",
            )
            connection.commit()
        except Exception:
            logger.exception("Could not write FAILED row to %s", INGESTION_LOG)
        raise

    finally:
        cursor.close()
