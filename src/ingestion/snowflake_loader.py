from pathlib import Path
import hashlib

import snowflake.connector


BRONZE_STAGE = "FINAI_BRONZE_INGEST_STAGE"
INGESTION_LOG = "INGESTION_FILE_LOG"


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

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {INGESTION_LOG}
        WHERE SOURCE_NAME = %s
          AND FILE_NAME = %s
          AND FILE_HASH = %s
          AND LOAD_STATUS = 'SUCCESS'
        """,
        (source_name, file_name, file_hash),
    )

    return cursor.fetchone()[0] > 0


def load_csv(
    connection,
    file_path: Path,
    table_name: str,
    columns: list[str],
    source_name: str,
) -> int:

    file_path = file_path.resolve()
    file_name = file_path.name
    file_size = file_path.stat().st_size
    file_hash = calculate_file_hash(file_path)

    cursor = connection.cursor()

    try:

        # ---------------------------------------------------------
        # Check whether this exact file version was already loaded
        # ---------------------------------------------------------

        if file_already_loaded(
            cursor,
            source_name,
            file_name,
            file_hash,
        ):
            print(
                f"SKIPPED: {source_name} / {file_name} "
                f"has already been loaded."
            )
            return 0

        # ---------------------------------------------------------
        # Create persistent Bronze ingestion stage
        # ---------------------------------------------------------

        cursor.execute(
            f"""
            CREATE STAGE IF NOT EXISTS {BRONZE_STAGE}
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                NULL_IF = ('', 'NULL')
            )
            """
        )

        # ---------------------------------------------------------
        # Upload file to per-source stage path
        # ---------------------------------------------------------

        cursor.execute(
            f"""
            PUT 'file://{file_path.as_posix()}'
            @{BRONZE_STAGE}/{source_name}/
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE
            """
        )

        column_list = ", ".join(columns)

        # ---------------------------------------------------------
        # Load Bronze with explicit stage subpath and filename
        # ---------------------------------------------------------

        cursor.execute(
            f"""
            COPY INTO {table_name} ({column_list})
            FROM @{BRONZE_STAGE}/{source_name}/
            FILES = ('{file_name}.gz')
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                NULL_IF = ('', 'NULL')
            )
            ON_ERROR = 'ABORT_STATEMENT'
            """
        )

        results = cursor.fetchall()

        rows_loaded = sum(
            int(row[3])
            for row in results
            if len(row) > 3
            and str(row[1]).upper() == "LOADED"
        )

        # ---------------------------------------------------------
        # Record successful load
        # ---------------------------------------------------------

        cursor.execute(
            f"""
            INSERT INTO {INGESTION_LOG}
            (
                SOURCE_NAME,
                FILE_NAME,
                FILE_SIZE,
                FILE_HASH,
                ROWS_LOADED,
                LOAD_STATUS
            )
            VALUES (%s, %s, %s, %s, %s, 'SUCCESS')
            """,
            (
                source_name,
                file_name,
                file_size,
                file_hash,
                rows_loaded,
            ),
        )

        connection.commit()

        return rows_loaded

    except Exception:
        try:
            connection.rollback()
            cursor.execute(
                f"""
                INSERT INTO {INGESTION_LOG}
                (
                    SOURCE_NAME,
                    FILE_NAME,
                    FILE_SIZE,
                    FILE_HASH,
                    ROWS_LOADED,
                    LOAD_STATUS
                )
                VALUES (%s, %s, %s, %s, 0, 'FAILED')
                """,
                (
                    source_name,
                    file_name,
                    file_size,
                    file_hash,
                ),
            )
            connection.commit()
        except Exception:
            pass

        raise

    finally:

        cursor.close()