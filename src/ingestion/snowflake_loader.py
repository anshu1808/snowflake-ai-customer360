from pathlib import Path

import snowflake.connector


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


def load_csv(
    connection,
    file_path: Path,
    table_name: str,
    columns: list[str],
) -> int:

    file_path = file_path.resolve()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE OR REPLACE TEMPORARY STAGE INGESTION_STAGE
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                NULL_IF = ('', 'NULL')
            )
            """
        )

        cursor.execute(
            f"""
            PUT 'file://{file_path.as_posix()}'
            @INGESTION_STAGE
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE
            """
        )

        column_list = ", ".join(columns)

        cursor.execute(
            f"""
            COPY INTO {table_name} ({column_list})
            FROM @INGESTION_STAGE
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

        connection.commit()
        return rows_loaded

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()