from pathlib import Path

import pandas as pd

from .config import get_snowflake_config
from .snowflake_loader import get_connection, load_csv
from .validators import (
    validate_not_null,
    validate_required_columns,
    validate_unique,
)


BASE_DIR = Path(__file__).resolve().parents[2]


SOURCE_CONFIG = {
    "customer": {
        "file": BASE_DIR / "data/customers/customers.csv",
        "table": "CUSTOMER_RAW",
        "columns": [
            "CUSTOMER_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "EMAIL",
            "PHONE",
            "DATE_OF_BIRTH",
            "CITY",
            "STATE",
            "CUSTOMER_SEGMENT",
            "CUSTOMER_STATUS",
            "CREATED_AT",
            "UPDATED_AT",
        ],
        "required": [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "date_of_birth",
            "city",
            "state",
            "customer_segment",
            "customer_status",
            "created_at",
            "updated_at",
        ],
        "key": "customer_id",
    },
    "account": {
        "file": BASE_DIR / "data/accounts/accounts.csv",
        "table": "ACCOUNT_RAW",
        "columns": [
            "ACCOUNT_ID",
            "CUSTOMER_ID",
            "ACCOUNT_TYPE",
            "ACCOUNT_STATUS",
            "OPENING_DATE",
            "BALANCE",
            "CURRENCY",
            "UPDATED_AT",
        ],
        "required": [
            "account_id",
            "customer_id",
            "account_type",
            "account_status",
            "opening_date",
            "balance",
            "currency",
            "updated_at",
        ],
        "key": "account_id",
    },
    "transaction": {
        "file": BASE_DIR / "data/transactions/transactions.csv",
        "table": "TRANSACTION_RAW",
        "columns": [
            "TRANSACTION_ID",
            "ACCOUNT_ID",
            "CUSTOMER_ID",
            "TRANSACTION_TIMESTAMP",
            "TRANSACTION_TYPE",
            "AMOUNT",
            "CURRENCY",
            "MERCHANT",
            "TRANSACTION_STATUS",
        ],
        "required": [
            "transaction_id",
            "account_id",
            "customer_id",
            "transaction_timestamp",
            "transaction_type",
            "amount",
            "currency",
            "merchant",
            "transaction_status",
        ],
        "key": "transaction_id",
    },
    "support": {
        "file": BASE_DIR / "data/support/support.csv",
        "table": "SUPPORT_TICKET_RAW",
        "columns": [
            "TICKET_ID",
            "CUSTOMER_ID",
            "CREATED_AT",
            "CATEGORY",
            "PRIORITY",
            "STATUS",
            "SUBJECT",
            "DESCRIPTION",
        ],
        "required": [
            "ticket_id",
            "customer_id",
            "created_at",
            "category",
            "priority",
            "status",
            "subject",
            "description",
        ],
        "key": "ticket_id",
    },
    "product": {
        "file": BASE_DIR / "data/products/products.csv",
        "table": "PRODUCT_RAW",
        "columns": [
            "PRODUCT_ID",
            "PRODUCT_NAME",
            "PRODUCT_TYPE",
            "PRODUCT_CATEGORY",
            "ANNUAL_FEE",
            "INTEREST_RATE",
            "STATUS",
        ],
        "required": [
            "product_id",
            "product_name",
            "product_type",
            "product_category",
            "annual_fee",
            "interest_rate",
            "status",
        ],
        "key": "product_id",
    },
}

def validate_source(df: pd.DataFrame, config: dict) -> None:
    validate_required_columns(df, config["required"])
    validate_not_null(df, config["key"])
    validate_unique(df, config["key"])


def ingest(source_name: str) -> None:

    config = SOURCE_CONFIG[source_name]
    file_path = config["file"]

    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {file_path}"
        )

    df = pd.read_csv(file_path)

    print(f"\nSource: {source_name}")
    print(f"File: {file_path}")
    print(f"Rows: {len(df)}")

    validate_source(df, config)

    snowflake_config = get_snowflake_config()
    connection = get_connection(snowflake_config)

    try:
        rows_loaded = load_csv(
            connection=connection,
            file_path=file_path,
            table_name=config["table"],
            columns=config["columns"],
        )

        print(
            f"Loaded {rows_loaded} rows into "
            f"{snowflake_config['SNOWFLAKE_DATABASE']}"
            f".BRONZE.{config['table']}"
        )

    finally:
        connection.close()


def main():

    for source_name in SOURCE_CONFIG:
        ingest(source_name)

    print("\nBronze ingestion completed successfully.")


if __name__ == "__main__":
    main()