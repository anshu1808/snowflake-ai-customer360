from pathlib import Path

import pandas as pd

from .config import get_snowflake_config
from .snowflake_loader import (
    get_connection,
    load_customer_csv,
)
from .validators import (
    validate_not_null,
    validate_required_columns,
    validate_unique,
)


REQUIRED_COLUMNS = [
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
]


def main():

    file_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "customers"
        / "customers.csv"
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"Customer source file not found: {file_path}"
        )

    # Read source
    df = pd.read_csv(file_path)

    print(f"Source file: {file_path}")
    print(f"Source rows: {len(df)}")

    # Schema validation
    validate_required_columns(
        df,
        REQUIRED_COLUMNS,
    )

    # Data quality validation
    validate_not_null(
        df,
        "customer_id",
    )

    validate_unique(
        df,
        "customer_id",
    )

    # Validate date columns
    df["date_of_birth"] = pd.to_datetime(
        df["date_of_birth"],
        errors="raise",
    )

    df["created_at"] = pd.to_datetime(
        df["created_at"],
        errors="raise",
    )

    df["updated_at"] = pd.to_datetime(
        df["updated_at"],
        errors="raise",
    )

    # Snowflake connection
    config = get_snowflake_config()

    connection = get_connection(config)

    try:

        rows_loaded = load_customer_csv(
            connection=connection,
            file_path=file_path,
        )

        print(
            "Customer ingestion completed successfully."
        )

        print(
            f"Rows loaded: {rows_loaded}"
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()