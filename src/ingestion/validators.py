import pandas as pd


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: list[str],
) -> None:
    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {', '.join(missing)}"
        )


def validate_not_null(
    df: pd.DataFrame,
    column: str,
) -> None:
    if df[column].isna().any():
        raise ValueError(
            f"Column '{column}' contains NULL values."
        )


def validate_unique(
    df: pd.DataFrame,
    column: str,
) -> None:
    if df[column].duplicated().any():
        raise ValueError(
            f"Column '{column}' contains duplicate values."
        )