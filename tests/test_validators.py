import pandas as pd
import pytest

from src.ingestion.validators import (
    validate_not_null,
    validate_required_columns,
    validate_unique,
)


def test_validate_required_columns_success():
    df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
    validate_required_columns(df, ["col_a", "col_b"])


def test_validate_required_columns_missing():
    df = pd.DataFrame({"col_a": [1, 2]})
    with pytest.raises(ValueError, match="Missing required columns: col_b"):
        validate_required_columns(df, ["col_a", "col_b"])


def test_validate_not_null_success():
    df = pd.DataFrame({"id": [1, 2, 3]})
    validate_not_null(df, "id")


def test_validate_not_null_failure():
    df = pd.DataFrame({"id": [1, None, 3]})
    with pytest.raises(ValueError, match="Column 'id' contains NULL values"):
        validate_not_null(df, "id")


def test_validate_unique_success():
    df = pd.DataFrame({"id": ["a", "b", "c"]})
    validate_unique(df, "id")


def test_validate_unique_failure():
    df = pd.DataFrame({"id": ["a", "b", "a"]})
    with pytest.raises(ValueError, match="Column 'id' contains duplicate values"):
        validate_unique(df, "id")
