"""Python to PostgreSQL type mapping utilities."""

from typing import Any

# Mapping of Python types to PostgreSQL types
PYTHON_TO_POSTGRES: dict[type, str] = {
    str: "TEXT",
    int: "BIGINT",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    list: "JSONB",
    dict: "JSONB",
    type(None): "TEXT",
}


def infer_postgres_type(value: Any) -> str:
    """
    Infer the PostgreSQL type from a Python value.

    Args:
        value: The Python value to analyze.

    Returns:
        The corresponding PostgreSQL type as a string.
    """
    if value is None:
        return "TEXT"

    value_type = type(value)

    # Direct type match
    if value_type in PYTHON_TO_POSTGRES:
        return PYTHON_TO_POSTGRES[value_type]

    # Handle nested structures as JSONB
    if isinstance(value, (list, dict)):
        return "JSONB"

    # Fallback to TEXT for unknown types
    return "TEXT"


def infer_column_types(records: list[dict[str, Any]]) -> dict[str, str]:
    """
    Infer PostgreSQL column types from a list of records.

    Analyzes all records to determine the most appropriate type for each column,
    handling cases where values might be None in some records.

    Args:
        records: List of dictionaries representing data records.

    Returns:
        Dictionary mapping column names to PostgreSQL types.
    """
    if not records:
        return {}

    column_types: dict[str, str] = {}

    # Analyze all records to infer types
    for record in records:
        for key, value in record.items():
            if key not in column_types:
                # First non-None value determines the type
                if value is not None:
                    column_types[key] = infer_postgres_type(value)
            elif column_types[key] == "TEXT" and value is not None:
                # Update TEXT to a more specific type if we find a non-None value
                inferred = infer_postgres_type(value)
                if inferred != "TEXT":
                    column_types[key] = inferred

    # Ensure all columns have a type (default to TEXT if all values were None)
    all_columns = set()
    for record in records:
        all_columns.update(record.keys())

    for col in all_columns:
        if col not in column_types:
            column_types[col] = "TEXT"

    return column_types
