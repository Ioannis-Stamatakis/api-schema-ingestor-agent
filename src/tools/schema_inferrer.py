"""Schema inference tool for generating PostgreSQL DDL from JSON data."""

import json
from typing import Any

from src.config import get_settings
from src.utils.type_mapper import infer_column_types


# Columns that are likely primary keys
PRIMARY_KEY_CANDIDATES = {"id", "uuid", "_id", "pk", "key"}


def infer_schema(
    data: list[dict[str, Any]] | dict[str, Any],
    table_name: str,
) -> dict[str, Any]:
    """
    Infer PostgreSQL schema from JSON data and generate DDL.

    Analyzes the structure of JSON data to determine column types,
    detect primary keys, and generate a CREATE TABLE statement.

    Args:
        data: The JSON data (list of objects or single object).
        table_name: The name for the PostgreSQL table.

    Returns:
        A dictionary containing:
        - ddl: The CREATE TABLE statement
        - columns: Dictionary mapping column names to types
        - primary_key: The detected primary key column (or None)
        - record_count: Number of records to insert
        - table_name: The sanitized table name
    """
    settings = get_settings()
    schema = settings.db_schema

    # Normalize data to list of records
    if isinstance(data, dict):
        records = [data]
    else:
        records = data

    if not records:
        return {
            "ddl": None,
            "columns": {},
            "primary_key": None,
            "record_count": 0,
            "table_name": table_name,
            "error": "No data to infer schema from",
        }

    # Infer column types from all records
    column_types = infer_column_types(records)

    # Detect primary key
    primary_key = _detect_primary_key(column_types)

    # Generate DDL
    ddl = _generate_ddl(table_name, schema, column_types, primary_key)

    return {
        "ddl": ddl,
        "columns": column_types,
        "primary_key": primary_key,
        "record_count": len(records),
        "table_name": table_name,
        "error": None,
    }


def _detect_primary_key(column_types: dict[str, str]) -> str | None:
    """
    Detect the primary key column from column names.

    Args:
        column_types: Dictionary of column names to types.

    Returns:
        The primary key column name, or None if not detected.
    """
    columns_lower = {col.lower(): col for col in column_types.keys()}

    # Check for common primary key names
    for candidate in PRIMARY_KEY_CANDIDATES:
        if candidate in columns_lower:
            return columns_lower[candidate]

    return None


def _generate_ddl(
    table_name: str,
    schema: str,
    column_types: dict[str, str],
    primary_key: str | None,
) -> str:
    """
    Generate CREATE TABLE DDL statement.

    Args:
        table_name: The table name.
        schema: The database schema.
        column_types: Dictionary mapping column names to PostgreSQL types.
        primary_key: The primary key column (or None).

    Returns:
        The CREATE TABLE SQL statement.
    """
    if not column_types:
        return ""

    # Build column definitions
    column_defs = []
    for col_name, col_type in column_types.items():
        # Quote column names to handle reserved words and special characters
        quoted_name = f'"{col_name}"'

        if col_name == primary_key:
            column_defs.append(f"    {quoted_name} {col_type} PRIMARY KEY")
        else:
            column_defs.append(f"    {quoted_name} {col_type}")

    columns_sql = ",\n".join(column_defs)

    # Build full statement with schema
    full_table_name = f'"{schema}"."{table_name}"'

    ddl = f"""CREATE TABLE IF NOT EXISTS {full_table_name} (
{columns_sql}
);"""

    return ddl


def generate_insert_statement(
    table_name: str,
    columns: list[str],
    primary_key: str | None = None,
) -> str:
    """
    Generate a parameterized INSERT statement.

    Args:
        table_name: The table name.
        columns: List of column names.
        primary_key: The primary key column for ON CONFLICT handling.

    Returns:
        The INSERT SQL statement with placeholders.
    """
    settings = get_settings()
    schema = settings.db_schema

    # Quote column names
    quoted_columns = [f'"{col}"' for col in columns]
    columns_sql = ", ".join(quoted_columns)

    # Create placeholders
    placeholders = ", ".join(["%s"] * len(columns))

    # Build full table name
    full_table_name = f'"{schema}"."{table_name}"'

    # Base INSERT statement
    insert_sql = f"""INSERT INTO {full_table_name} ({columns_sql})
VALUES ({placeholders})"""

    # Add ON CONFLICT clause if we have a primary key
    if primary_key:
        insert_sql += f'\nON CONFLICT ("{primary_key}") DO NOTHING'

    return insert_sql + ";"


def prepare_record_values(
    record: dict[str, Any],
    columns: list[str],
) -> list[Any]:
    """
    Prepare record values for insertion, handling JSONB serialization.

    Args:
        record: The data record.
        columns: List of column names in order.

    Returns:
        List of values ready for database insertion.
    """
    values = []
    for col in columns:
        value = record.get(col)
        # Serialize dicts and lists to JSON strings for JSONB columns
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        values.append(value)
    return values
