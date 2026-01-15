"""Database executor tool for PostgreSQL DDL and DML operations."""

from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.config import get_settings
from src.tools.schema_inferrer import generate_insert_statement, prepare_record_values


def get_connection() -> psycopg.Connection:
    """
    Create a new database connection.

    Returns:
        A psycopg connection object.
    """
    settings = get_settings()
    return psycopg.connect(settings.db_url)


def check_table_exists(table_name: str) -> dict[str, Any]:
    """
    Check if a table already exists in the database.

    Args:
        table_name: The table name to check.

    Returns:
        Dictionary with exists status and table info.
    """
    settings = get_settings()
    schema = settings.db_schema

    try:
        with get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_name = %s
                    ) as exists
                    """,
                    (schema, table_name),
                )
                result = cur.fetchone()
                exists = result["exists"] if result else False

                return {
                    "exists": exists,
                    "table_name": table_name,
                    "schema": schema,
                    "error": None,
                }

    except psycopg.Error as e:
        return {
            "exists": False,
            "table_name": table_name,
            "schema": schema,
            "error": f"Database error: {str(e)}",
        }


def execute_ddl(ddl: str) -> dict[str, Any]:
    """
    Execute a DDL statement (CREATE TABLE).

    Args:
        ddl: The DDL statement to execute.

    Returns:
        Dictionary with execution status and details.
    """
    if not ddl:
        return {
            "success": False,
            "message": "No DDL statement provided",
            "error": "Empty DDL",
        }

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

        return {
            "success": True,
            "message": "Table created successfully",
            "ddl": ddl,
            "error": None,
        }

    except psycopg.Error as e:
        return {
            "success": False,
            "message": "Failed to create table",
            "ddl": ddl,
            "error": f"Database error: {str(e)}",
        }


def execute_insert(
    table_name: str,
    records: list[dict[str, Any]],
    columns: list[str],
    primary_key: str | None = None,
    batch_size: int = 100,
) -> dict[str, Any]:
    """
    Insert records into a table using batch inserts.

    Args:
        table_name: The target table name.
        records: List of records to insert.
        columns: List of column names in order.
        primary_key: Primary key column for ON CONFLICT handling.
        batch_size: Number of records per batch.

    Returns:
        Dictionary with insertion results.
    """
    if not records:
        return {
            "success": True,
            "rows_inserted": 0,
            "message": "No records to insert",
            "error": None,
        }

    insert_sql = generate_insert_statement(table_name, columns, primary_key)
    total_inserted = 0
    errors = []

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Process in batches
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]

                    for record in batch:
                        try:
                            values = prepare_record_values(record, columns)
                            cur.execute(insert_sql, values)
                            total_inserted += cur.rowcount
                        except psycopg.Error as e:
                            errors.append(f"Record {i}: {str(e)}")

                    # Commit each batch
                    conn.commit()

        return {
            "success": len(errors) == 0,
            "rows_inserted": total_inserted,
            "total_records": len(records),
            "message": f"Inserted {total_inserted} of {len(records)} records",
            "errors": errors if errors else None,
            "error": None,
        }

    except psycopg.Error as e:
        return {
            "success": False,
            "rows_inserted": total_inserted,
            "total_records": len(records),
            "message": "Insert operation failed",
            "error": f"Database error: {str(e)}",
        }


def get_table_row_count(table_name: str) -> dict[str, Any]:
    """
    Get the current row count for a table.

    Args:
        table_name: The table to count rows in.

    Returns:
        Dictionary with row count and status.
    """
    settings = get_settings()
    schema = settings.db_schema

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Use quoted identifiers
                cur.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
                )
                count = cur.fetchone()[0]

        return {
            "count": count,
            "table_name": table_name,
            "error": None,
        }

    except psycopg.Error as e:
        return {
            "count": 0,
            "table_name": table_name,
            "error": f"Database error: {str(e)}",
        }
