"""Agno Agent initialization with Google Gemini and custom tools."""

from typing import Any

from agno.agent import Agent
from agno.models.google import Gemini

from src.config import get_settings
from src.tools.api_fetcher import fetch_api_data
from src.tools.db_executor import (
    check_table_exists,
    execute_ddl,
    execute_insert,
    get_table_row_count,
)
from src.tools.schema_inferrer import infer_schema
from src.utils.table_namer import extract_table_name


def create_agent() -> Agent:
    """
    Create and configure the data ingestion agent.

    Returns:
        Configured Agno Agent with Gemini model and custom tools.
    """
    settings = get_settings()

    agent = Agent(
        name="Universal Data Ingestor",
        model=Gemini(
            id="gemini-2.0-flash",
            api_key=settings.google_api_key,
        ),
        tools=[
            fetch_api_data,
            infer_schema,
            check_table_exists,
            execute_ddl,
            execute_insert,
            get_table_row_count,
            extract_table_name,
        ],
        instructions=[
            "You are a Universal Data Ingestor agent that helps users load data from public APIs into PostgreSQL.",
            "When given an API URL, follow these steps:",
            "1. First, extract a table name from the URL using extract_table_name",
            "2. Check if the table already exists using check_table_exists",
            "3. If the table exists, inform the user and stop (do not insert duplicate data)",
            "4. If the table doesn't exist, fetch the data using fetch_api_data",
            "5. Infer the schema using infer_schema",
            "6. Create the table using execute_ddl",
            "7. Insert the data using execute_insert",
            "8. Report the results to the user",
            "",
            "Important rules:",
            "- Always check if the table exists before creating it",
            "- If the table exists, report this and do NOT attempt to insert data",
            "- Nested JSON objects should be stored as JSONB columns",
            "- Detect and use 'id' or 'uuid' fields as primary keys",
            "- Provide clear summaries of what was done",
        ],
        markdown=True,
    )

    return agent


def ingest_data(url: str, table_name: str | None = None, dry_run: bool = False) -> dict[str, Any]:
    """
    Ingest data from an API URL into PostgreSQL.

    This function provides a programmatic interface for data ingestion,
    separate from the interactive agent.

    Args:
        url: The API URL to fetch data from.
        table_name: Optional custom table name.
        dry_run: If True, only infer schema without creating table or inserting.

    Returns:
        Dictionary with ingestion results.
    """
    # Step 1: Extract table name
    final_table_name = extract_table_name(url, table_name)

    # Step 2: Check if table exists
    table_check = check_table_exists(final_table_name)
    if table_check.get("error"):
        return {"success": False, "error": table_check["error"]}

    if table_check["exists"]:
        return {
            "success": False,
            "table_name": final_table_name,
            "message": f"Table '{final_table_name}' already exists. Skipping to avoid duplicates.",
            "action": "skipped",
        }

    # Step 3: Fetch data
    fetch_result = fetch_api_data(url)
    if fetch_result.get("error"):
        return {"success": False, "error": fetch_result["error"]}

    data = fetch_result["data"]
    if not data:
        return {"success": False, "error": "No data returned from API"}

    # Normalize to list
    records = data if isinstance(data, list) else [data]

    # Step 4: Infer schema
    schema_result = infer_schema(records, final_table_name)
    if schema_result.get("error"):
        return {"success": False, "error": schema_result["error"]}

    if dry_run:
        return {
            "success": True,
            "dry_run": True,
            "table_name": final_table_name,
            "ddl": schema_result["ddl"],
            "columns": schema_result["columns"],
            "primary_key": schema_result["primary_key"],
            "record_count": len(records),
        }

    # Step 5: Create table
    ddl_result = execute_ddl(schema_result["ddl"])
    if not ddl_result["success"]:
        return {"success": False, "error": ddl_result["error"]}

    # Step 6: Insert data
    columns = list(schema_result["columns"].keys())
    insert_result = execute_insert(
        final_table_name,
        records,
        columns,
        schema_result["primary_key"],
    )

    return {
        "success": insert_result["success"],
        "table_name": final_table_name,
        "columns": schema_result["columns"],
        "primary_key": schema_result["primary_key"],
        "rows_inserted": insert_result["rows_inserted"],
        "total_records": len(records),
        "message": insert_result["message"],
        "errors": insert_result.get("errors"),
    }
