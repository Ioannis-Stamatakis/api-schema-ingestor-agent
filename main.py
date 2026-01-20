#!/usr/bin/env python3
"""
API Schema Agent - Universal Data Ingestor CLI

A CLI tool that uses an AI agent to ingest data from public APIs into PostgreSQL.
Built with the Agno framework and Google Gemini.
"""

import sys
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.agent import create_agent, ingest_data
from src.config import get_settings

app = typer.Typer(
    name="api-schema-agent",
    help="Universal Data Ingestor - Load data from public APIs into PostgreSQL",
    add_completion=False,
)
console = Console()


@app.command()
def ingest(
    url: str = typer.Argument(..., help="The public API URL to fetch data from"),
    table_name: Optional[str] = typer.Option(
        None,
        "--table-name",
        "-t",
        help="Custom table name (auto-derived from URL if not provided)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-d",
        help="Only show the inferred schema without creating table or inserting data",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed output including DDL statements",
    ),
    interactive: bool = typer.Option(
        False,
        "--interactive",
        "-i",
        help="Use interactive agent mode (chat with the AI)",
    ),
    flatten: bool = typer.Option(
        False,
        "--flatten",
        "-f",
        help="Flatten nested JSON objects into separate columns instead of JSONB",
    ),
    depth: int = typer.Option(
        1,
        "--depth",
        help="Maximum depth to flatten nested objects (default: 1, requires --flatten)",
    ),
    append: bool = typer.Option(
        False,
        "--append",
        "-a",
        help="Append data to existing table (skip table creation if exists)",
    ),
):
    """
    Ingest data from a public API URL into PostgreSQL.

    Examples:
        python main.py https://jsonplaceholder.typicode.com/users
        python main.py https://jsonplaceholder.typicode.com/posts --table-name my_posts
        python main.py https://api.coingecko.com/api/v3/coins/list --dry-run
    """
    # Validate settings
    try:
        settings = get_settings()
    except Exception as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        console.print("Please ensure .env file exists with DB_URL and GOOGLE_API_KEY")
        raise typer.Exit(code=1)

    # Build mode string
    mode_parts = []
    if dry_run:
        mode_parts.append("Dry Run")
    if interactive:
        mode_parts.append("Interactive")
    if flatten:
        mode_parts.append(f"Flatten (depth={depth})")
    if append:
        mode_parts.append("Append")
    if not mode_parts:
        mode_parts.append("Direct")
    mode_str = ", ".join(mode_parts)

    console.print(Panel.fit(
        f"[bold blue]API Schema Agent[/bold blue]\n"
        f"URL: {url}\n"
        f"Mode: {mode_str}",
        title="Universal Data Ingestor",
    ))

    if interactive:
        # Interactive agent mode
        _run_interactive_mode(url, table_name)
    else:
        # Direct ingestion mode
        _run_direct_mode(url, table_name, dry_run, verbose, flatten, depth, append)


def _run_interactive_mode(url: str, table_name: Optional[str]):
    """Run the agent in interactive chat mode."""
    console.print("\n[yellow]Starting interactive agent...[/yellow]\n")

    try:
        agent = create_agent()

        # Build the initial prompt
        prompt = f"Please ingest data from this API URL into PostgreSQL: {url}"
        if table_name:
            prompt += f"\nUse the table name: {table_name}"

        # Run the agent
        agent.print_response(prompt)

    except Exception as e:
        console.print(f"[red]Agent error:[/red] {e}")
        raise typer.Exit(code=1)


def _run_direct_mode(
    url: str,
    table_name: Optional[str],
    dry_run: bool,
    verbose: bool,
    flatten: bool = False,
    depth: int = 1,
    append: bool = False,
):
    """Run direct data ingestion without agent interaction."""
    with console.status("[bold green]Processing...[/bold green]"):
        result = ingest_data(url, table_name, dry_run, flatten=flatten, depth=depth, append=append)

    if not result.get("success"):
        if result.get("action") == "skipped":
            console.print(f"\n[yellow]Skipped:[/yellow] {result.get('message')}")
            raise typer.Exit(code=0)
        else:
            error_msg = result.get('error') or result.get('message') or 'Unknown error'
            if result.get('errors'):
                error_msg += f"\n  {result['errors'][0]}"
            console.print(f"\n[red]Error:[/red] {error_msg}")
            raise typer.Exit(code=1)

    # Display results
    if dry_run:
        _display_dry_run_results(result, verbose)
    else:
        _display_ingestion_results(result, verbose)


def _display_dry_run_results(result: dict, verbose: bool):
    """Display results from a dry run."""
    console.print("\n[bold green]Dry Run Results[/bold green]\n")

    # Table info
    info_table = Table(show_header=False, box=None)
    info_table.add_column("Property", style="cyan")
    info_table.add_column("Value")

    info_table.add_row("Table Name", result["table_name"])
    info_table.add_row("Primary Key", result.get("primary_key") or "None detected")
    info_table.add_row("Record Count", str(result["record_count"]))
    if result.get("flatten"):
        info_table.add_row("Flatten Mode", f"Enabled (depth={result.get('depth', 1)})")

    console.print(info_table)

    # Warnings
    if result.get("warnings"):
        console.print("\n[yellow]Warnings:[/yellow]")
        for warning in result["warnings"]:
            console.print(f"  - {warning}")

    # Columns
    console.print("\n[bold]Inferred Columns:[/bold]")
    columns_table = Table(show_header=True)
    columns_table.add_column("Column", style="cyan")
    columns_table.add_column("PostgreSQL Type", style="green")

    for col_name, col_type in result["columns"].items():
        columns_table.add_row(col_name, col_type)

    console.print(columns_table)

    # DDL
    if verbose and result.get("ddl"):
        console.print("\n[bold]Generated DDL:[/bold]")
        console.print(Panel(result["ddl"], title="CREATE TABLE", border_style="blue"))


def _display_ingestion_results(result: dict, verbose: bool):
    """Display results from actual data ingestion."""
    console.print("\n[bold green]Ingestion Complete![/bold green]\n")

    # Summary table
    summary_table = Table(show_header=False, box=None)
    summary_table.add_column("Property", style="cyan")
    summary_table.add_column("Value")

    summary_table.add_row("Table Name", result["table_name"])
    summary_table.add_row("Primary Key", result.get("primary_key") or "None")
    summary_table.add_row("Rows Inserted", f"{result['rows_inserted']} / {result['total_records']}")
    if result.get("flatten"):
        summary_table.add_row("Flatten Mode", f"Enabled (depth={result.get('depth', 1)})")

    console.print(summary_table)

    # Schema warnings (type conflicts, collisions)
    if result.get("warnings"):
        console.print("\n[yellow]Schema Warnings:[/yellow]")
        for warning in result["warnings"][:5]:
            console.print(f"  - {warning}")
        if len(result["warnings"]) > 5:
            console.print(f"  ... and {len(result['warnings']) - 5} more")

    # Columns
    if verbose:
        console.print("\n[bold]Table Schema:[/bold]")
        columns_table = Table(show_header=True)
        columns_table.add_column("Column", style="cyan")
        columns_table.add_column("Type", style="green")

        for col_name, col_type in result["columns"].items():
            columns_table.add_row(col_name, col_type)

        console.print(columns_table)

    # Insert errors
    if result.get("errors"):
        console.print("\n[red]Insert Errors:[/red]")
        for error in result["errors"][:5]:  # Show first 5 errors
            console.print(f"  - {error}")
        if len(result["errors"]) > 5:
            console.print(f"  ... and {len(result['errors']) - 5} more")

    console.print(f"\n[dim]{result['message']}[/dim]")


@app.command()
def chat():
    """
    Start an interactive chat session with the data ingestion agent.

    In this mode, you can have a conversation with the AI agent about
    ingesting data from various APIs.
    """
    console.print(Panel.fit(
        "[bold blue]API Schema Agent - Interactive Mode[/bold blue]\n"
        "Chat with the AI agent to ingest data from APIs.\n"
        "Type 'quit' or 'exit' to end the session.",
        title="Interactive Chat",
    ))

    try:
        agent = create_agent()

        while True:
            try:
                user_input = console.input("\n[bold cyan]You:[/bold cyan] ")
            except (KeyboardInterrupt, EOFError):
                break

            if user_input.lower().strip() in ("quit", "exit", "q"):
                break

            if not user_input.strip():
                continue

            console.print()
            agent.print_response(user_input)

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)

    console.print("\n[dim]Goodbye![/dim]")


@app.command()
def status():
    """
    Show current configuration and connection status.

    Displays the current settings and tests the database connection.
    """
    console.print(Panel.fit(
        "[bold blue]API Schema Agent - Status[/bold blue]",
        title="Configuration & Connection Status",
    ))

    # Load and display settings
    try:
        settings = get_settings()
    except Exception as e:
        console.print(f"\n[red]Configuration Error:[/red] {e}")
        raise typer.Exit(code=1)

    # Display configuration
    config_table = Table(show_header=True, title="Configuration")
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")

    config_table.add_row("Table Prefix", settings.table_prefix)
    config_table.add_row("DB Schema", settings.db_schema)
    config_table.add_row("Request Timeout", f"{settings.request_timeout}s")
    config_table.add_row("Google API Key", "***configured***" if settings.google_api_key else "[red]Not set[/red]")

    # Mask the database URL for security
    db_url_masked = settings.db_url[:20] + "***" if len(settings.db_url) > 20 else "***"
    config_table.add_row("Database URL", db_url_masked)

    console.print(config_table)

    # Test database connection
    console.print("\n[bold]Database Connection Test:[/bold]")
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(settings.db_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version_info = result.scalar()
            console.print(f"  [green]Connected successfully[/green]")
            console.print(f"  PostgreSQL: {version_info[:50]}..." if len(version_info) > 50 else f"  PostgreSQL: {version_info}")
    except Exception as e:
        console.print(f"  [red]Connection failed:[/red] {e}")


@app.command()
def version():
    """Show version information."""
    console.print("[bold]API Schema Agent[/bold] v0.1.0")
    console.print("Built with Agno Framework + Google Gemini")


if __name__ == "__main__":
    app()
