# API Schema Agent

A CLI-based AI Agent that acts as a **Universal Data Ingestor** - automatically fetching data from public APIs, inferring PostgreSQL schemas, and loading data into your database.

Built with the [Agno](https://github.com/agno-agi/agno) framework and Google Gemini 2.0 Flash.

## Features

- **Automatic Schema Inference**: Analyzes JSON structure and maps Python types to PostgreSQL types
- **Primary Key Detection**: Automatically identifies `id`, `uuid`, or `_id` fields as primary keys
- **Nested JSON Handling**: Stores complex nested objects as JSONB columns
- **Idempotent Operations**: Safely skips tables that already exist
- **Dual Mode**: Direct CLI execution or interactive AI agent chat
- **Dry Run Support**: Preview inferred schema without database changes

## Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL database
- Google Gemini API key ([Get one here](https://makersuite.google.com/app/apikey))

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/api-schema-agent.git
cd api-schema-agent

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file (use `.env.example` as template):

```bash
# PostgreSQL Connection
# Note: URL-encode special characters (@ = %40, ! = %21)
DB_URL=postgresql://user:password@host:5432/database

# Google Gemini API Key
GOOGLE_API_KEY=your_api_key_here

# Optional settings
TABLE_PREFIX=api_
DB_SCHEMA=public
```

### Usage

```bash
# Ingest data from any public API
python main.py ingest "https://jsonplaceholder.typicode.com/users"

# Preview schema without creating table (dry run)
python main.py ingest "https://api.example.com/data" --dry-run --verbose

# Use custom table name
python main.py ingest "https://api.example.com/data" --table-name my_custom_table

# Interactive AI agent mode (chat with Gemini)
python main.py ingest "https://api.example.com/data" --interactive

# Start a chat session
python main.py chat
```

## Example Output

```
$ python main.py ingest "https://jsonplaceholder.typicode.com/users" --verbose

╭──────────── Universal Data Ingestor ────────────╮
│ API Schema Agent                                │
│ URL: https://jsonplaceholder.typicode.com/users │
│ Mode: Direct                                    │
╰─────────────────────────────────────────────────╯

Ingestion Complete!

 Table Name     api_users
 Primary Key    id
 Rows Inserted  10 / 10

Table Schema:
┏━━━━━━━━━━┳━━━━━━━━┓
┃ Column   ┃ Type   ┃
┡━━━━━━━━━━╇━━━━━━━━┩
│ id       │ BIGINT │
│ name     │ TEXT   │
│ username │ TEXT   │
│ email    │ TEXT   │
│ address  │ JSONB  │
│ phone    │ TEXT   │
│ website  │ TEXT   │
│ company  │ JSONB  │
└──────────┴────────┘
```

## Design Decisions

### Why JSONB for Nested Objects?

When the agent encounters nested JSON structures (like `address` containing `street`, `city`, `geo`, etc.), it stores them as **JSONB columns** rather than flattening into separate columns.

**Rationale:**

1. **Flexibility**: Real-world APIs have unpredictable structures. JSONB handles any nesting depth without schema changes.

2. **Reliability**: Flattening creates fragile schemas - if an API adds a new nested field, the ingestor would need to alter the table. JSONB absorbs changes gracefully.

3. **PostgreSQL JSONB is Powerful**: You can still query nested fields efficiently:
   ```sql
   -- Get city from address
   SELECT name, address->>'city' as city FROM api_users;

   -- Get latitude from nested geo object
   SELECT name, address->'geo'->>'lat' as latitude FROM api_users;

   -- Filter by nested value
   SELECT * FROM api_users WHERE address->>'city' = 'Gwenborough';

   -- Index JSONB fields for performance
   CREATE INDEX idx_users_city ON api_users ((address->>'city'));
   ```

4. **Industry Standard**: This approach mirrors how modern data platforms (Snowflake, BigQuery, etc.) handle semi-structured data.

**Trade-off**: Queries on nested fields require JSONB operators (`->`, `->>`) instead of simple column access. This is acceptable for a universal ingestor prioritizing flexibility over query simplicity.

### Why Skip Existing Tables?

When a table already exists, the agent **skips** rather than appending or replacing data.

**Rationale:**

1. **Safety**: Prevents accidental data duplication or loss
2. **Predictability**: Clear behavior - run once to create, explicit action needed to modify
3. **Idempotency**: Safe to re-run scripts without side effects

To reload data, manually drop the table first:
```sql
DROP TABLE IF EXISTS api_users;
```

### Why Auto-Derive Table Names?

Table names are automatically extracted from the API URL path (e.g., `/users` → `api_users`).

**Rationale:**

1. **Convention over Configuration**: Reduces required user input
2. **Consistency**: All tables get the same prefix (`api_` by default)
3. **Override Available**: Use `--table-name` flag when needed

## Type Mapping

| Python Type | PostgreSQL Type |
|-------------|-----------------|
| `str` | TEXT |
| `int` | BIGINT |
| `float` | DOUBLE PRECISION |
| `bool` | BOOLEAN |
| `list` | JSONB |
| `dict` | JSONB |
| `None` | TEXT (nullable) |

## Project Structure

```
api-schema-agent/
├── main.py                 # CLI entry point (Typer)
├── requirements.txt        # Dependencies
├── .env.example           # Environment template
└── src/
    ├── agent.py           # Agno Agent with Gemini
    ├── config.py          # Pydantic settings
    ├── tools/
    │   ├── api_fetcher.py      # HTTP requests
    │   ├── schema_inferrer.py  # JSON → DDL
    │   └── db_executor.py      # PostgreSQL operations
    └── utils/
        ├── type_mapper.py      # Python → PostgreSQL types
        └── table_namer.py      # URL → table name
```

## Tech Stack

- **Framework**: [Agno](https://agno.com) (formerly Phidata) - AI agent framework
- **LLM**: Google Gemini 2.0 Flash
- **Database**: PostgreSQL with psycopg3
- **CLI**: Typer + Rich for beautiful terminal output
- **Config**: Pydantic Settings with python-dotenv

## License

MIT
