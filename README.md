# API Schema Agent

<div align="center">

**Universal Data Ingestor** - Automatically fetch data from public APIs, infer PostgreSQL schemas, and load data into your database.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-316192.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Built with [Agno](https://github.com/agno-agi/agno) and Google Gemini 2.0 Flash

</div>

---

## Features

- **Automatic Schema Inference** - Analyzes JSON structure and maps to PostgreSQL types
- **Flatten Mode** - Expand nested objects into columns (`user.address.city` → `user_address_city`)
- **Primary Key Detection** - Identifies `id`, `uuid`, or `_id` fields automatically
- **JSONB Support** - Stores complex nested objects as queryable JSONB
- **Idempotent Operations** - Safely skips tables that already exist
- **Dual Mode** - Direct CLI execution or interactive AI agent chat
- **Dry Run** - Preview inferred schema without database changes

---

## Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL database
- Google Gemini API key ([Get one here](https://makersuite.google.com/app/apikey))

### Installation

```bash
git clone https://github.com/yourusername/api-schema-agent.git
cd api-schema-agent

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### Configuration

Create a `.env` file:

```bash
# PostgreSQL (URL-encode special chars: @ = %40, ! = %21)
DB_URL=postgresql://user:password@host:5432/database

# Google Gemini API Key
GOOGLE_API_KEY=your_api_key_here

# Optional
TABLE_PREFIX=api_
DB_SCHEMA=public
```

---

## Usage

### Basic Ingestion

```bash
# Ingest from any public API
python main.py ingest "https://jsonplaceholder.typicode.com/users"

# Preview schema without creating table
python main.py ingest "https://api.example.com/data" --dry-run --verbose

# Custom table name
python main.py ingest "https://api.example.com/data" --table-name my_table
```

### Flatten Mode

Expand nested JSON objects into separate columns instead of JSONB:

```bash
# Flatten one level deep (default)
python main.py ingest "https://jsonplaceholder.typicode.com/users" --flatten

# Flatten two levels deep
python main.py ingest "https://jsonplaceholder.typicode.com/users" --flatten --depth 2
```

**Example transformation:**

```json
{"id": 1, "user": {"name": "John", "address": {"city": "NYC"}}}
```

| Flag | Result |
|------|--------|
| (none) | `user` → JSONB |
| `--flatten` | `user_name` → TEXT, `user_address` → JSONB |
| `--flatten --depth 2` | `user_name` → TEXT, `user_address_city` → TEXT |

### Interactive Mode

```bash
# AI agent mode - chat with Gemini to ingest data
python main.py ingest "https://api.example.com/data" --interactive

# Start a chat session
python main.py chat
```

### CLI Options

| Option | Short | Description |
|--------|-------|-------------|
| `--table-name` | `-t` | Custom table name |
| `--dry-run` | `-d` | Preview schema only |
| `--verbose` | `-v` | Show DDL statements |
| `--flatten` | `-f` | Flatten nested objects |
| `--depth` | | Flatten depth (default: 1) |
| `--interactive` | `-i` | AI agent mode |

---

## Example Output

### Standard Mode (JSONB)

```
$ python main.py ingest "https://jsonplaceholder.typicode.com/users" -v

╭──────────── Universal Data Ingestor ────────────╮
│ API Schema Agent                                │
│ URL: https://jsonplaceholder.typicode.com/users │
│ Mode: Direct                                    │
╰─────────────────────────────────────────────────╯

Ingestion Complete!

 Table Name     api_users
 Primary Key    id
 Rows Inserted  10 / 10

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

### Flatten Mode

```
$ python main.py ingest "https://jsonplaceholder.typicode.com/users" --flatten --depth 2 -v -d

╭──────────── Universal Data Ingestor ────────────╮
│ API Schema Agent                                │
│ URL: https://jsonplaceholder.typicode.com/users │
│ Mode: Dry Run, Flatten (depth=2)                │
╰─────────────────────────────────────────────────╯

Dry Run Results

 Table Name    api_users
 Primary Key   id
 Record Count  10
 Flatten Mode  Enabled (depth=2)

┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Column              ┃ PostgreSQL Type ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ id                  │ BIGINT          │
│ name                │ TEXT            │
│ username            │ TEXT            │
│ email               │ TEXT            │
│ address_street      │ TEXT            │
│ address_suite       │ TEXT            │
│ address_city        │ TEXT            │
│ address_zipcode     │ TEXT            │
│ address_geo_lat     │ TEXT            │
│ address_geo_lng     │ TEXT            │
│ phone               │ TEXT            │
│ website             │ TEXT            │
│ company_name        │ TEXT            │
│ company_catchPhrase │ TEXT            │
│ company_bs          │ TEXT            │
└─────────────────────┴─────────────────┘
```

---

## Design Decisions

### JSONB vs Flatten

| Approach | Use When |
|----------|----------|
| **JSONB** (default) | Schema flexibility, unknown nesting, frequent API changes |
| **Flatten** (`--flatten`) | SQL simplicity, known structure, analytics/BI tools |

**Querying JSONB columns:**

```sql
-- Get city from address
SELECT name, address->>'city' as city FROM api_users;

-- Filter by nested value
SELECT * FROM api_users WHERE address->>'city' = 'Gwenborough';

-- Index for performance
CREATE INDEX idx_users_city ON api_users ((address->>'city'));
```

### Idempotent Operations

When a table exists, the agent **skips** rather than appending or replacing. This prevents accidental duplication and makes scripts safe to re-run.

To reload data:
```sql
DROP TABLE IF EXISTS api_users;
```

### Type Mapping

| Python | PostgreSQL |
|--------|------------|
| `str` | TEXT |
| `int` | BIGINT |
| `float` | DOUBLE PRECISION |
| `bool` | BOOLEAN |
| `list` | JSONB |
| `dict` | JSONB |
| `None` | TEXT (nullable) |

### Flatten Edge Cases

| Scenario | Behavior |
|----------|----------|
| **Arrays** | Always JSONB (never flattened) |
| **Name collision** | First occurrence wins + warning |
| **Type conflict** | More permissive type used |

Type precedence: `JSONB > TEXT > DOUBLE PRECISION > BIGINT > BOOLEAN`

---

## Project Structure

```
api-schema-agent/
├── main.py                 # CLI entry point (Typer)
├── requirements.txt
├── .env.example
└── src/
    ├── agent.py            # Agno Agent + Gemini
    ├── config.py           # Pydantic settings
    ├── tools/
    │   ├── api_fetcher.py      # HTTP requests
    │   ├── schema_inferrer.py  # JSON → DDL
    │   └── db_executor.py      # PostgreSQL ops
    └── utils/
        ├── type_mapper.py      # Type mapping + flatten logic
        └── table_namer.py      # URL → table name
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| AI Framework | [Agno](https://agno.com) |
| LLM | Google Gemini 2.0 Flash |
| Database | PostgreSQL + psycopg3 |
| CLI | Typer + Rich |
| Config | Pydantic Settings |

---

## License

MIT
