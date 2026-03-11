# 🏗️ Introduction to dbt (Data Build Tool)

---

## 🤔 What Is dbt?

**dbt (Data Build Tool)** transforms data **inside your data warehouse** using SQL. It handles the **Transform** step in the modern ELT (Extract → Load → Transform) pattern.

> 💡 **The shift from ETL to ELT**: Modern cloud warehouses (Snowflake, BigQuery, Redshift) are powerful enough to transform data inside the warehouse itself. dbt lets you write that transformation logic in clean SQL, with software engineering best practices: version control, tests, documentation, and modularity.

**What dbt does:**

- Runs your `.sql` files as `SELECT` queries
- Wraps them in `CREATE TABLE/VIEW` automatically
- Handles dependency ordering between models
- Runs tests and generates documentation

**What dbt does NOT do:**

- Extract (fetch from APIs/databases)
- Load (move raw data into warehouse)

---

## 🧱 dbt vs Traditional SQL Scripts

| Traditional SQL Scripts         | dbt                                  |
| ------------------------------- | ------------------------------------ |
| One giant `CREATE TABLE` script | Modular `.sql` files (one per model) |
| Manual dependency management    | Automatic via `ref()`                |
| No testing                      | Built-in schema + custom tests       |
| No documentation                | Auto-generated from `schema.yml`     |
| No version control conventions  | Git-first workflow                   |

---

## 💻 Example 1: Installation & Project Setup

```python
# All dbt commands are run in the terminal
# In notebooks: prefix shell commands with !

# Install dbt (choose your adapter)
# !pip install dbt-core dbt-bigquery    # For BigQuery
# !pip install dbt-core dbt-snowflake   # For Snowflake
# !pip install dbt-core dbt-postgres    # For PostgreSQL
# !pip install dbt-core dbt-duckdb      # For DuckDB (local dev — easiest for learning!)

print("dbt installation commands (run in terminal):")
print("pip install dbt-core dbt-duckdb")
```

```python
# Create a new dbt project (run in terminal)
# !dbt init my_etl_project
# This creates the project structure:

project_structure = """
my_etl_project/
├── dbt_project.yml        ← Project config: name, version, model config
├── profiles.yml           ← Database connection settings (usually in ~/.dbt/)
├── models/                ← Your SQL transformation files live here
│   ├── staging/           ← Clean/rename raw source data
│   ├── intermediate/      ← Business logic joins
│   └── marts/             ← Final analytics-ready tables
├── tests/                 ← Custom SQL tests
├── macros/                ← Reusable Jinja functions
├── seeds/                 ← CSV files loaded as tables
├── snapshots/             ← SCD Type 2 history tables
└── analyses/              ← Ad-hoc SQL (not materialized)
"""
print(project_structure)
```

---

## 💻 Example 2: `dbt_project.yml` — Project Configuration

```python
# dbt_project.yml content
dbt_project_yml = """
name: my_etl_project
version: '1.0.0'
config-version: 2

profile: my_etl_project   # References the profile in ~/.dbt/profiles.yml

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]

models:
  my_etl_project:
    staging:
      +materialized: view      # Staging models → views (fast, no storage cost)
    intermediate:
      +materialized: view
    marts:
      +materialized: table     # Final models → tables (fast to query)
      +schema: analytics       # Write to 'analytics' schema in warehouse
"""
print(dbt_project_yml)
```

---

## 💻 Example 3: `profiles.yml` — Database Connection

```python
# ~/.dbt/profiles.yml (never commit this to git — it has credentials!)
profiles_yml = """
my_etl_project:
  target: dev            # Default target environment
  outputs:
    dev:
      type: duckdb       # Or: bigquery, snowflake, postgres, redshift
      path: /tmp/dev.duckdb   # DuckDB: just a local file!

    prod:
      type: snowflake
      account: myorg.us-east-1
      user: "{{ env_var('SNOWFLAKE_USER') }}"        # ← Read from env variable
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: PROD_DB
      schema: ANALYTICS
      warehouse: TRANSFORM_WH
      role: TRANSFORMER_ROLE
"""
print(profiles_yml)
```

---

## 💻 Example 4: Core dbt Commands

```python
# All run in your terminal inside the project directory

commands = {
    "dbt debug":        "Test the connection to your warehouse",
    "dbt run":          "Execute all models (CREATE the tables/views)",
    "dbt test":         "Run all data quality tests",
    "dbt docs generate":"Generate HTML documentation",
    "dbt docs serve":   "Open docs in browser at http://localhost:8080",
    "dbt compile":      "Compile SQL without running (useful for debugging)",
    "dbt seed":         "Load CSV files in seeds/ to warehouse tables",
    "dbt snapshot":     "Run SCD Type 2 history tracking",
}

for cmd, desc in commands.items():
    print(f"  {cmd:<25} → {desc}")
```

---

## 💻 Example 5: Run Specific Models

```python
# Run a specific model
# !dbt run --select stg_orders

# Run all models in a folder
# !dbt run --select staging.*

# Run a model and all its UPSTREAM dependencies (+prefix)
# !dbt run --select +fct_sales

# Run a model and all its DOWNSTREAM dependents (suffix+)
# !dbt run --select stg_orders+

# Run all models BETWEEN two nodes
# !dbt run --select stg_orders+,+fct_sales

# Run changed models only (great for CI)
# !dbt run --select state:modified

examples = [
    "dbt run --select stg_orders",
    "dbt run --select staging.*",
    "dbt run --select +fct_sales",
    "dbt test --select stg_orders",
    "dbt run --select state:modified",
]
for e in examples:
    print(f"  {e}")
```

---

## 🏭 Summary

| Concept           | What It Is                                                   |
| ----------------- | ------------------------------------------------------------ |
| `dbt_project.yml` | Project-level config — model paths, materialization defaults |
| `profiles.yml`    | Warehouse connection credentials (keep secret!)              |
| `dbt run`         | Execute all models — creates tables/views                    |
| `dbt test`        | Run all quality tests — catches bad data                     |
| `dbt docs`        | Auto-generate interactive documentation                      |
| `models/` folder  | Home of all your `.sql` transformation files                 |

---

## ⚠️ Common Mistakes Beginners Make

```python
mistakes = [
    "Committing profiles.yml to git — NEVER do this (passwords in plain text)!",
    "Not using ref() between models — causes ordering issues",
    "Putting all SQL in one giant model — split into staging → intermediate → marts",
    "Running dbt run before dbt debug — check connection first!",
    "Setting marts/ to 'view' — final analytics tables should be materialized as tables",
]
for i, m in enumerate(mistakes, 1):
    print(f"  ❌ Mistake {i}: {m}")
```
