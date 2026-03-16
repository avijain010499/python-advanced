# Full dbt Project — Walkthrough

## What Was Built

A complete, runnable dbt project at:
`/Users/aviraljain/Downloads/python advanced/my_etl_project/`

With a `venvdbt` virtual environment using **Python 3.11 + DuckDB** (zero cloud setup).

---

## Project Structure

```
my_etl_project/
├── venvdbt/                        ← Python 3.11 virtual env (dbt-core + dbt-duckdb)
├── dbt_project.yml                 ← Project config + materialization defaults
├── profiles.yml                    ← DuckDB connection (dev + prod targets)
├── dev.duckdb                      ← Local warehouse file (auto-created by dbt)
│
├── seeds/
│   ├── raw_orders.csv              → 12 sample orders
│   ├── raw_customers.csv           → 8 customers (regions, segments)
│   ├── raw_products.csv            → 8 products
│   └── country_codes.csv           → Country lookup table
│
├── models/
│   ├── staging/
│   │   ├── src_raw.yml             ← Source definitions (freshness + tests)
│   │   ├── schema.yml              ← Column tests + relationships
│   │   ├── stg_orders.sql          → View: clean & cast raw orders
│   │   ├── stg_customers.sql       → View: clean customer records
│   │   └── stg_products.sql        → View: clean product catalog
│   │
│   ├── intermediate/
│   │   ├── schema.yml
│   │   ├── int_orders_cleaned.sql  → Ephemeral CTE: null/negative amount handling
│   │   └── int_orders_customers.sql → View: orders enriched with customer data
│   │
│   └── marts/
│       ├── schema.yml              ← Full documentation (owner, SLA, columns)
│       ├── fct_sales.sql           → Table: final sales fact (COMPLETE orders only)
│       └── fct_sales_incremental.sql → Incremental: only new rows on each run
│
├── tests/
│   └── generic/
│       └── positive_values.sql     ← Custom generic test (fails if value ≤ 0)
│
└── macros/
    └── cents_to_dollars.sql        ← Jinja macro: column / 100.0
```

---

## Verification Results

| Command | Result |
| :--- | :--- |
| `dbt debug` | ✅ All checks passed (DuckDB connected) |
| `dbt seed` | ✅ PASS=4 — 4 CSV files loaded as tables |
| `dbt run` | ✅ PASS=6 — All 6 models built (3 views + 1 view + 1 table + 1 incremental) |
| `dbt test` | ✅ **PASS=37, ERROR=0** — All data quality tests green |
| `dbt docs generate` | ✅ Catalog written to `target/catalog.json` |

### dbt test breakdown (37 tests, all green):
- `not_null` tests on all primary + foreign keys
- `unique` tests on all PKs
- `accepted_values` — status, region, segment values validated
- `relationships` — `stg_orders.customer_id` → `stg_customers.customer_id`
- `positive_values` (custom) — amount fields validated across staging + marts
- Source-level tests on raw seed tables

---

## How to Run It

Open terminal and run:

```bash
cd "/Users/aviraljain/Downloads/python advanced/my_etl_project"

# Activate the virtual environment
source venvdbt/bin/activate

# Always prefix dbt commands with DBT_PROFILES_DIR=. (profiles.yml is inside the project)
dbt debug                      # Check connection
dbt seed                       # Load CSVs into DuckDB
dbt run                        # Build all models
dbt test                       # Run all 37 tests
dbt docs generate              # Generate docs
dbt docs serve                 # Open browser at http://localhost:8080
```

### Useful Selective Commands:
```bash
dbt run --select staging.*              # Run only staging models
dbt run --select tag:sales              # Run only 'sales' tagged models
dbt run --select +fct_sales             # Run fct_sales + all its upstream dependencies
dbt run --full-refresh                  # Force full rebuild of incremental models
dbt test --select stg_orders            # Test only one model
```

---

## Key Concepts Demonstrated

| Concept | File |
| :--- | :--- |
| `materialized: view` (staging) | `stg_orders.sql`, `stg_customers.sql`, `stg_products.sql` |
| `materialized: view` (intermediate) | `int_orders_customers.sql` |
| `materialized: ephemeral` | `int_orders_cleaned.sql` |
| `materialized: table` | `fct_sales.sql` |
| `materialized: incremental` | `fct_sales_incremental.sql` |
| `{{ source('raw', 'raw_orders') }}` | All staging models |
| `{{ ref('stg_orders') }}` | All intermediate + mart models |
| Source freshness check | `src_raw.yml` |
| Custom generic test | `tests/generic/positive_values.sql` |
| Jinja macro | `macros/cents_to_dollars.sql` |
| Tags + selective run | `fct_sales.sql` config block |
| Dev vs Prod targets | `profiles.yml` |
