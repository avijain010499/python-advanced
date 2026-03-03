# 🗄️ Database Connectivity in Python (ETL Context)

---

## 🤔 What Is Database Connectivity?

ETL pipelines need to **read from** and **write to** databases. Python has three main approaches:

| Tool         | Level | Best For                               |
| ------------ | ----- | -------------------------------------- |
| `sqlite3`    | Low   | Local dev, testing, lightweight ETL    |
| `psycopg2`   | Low   | High-performance PostgreSQL            |
| `SQLAlchemy` | High  | Database-agnostic ETL, complex schemas |

> 💡 **Golden Rule**: **Always use parameterized queries** — never format SQL strings with `f"... {user_input}"`. This prevents SQL injection attacks and handles special characters correctly.

---

## 🧱 The Connection Lifecycle

```
Open Connection → Execute Query → Commit/Rollback → Close Connection
```

Always use `with` blocks so connections are closed even if an error occurs.

---

## 💻 Example 1: SQLite — Built-in, No Setup Needed

SQLite is perfect for learning, testing, and lightweight pipelines:

```python
import sqlite3

# Creates 'etl_test.db' file (or ':memory:' for in-memory, disappears on close)
with sqlite3.connect("etl_test.db") as conn:
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            dept TEXT,
            salary REAL
        )
    """)

    # ✅ Parameterized INSERT — safe from SQL injection
    employees = [(1, "Alice", "IT", 75000), (2, "Bob", "HR", 60000)]
    cursor.executemany(
        "INSERT OR REPLACE INTO employees VALUES (?, ?, ?, ?)",
        employees    # '?' placeholders — values passed separately
    )
    conn.commit()    # Save changes

    # ✅ Parameterized SELECT
    threshold = 65000
    cursor.execute("SELECT * FROM employees WHERE salary > ?", (threshold,))
    rows = cursor.fetchall()   # Returns list of tuples
    for row in rows:
        print(row)   # (1, 'Alice', 'IT', 75000.0)
```

---

## 💻 Example 2: PostgreSQL with `psycopg2`

```python
import psycopg2
import psycopg2.extras

# ---- Connect ----
conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="etl_db", user="etl_user", password="secret",
)

try:
    with conn.cursor() as cursor:
        # ---- Parameterized INSERT (%s placeholders for psycopg2) ----
        records = [
            ("ORD-001", 101, 1500.00, "2024-01-15"),
            ("ORD-002", 102, 250.50,  "2024-01-16"),
        ]
        # execute_values is MUCH faster than individual inserts in a loop
        psycopg2.extras.execute_values(
            cursor,
            "INSERT INTO sales (order_id, cust_id, amount, date) VALUES %s "
            "ON CONFLICT (order_id) DO UPDATE SET amount = EXCLUDED.amount",
            records,
        )
        conn.commit()
        print(f"Inserted/updated {cursor.rowcount} records")

    # ---- Read as dicts using RealDictCursor ----
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM sales WHERE amount > %s", (1000,))
        for row in cur.fetchall():
            print(dict(row))   # {'order_id': 'ORD-001', 'amount': 1500.0, ...}

except psycopg2.Error as e:
    conn.rollback()            # Undo changes on error
    raise
finally:
    conn.close()
```

---

## 💻 Example 3: SQLAlchemy — Database-Agnostic ETL

```python
from sqlalchemy import create_engine, text

# Connection string format: dialect+driver://user:pass@host:port/dbname
engine = create_engine(
    "postgresql+psycopg2://user:pass@localhost:5432/etl_db",
    pool_size=5,       # Maintain 5 connections in pool
    echo=False,        # Set True for SQL debug logging
)

# ---- Execute raw SQL safely ----
with engine.connect() as conn:
    result = conn.execute(
        text("SELECT COUNT(*) FROM sales WHERE amount > :min_amount"),
        {"min_amount": 500}   # Named parameter — safe!
    )
    count = result.scalar()
    print(f"Records: {count:,}")

# ---- Batch insert ----
with engine.begin() as conn:   # begin() auto-commits on exit, rollbacks on error
    conn.execute(
        text("INSERT INTO sales (order_id, amount) VALUES (:oid, :amt)"),
        [{"oid": "ORD-003", "amt": 800}, {"oid": "ORD-004", "amt": 1200}],
    )
    print("Inserted and committed!")
```

---

## 💻 Example 4: Pandas + SQLAlchemy for ETL

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://user:pass@localhost/etl_db")

# ---- Extract: SQL → DataFrame ----
df = pd.read_sql(
    "SELECT * FROM sales WHERE order_date >= '2024-01-01'",
    engine,
    parse_dates=["order_date"],
)
print(f"Loaded {len(df):,} rows")

# ---- Extract large table in chunks (avoid OOM) ----
chunks = []
for chunk in pd.read_sql("SELECT * FROM large_table", engine, chunksize=10000):
    clean = chunk.dropna(subset=["order_id"])
    chunks.append(clean)
df = pd.concat(chunks, ignore_index=True)

# ---- Load: DataFrame → SQL ----
df_transformed.to_sql(
    "sales_transformed",
    engine,
    if_exists="append",   # Options: 'replace', 'append', 'fail'
    index=False,
    method="multi",        # Batch insert (much faster than row-by-row)
    chunksize=5000,
)
print(f"Loaded {len(df_transformed):,} rows to sales_transformed")
```

---

## 💻 Example 5: Generic Upsert Pattern

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://user:pass@localhost/etl_db")

def upsert(records: list[dict], table: str, conflict_col: str) -> None:
    """
    Insert records. On conflict (duplicate key), update instead.
    Makes your ETL idempotent — safe to re-run without creating duplicates.
    """
    if not records:
        return
    cols      = list(records[0].keys())
    col_str   = ", ".join(cols)
    val_str   = ", ".join(f":{c}" for c in cols)
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col)

    sql = text(f"""
        INSERT INTO {table} ({col_str}) VALUES ({val_str})
        ON CONFLICT ({conflict_col}) DO UPDATE SET {update_str}
    """)
    with engine.begin() as conn:
        conn.execute(sql, records)
    print(f"Upserted {len(records)} records into {table}")

# Usage
upsert(
    [{"order_id": "ORD-001", "amount": 1750.0}, {"order_id": "ORD-999", "amount": 500.0}],
    table="sales",
    conflict_col="order_id",
)
```

---

## 🏭 ETL Use Cases Summary

| Tool                             | Use Case                            |
| -------------------------------- | ----------------------------------- |
| `sqlite3`                        | Local dev, testing with no DB setup |
| `psycopg2.extras.execute_values` | Fastest PostgreSQL batch inserts    |
| `SQLAlchemy engine.begin()`      | Auto-commit transactions            |
| `pd.read_sql(chunksize=N)`       | Extract large tables without OOM    |
| `df.to_sql(method="multi")`      | Fast DataFrame-to-DB loads          |
| Upsert pattern                   | Idempotent ETL — can re-run safely  |

---

## 🧠 Must-Remember Points

| Rule                      | Remember                                                           |
| ------------------------- | ------------------------------------------------------------------ |
| Parameterized queries     | `%s` (psycopg2) or `:name` (SQLAlchemy) — NEVER f-string SQL       |
| `conn.commit()`           | Required to persist writes (not needed with `with engine.begin()`) |
| `conn.rollback()`         | Call in `except` to undo partial writes                            |
| `chunksize` in `read_sql` | For large tables — avoid loading billions of rows at once          |
| `execute_values`          | 10–100x faster than looping `cursor.execute()`                     |

---

## ⚠️ Common Mistakes

```python
# ❌ SQL injection — NEVER do this!
name = "Alice'; DROP TABLE employees; --"
cursor.execute(f"SELECT * FROM employees WHERE name = '{name}'")   # DANGEROUS!

# ✅ Parameterized — always safe
cursor.execute("SELECT * FROM employees WHERE name = %s", (name,))

# ❌ Loading entire large table then filtering
df = pd.read_sql("SELECT * FROM fact_sales", engine)  # Millions of rows → OOM!

# ✅ Filter in SQL before pandas sees the data
df = pd.read_sql("SELECT * FROM fact_sales WHERE year = 2024", engine)
```
