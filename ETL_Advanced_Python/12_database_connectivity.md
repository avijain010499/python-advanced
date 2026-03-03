# 🗄️ Database Connectivity in Python (ETL Context)

---

## 📖 Explanation

Database connectivity is the backbone of ETL — extracting from and loading into relational databases. Python has multiple options:

- **`psycopg2`**: Low-level PostgreSQL adapter (fast, widely used).
- **`SQLAlchemy`**: High-level ORM + Core — database-agnostic, used in most ETL frameworks.
- **`sqlite3`**: Built-in SQLite for local/dev/testing.
- **`pandas`**: `read_sql()` / `to_sql()` for direct DataFrame-to-DB operations.

---

## 🧠 Must-Remember Points

- Always use **parameterized queries** (`%s` / `?`) — never string-format SQL (SQL injection!).
- Use `connection.commit()` to persist writes; `connection.rollback()` to undo.
- Always close connections with `with` context managers or `finally` blocks.
- `cursor.fetchall()` loads everything into memory — use `fetchmany(n)` for large results.
- `cursor.executemany()` is faster than looping individual `execute()` calls.
- SQLAlchemy `engine` is reusable; `connection` is for a single session.
- `psycopg2.extras.execute_values()` is the fastest batch insert method for PostgreSQL.
- Connection pooling (`pool_size`, `max_overflow`) is critical in multi-threaded ETL.
- Use `pandas.read_sql(query, engine, chunksize=N)` to read large tables in chunks.

---

## 💻 Code Examples

### 1️⃣ SQLite (Built-in — Great for Testing ETL)

```python
import sqlite3

# Create and connect to a local SQLite database
# (Creates file if not exists, :memory: for in-memory)
with sqlite3.connect("etl_test.db") as conn:
    cursor = conn.cursor()

    # DDL
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT,
            salary REAL
        )
    """)

    # Parameterized INSERT (safe from SQL injection)
    employees = [(1, "Alice", "IT", 75000), (2, "Bob", "HR", 60000)]
    cursor.executemany(
        "INSERT OR REPLACE INTO employees VALUES (?, ?, ?, ?)",
        employees
    )
    conn.commit()

    # SELECT
    cursor.execute("SELECT * FROM employees WHERE salary > ?", (65000,))
    rows = cursor.fetchall()
    for row in rows:
        print(row)  # (1, 'Alice', 'IT', 75000.0)
```

---

### 2️⃣ PostgreSQL with `psycopg2`

```python
import psycopg2
import psycopg2.extras

# Connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="etl_db",
    user="etl_user",
    password="secret",
    connect_timeout=10,
)

try:
    with conn.cursor() as cursor:
        # CREATE
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                order_id VARCHAR(20) PRIMARY KEY,
                customer_id INT NOT NULL,
                amount NUMERIC(12, 2),
                order_date DATE
            )
        """)

        # Batch INSERT — fast with execute_values
        records = [
            ("ORD-001", 101, 1500.00, "2024-01-15"),
            ("ORD-002", 102, 250.50, "2024-01-16"),
        ]
        psycopg2.extras.execute_values(
            cursor,
            "INSERT INTO sales (order_id, customer_id, amount, order_date) VALUES %s "
            "ON CONFLICT (order_id) DO UPDATE SET amount = EXCLUDED.amount",
            records,
        )
        conn.commit()
        print(f"Inserted {cursor.rowcount} records")

        # SELECT using DictCursor — rows as dicts
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as dict_cursor:
            dict_cursor.execute("SELECT * FROM sales WHERE amount > %s", (500,))
            for row in dict_cursor.fetchall():
                print(dict(row))  # {'order_id': 'ORD-001', ...}

except psycopg2.Error as e:
    conn.rollback()
    print(f"Database error: {e}")
    raise
finally:
    conn.close()
```

---

### 3️⃣ SQLAlchemy Core (Database-Agnostic ETL)

```python
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float
from sqlalchemy import insert, select

# Engine — one per application (manages connection pool)
engine = create_engine(
    "postgresql+psycopg2://user:password@localhost:5432/etl_db",
    pool_size=5,
    max_overflow=10,
    echo=False,  # Set to True for SQL debugging
)

# Execute raw SQL safely
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM sales WHERE amount > :amt"), {"amt": 500})
    count = result.scalar()
    print(f"Records: {count}")

# SQLAlchemy Table definition
metadata = MetaData()
products = Table("products", metadata,
    Column("product_id", Integer, primary_key=True),
    Column("name", String(100)),
    Column("price", Float),
)

# Create table
metadata.create_all(engine)

# Insert
with engine.begin() as conn:  # Auto-commits on exit
    conn.execute(insert(products), [
        {"product_id": 1, "name": "Widget A", "price": 19.99},
        {"product_id": 2, "name": "Widget B", "price": 29.99},
    ])

# Select
with engine.connect() as conn:
    rows = conn.execute(select(products).where(products.c.price > 20)).fetchall()
    for row in rows:
        print(row._asdict())
```

---

### 4️⃣ ETL: Pandas + SQLAlchemy (Read/Write DataFrames)

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://user:pass@localhost/etl_db")

# Read entire table into DataFrame
df = pd.read_sql("SELECT * FROM sales WHERE order_date >= '2024-01-01'", engine)

# Read in chunks (for large tables)
chunks = []
for chunk in pd.read_sql("SELECT * FROM large_table", engine, chunksize=10000):
    chunk_clean = chunk.dropna()
    chunks.append(chunk_clean)
df = pd.concat(chunks, ignore_index=True)

# Write DataFrame to table
df_transformed.to_sql(
    name="sales_transformed",
    con=engine,
    if_exists="append",   # 'replace', 'append', or 'fail'
    index=False,
    method="multi",       # Fast batch insert
    chunksize=1000,
)
print(f"Loaded {len(df_transformed)} rows to sales_transformed")
```

---

### 5️⃣ ETL: Upsert Pattern (Merge/Insert-or-Update)

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://user:pass@localhost/etl_db")

def upsert_records(records: list[dict], table: str, conflict_col: str) -> int:
    """Generic upsert for PostgreSQL using INSERT ... ON CONFLICT DO UPDATE."""
    if not records:
        return 0

    columns = list(records[0].keys())
    col_names = ", ".join(columns)
    placeholders = ", ".join(f":{col}" for col in columns)
    update_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_col)

    sql = text(f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col})
        DO UPDATE SET {update_set}
    """)

    with engine.begin() as conn:
        conn.execute(sql, records)
    return len(records)

records = [
    {"order_id": "ORD-001", "customer_id": 101, "amount": 1750.00},
    {"order_id": "ORD-003", "customer_id": 103, "amount": 500.00},
]
count = upsert_records(records, "sales", "order_id")
print(f"Upserted {count} records")
```

---

## 🏭 ETL Use Cases

| Tool                            | ETL Use Case                                    |
| ------------------------------- | ----------------------------------------------- |
| `sqlite3`                       | Local dev/testing pipelines, staging            |
| `psycopg2` + `execute_values`   | High-performance PostgreSQL batch inserts       |
| `SQLAlchemy` Core               | Database-agnostic ETL, table reflection         |
| `pd.read_sql(..., chunksize=N)` | Read large tables without OOM                   |
| `df.to_sql(method="multi")`     | Fast DataFrame to DB load                       |
| Upsert pattern                  | Idempotent ETL — re-runnable without duplicates |

---

## ⚠️ Common Pitfalls

```python
# WRONG: String formatting in SQL (SQL Injection!)
query = f"SELECT * FROM users WHERE name = '{user_input}'"  # ❌ DANGEROUS

# RIGHT: Always use parameterized queries
cursor.execute("SELECT * FROM users WHERE name = %s", (user_input,))

# WRONG: Loading entire large table at once
df = pd.read_sql("SELECT * FROM billion_row_table", engine)  # OOM crash!

# RIGHT: Use chunksize
for chunk in pd.read_sql("SELECT * FROM billion_row_table", engine, chunksize=10000):
    process(chunk)

# WRONG: Not closing connections
conn = psycopg2.connect(...)
data = conn.cursor().fetchall()
# conn is never closed — connection pool exhaust!

# RIGHT: Always use context managers
with psycopg2.connect(...) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT ...")
        data = cursor.fetchall()
```
