# 💾 Pandas I/O — Read & Write (ETL Context)

---

## 🤔 What Is Pandas I/O?

ETL begins with reading data from a source (CSV, JSON, Excel, database, Parquet) and ends with writing to a destination. Choosing the right function and options directly affects performance, correctness, and memory usage.

> 💡 **Format Guide**: Use **Parquet** for analytical data lakes. Use **CSV** for system integration. Use **JSON** for APIs and config. Use **Excel** only when required by business users.

---

## 💻 Example 1: CSV — Best Practice Options

```python
import pandas as pd

# ---- READING CSV (all important options) ----
df = pd.read_csv(
    "sales_data.csv",
    sep           = ",",           # Change to "\t" for TSV, "|" for pipe-delimited
    encoding      = "utf-8",       # Always specify
    dtype         = {              # Pre-specify to avoid guessing
        "order_id":    "string",
        "customer_id": "int32",
        "amount":      "float32",
    },
    parse_dates   = ["order_date"],
    date_format   = "%Y-%m-%d",    # Explicit format (faster than guessing)
    usecols       = ["order_id","customer_id","amount","order_date"],  # Only needed
    na_values     = ["N/A","NULL","","none","None","-"],               # Treat as NaN
    on_bad_lines  = "skip",        # Skip malformed rows (Pandas 1.3+)
    low_memory    = False,         # Prevents dtype inference warnings
)

# ---- WRITING CSV ----
df.to_csv(
    "output/cleaned_sales.csv",
    index        = False,          # Don't write row numbers
    encoding     = "utf-8",
    date_format  = "%Y-%m-%d",
    float_format = "%.2f",         # 2 decimal places
    na_rep       = "",             # Empty string for NaN
)

# Compressed (saves ~70% disk space)
df.to_csv("output/sales.csv.gz", index=False, compression="gzip")
```

---

## 💻 Example 2: Parquet — The ETL Standard

```python
import pandas as pd

# ---- WRITING Parquet ----
df.to_parquet(
    "output/sales.parquet",
    engine      = "pyarrow",      # or "fastparquet"
    index       = False,
    compression = "snappy",       # Fast compression (gzip for smallest files)
)

# ---- READING Parquet ----
df = pd.read_parquet("output/sales.parquet", engine="pyarrow")

# Column pruning: only read the columns you need (Parquet native feature!)
df_subset = pd.read_parquet("output/sales.parquet",
                             columns=["order_id", "amount"])

# ---- WHY PARQUET? ----
# Parquet vs CSV comparison:
# - 3-10x smaller file size
# - 10x faster read/write
# - Preserves dtypes (no re-parsing strings to numbers!)
# - Native support in Spark, BigQuery, Snowflake, etc.
```

---

## 💻 Example 3: JSON — Reading and Writing

```python
import pandas as pd, json

# ---- READING JSON ----
# orient="records" → [{col:val, ...}, {col:val, ...}] ← most common ETL format
df = pd.read_json("data.json", orient="records")

# JSON Lines / NDJSON (one JSON object per line — streaming friendly)
df = pd.read_json("data.jsonl", lines=True)

# ---- WRITING JSON ----
df.to_json("output.json",  orient="records", indent=2, date_format="iso")
df.to_json("output.jsonl", orient="records", lines=True)

# ---- Normalize nested JSON (from REST API) ----
with open("api_response.json") as f:
    raw = json.load(f)

df_flat = pd.json_normalize(
    raw["data"],                      # List of records
    record_path = ["orders"],         # Nested list to expand
    meta        = ["customer_id"],    # Parent fields to keep
    sep         = "_",                # Nested: order.amount → order_amount
)
```

---

## 💻 Example 4: Excel — Multi-Sheet Reports

```python
import pandas as pd

# ---- READING Excel ----
df = pd.read_excel("report.xlsx", sheet_name="Sales", skiprows=2)

# Read ALL sheets at once → dict of DataFrames
all_sheets = pd.read_excel("report.xlsx", sheet_name=None)
for sheet_name, sheet_df in all_sheets.items():
    print(f"Sheet: {sheet_name}, Rows: {len(sheet_df)}")

# Combine all sheets
df_all = pd.concat(all_sheets.values(), ignore_index=True)

# ---- WRITING Excel (multiple sheets) ----
with pd.ExcelWriter("etl_output.xlsx", engine="openpyxl") as writer:
    df_sales.to_excel(writer,    sheet_name="Sales",     index=False)
    df_customers.to_excel(writer, sheet_name="Customers", index=False)
    summary.to_excel(writer,     sheet_name="Summary",   index=False)
print("Written 3-sheet Excel file")
```

---

## 💻 Example 5: SQL — Read and Write with SQLAlchemy

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://user:pass@localhost/etl_db")

# ---- READING ----
df = pd.read_sql(
    "SELECT order_id, customer_id, amount FROM sales WHERE year = 2024",
    engine,
    parse_dates=["order_date"],
)

# Reading large tables in chunks (avoid OOM)
chunks = []
for chunk in pd.read_sql("SELECT * FROM large_table", engine, chunksize=50_000):
    chunks.append(chunk.dropna(subset=["order_id"]))
df = pd.concat(chunks, ignore_index=True)

# ---- WRITING ----
df_transformed.to_sql(
    "sales_transformed",
    engine,
    if_exists = "replace",   # "replace" = drop+create; "append" = incremental
    index     = False,
    method    = "multi",     # Batch inserts (much faster)
    chunksize = 10_000,
)
print(f"Loaded {len(df_transformed):,} rows")
```

---

## 💻 Example 6: Auto-Detect Format (Universal Loader)

```python
from pathlib import Path
import pandas as pd

def read_etl_source(filepath: str, **kwargs) -> pd.DataFrame:
    """Auto-detect file format and load into DataFrame."""
    ext = Path(filepath).suffix.lower()
    readers = {
        ".csv":     pd.read_csv,
        ".tsv":     lambda f, **kw: pd.read_csv(f, sep="\t", **kw),
        ".txt":     lambda f, **kw: pd.read_csv(f, sep="|",  **kw),
        ".json":    lambda f, **kw: pd.read_json(f, orient="records", **kw),
        ".jsonl":   lambda f, **kw: pd.read_json(f, lines=True, **kw),
        ".parquet": pd.read_parquet,
        ".xlsx":    pd.read_excel,
        ".xls":     pd.read_excel,
    }
    reader = readers.get(ext)
    if not reader:
        raise ValueError(f"Unsupported format: {ext}")
    df = reader(filepath, **kwargs)
    print(f"✅ Loaded {len(df):,} rows from {Path(filepath).name}")
    return df

# Works for any format:
df = read_etl_source("data.parquet")
df = read_etl_source("report.xlsx", sheet_name="Sales")
df = read_etl_source("orders.csv", encoding="utf-8")
```

---

## 🏭 ETL Use Cases Summary

| Format       | Best For                         | Key Options                             |
| ------------ | -------------------------------- | --------------------------------------- |
| CSV          | System interoperability          | `usecols`, `dtype`, `encoding`          |
| Parquet      | Data lakes, analytical pipelines | `columns` pruning, `snappy` compression |
| JSON / JSONL | APIs, event data                 | `orient="records"`, `lines=True`        |
| Excel        | Business user deliverables       | `ExcelWriter` for multi-sheet           |
| SQL          | DB extract/load                  | `chunksize`, `method="multi"`           |

---

## ⚠️ Common Mistakes

```python
# ❌ No dtype → slow parsing and possible wrong types
df = pd.read_csv("data.csv")   # Pandas guesses every column's type

# ✅ Pre-specify
df = pd.read_csv("data.csv", dtype={"id": "int32", "status": "string"})

# ❌ to_sql default method — row-by-row inserts (very slow!)
df.to_sql("target", engine, if_exists="append")  # N individual INSERTs!

# ✅ Batch insert
df.to_sql("target", engine, if_exists="append", method="multi", chunksize=5000)

# ❌ Missing newline="" in csv.writer on Windows
with open("out.csv", "w") as f:
    f.write("...\n")   # Extra blank rows on Windows!
# ✅
with open("out.csv", "w", newline="", encoding="utf-8") as f:
    f.write("...\n")
```
