# 📁 File I/O in Python (ETL Context)

---

## 📖 Explanation

File I/O is fundamental in ETL — reading source files (CSV, JSON, XML, Parquet) and writing output. Python provides rich built-in and library support for working with various file formats.

Key modules: `open()`, `csv`, `json`, `pathlib`, `shutil`, `os`, `glob`.

---

## 🧠 Must-Remember Points

- Always use `with open(...)` — automatically closes the file.
- File modes: `'r'` (read), `'w'` (write/overwrite), `'a'` (append), `'b'` (binary), `'x'` (create, fail if exists).
- Always specify `encoding='utf-8'` explicitly to avoid platform differences.
- Use `pathlib.Path` over `os.path` — more modern and readable.
- `csv.DictReader` reads rows as dicts; `csv.DictWriter` writes dicts as rows.
- `json.load(f)` reads from file; `json.loads(s)` reads from string.
- `json.dump(obj, f)` writes to file; `json.dumps(obj)` writes to string.
- Use `indent=2` in `json.dump()` for readable JSON output.
- `newline=''` is required for `csv.writer` on Windows to avoid extra blank lines.
- `pathlib.Path.glob()` is great for batch file processing.

---

## 💻 Code Examples

### 1️⃣ Reading and Writing Text Files

```python
# Write a text file
with open("output.txt", "w", encoding="utf-8") as f:
    f.write("Hello, ETL!\n")
    f.writelines(["Line 1\n", "Line 2\n", "Line 3\n"])

# Read entire file
with open("output.txt", "r", encoding="utf-8") as f:
    content = f.read()

# Read line by line (memory efficient for large files)
with open("output.txt", "r", encoding="utf-8") as f:
    for line in f:
        print(line.strip())

# Read all lines into a list
with open("output.txt", "r", encoding="utf-8") as f:
    lines = f.readlines()  # ['Hello, ETL!\n', 'Line 1\n', ...]
```

---

### 2️⃣ CSV File Handling

```python
import csv

# Write CSV
employees = [
    {"id": 1, "name": "Alice", "salary": 75000},
    {"id": 2, "name": "Bob", "salary": 60000},
    {"id": 3, "name": "Charlie", "salary": 55000},
]

with open("employees.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "salary"])
    writer.writeheader()
    writer.writerows(employees)

# Read CSV
with open("employees.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(dict(row))  # {'id': '1', 'name': 'Alice', 'salary': '75000'}

# Read CSV with custom delimiter (TSV, pipe-delimited, etc.)
with open("data.tsv", "r", encoding="utf-8") as f:
    reader = csv.reader(f, delimiter="\t")
    for row in reader:
        print(row)
```

---

### 3️⃣ ETL: Chunk-based CSV Processing (Large Files)

```python
import csv

def process_large_csv(filepath, chunk_size=1000):
    """Read and process a large CSV in memory-efficient chunks."""
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk  # Last partial chunk

for i, batch in enumerate(process_large_csv("sales_10M.csv", chunk_size=500)):
    print(f"Batch {i+1}: {len(batch)} rows")
    # transform and load each batch
```

---

### 4️⃣ JSON File Handling

```python
import json

# Write JSON
config = {
    "pipeline": "sales_etl",
    "source": {"type": "csv", "path": "/data/input/"},
    "target": {"type": "postgres", "table": "sales_fact"},
    "batch_size": 1000,
}

with open("etl_config.json", "w", encoding="utf-8") as f:
    json.dump(config, f, indent=2)

# Read JSON
with open("etl_config.json", "r", encoding="utf-8") as f:
    loaded_config = json.load(f)
print(loaded_config["pipeline"])  # sales_etl

# Read JSON Lines (ndjson) — one JSON object per line
def read_jsonl(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line.strip())

# Write JSON Lines
records = [{"id": 1, "val": 10}, {"id": 2, "val": 20}]
with open("data.jsonl", "w", encoding="utf-8") as f:
    for record in records:
        f.write(json.dumps(record) + "\n")
```

---

### 5️⃣ `pathlib` — Modern File Path Handling

```python
from pathlib import Path

# Create paths
data_dir = Path("/data/etl")
input_dir = data_dir / "input"
output_dir = data_dir / "output"

# Create directories
output_dir.mkdir(parents=True, exist_ok=True)

# Check existence
if input_dir.exists():
    print(f"Input dir exists: {input_dir}")

# File properties
p = Path("employees.csv")
print(p.name)       # employees.csv
print(p.stem)       # employees
print(p.suffix)     # .csv
print(p.parent)     # . (current dir)

# Glob — find all CSVs in a directory
for csv_file in Path("/data/input").glob("*.csv"):
    print(csv_file)

# Recursive glob
for json_file in Path("/data").rglob("*.json"):
    print(json_file)

# Read/write using pathlib
p = Path("notes.txt")
p.write_text("ETL notes", encoding="utf-8")
content = p.read_text(encoding="utf-8")
```

---

### 6️⃣ ETL: Batch File Processing with `glob` and `pathlib`

```python
from pathlib import Path
import csv
import json

def process_all_csvs(input_dir: str, output_path: str):
    """Read all CSVs in a folder, merge, and write to JSON."""
    all_records = []
    input_path = Path(input_dir)

    for csv_file in sorted(input_path.glob("*.csv")):
        print(f"Processing: {csv_file.name}")
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            records = list(reader)
            all_records.extend(records)
            print(f"  -> {len(records)} records loaded")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    print(f"Total {len(all_records)} records merged to {output_path}")

# process_all_csvs("/data/monthly_sales/", "/data/merged_sales.json")
```

---

### 7️⃣ `shutil` — File & Directory Operations in ETL

```python
import shutil
from pathlib import Path

# Copy a file
shutil.copy2("source.csv", "backup/source.csv")  # Preserves metadata

# Move (rename) file after processing (archive pattern)
shutil.move("input/sales.csv", "archive/sales_2024.csv")

# Delete directory tree (cleanup)
shutil.rmtree("/tmp/etl_staging", ignore_errors=True)

# Compress output directory
shutil.make_archive("output_backup", "zip", "/data/output")

# ETL Archive Pattern: move processed files
def archive_processed(source_dir, archive_dir):
    from datetime import datetime
    source = Path(source_dir)
    archive = Path(archive_dir)
    archive.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for f in source.glob("*.csv"):
        dest = archive / f"{f.stem}_{timestamp}{f.suffix}"
        shutil.move(str(f), str(dest))
        print(f"Archived: {f.name} -> {dest.name}")
```

---

### 8️⃣ Reading Parquet Files (ETL Standard Format)

```python
import pandas as pd

# Read Parquet (used in big data ETL — columnar, compressed)
df = pd.read_parquet("data.parquet", engine="pyarrow")

# Write Parquet
df.to_parquet("output.parquet", engine="pyarrow", index=False, compression="snappy")

# Read specific columns only (efficient for wide datasets)
df_subset = pd.read_parquet("data.parquet", columns=["id", "revenue", "date"])

# Partition-based reading (common in data lakes)
df_partition = pd.read_parquet("/data/sales/year=2024/month=01/")
```

---

## 🏭 ETL Use Cases

| Tool                    | ETL Use Case                                       |
| ----------------------- | -------------------------------------------------- |
| `csv.DictReader/Writer` | Read/write flat files (standard ETL source/target) |
| `json.load/dump`        | Config files, API responses, nested data           |
| JSON Lines (ndjson)     | Streaming JSON — one record per line               |
| `pathlib.glob()`        | Batch-process multiple source files                |
| `shutil.move()`         | Archive/move files after ETL processing            |
| Parquet with pandas     | Efficient big-data ETL file format                 |
| Chunk-based CSV reading | Handle files larger than available RAM             |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Not specifying encoding — platform-dependent!
with open("data.csv") as f:  # Could be 'cp1252' on Windows!
    data = f.read()

# RIGHT: Always specify encoding
with open("data.csv", encoding="utf-8") as f:
    data = f.read()

# WRONG: Missing newline='' in csv.writer on Windows
with open("out.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows(data)  # Adds extra blank lines on Windows!

# RIGHT:
with open("out.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(data)

# WRONG: json.load vs json.loads confusion
data = json.load('{"key": "value"}')   # TypeError! load() needs a file object

# RIGHT:
data = json.loads('{"key": "value"}')  # loads() for strings
with open("file.json") as f:
    data = json.load(f)                # load() for files
```
