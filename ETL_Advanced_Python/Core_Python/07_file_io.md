# 📁 File I/O in Python (ETL Context)

---

## 🤔 What Is File I/O and Why Should You Care?

ETL pipelines live and breathe files:

- **Extracting** from `.csv`, `.json`, `.xlsx`, `.parquet` source files
- **Staging** intermediate data between pipeline steps
- **Loading** to output files or archiving processed inputs

Python gives you rich, built-in tools for every file type you'll encounter.

> 💡 **Key mindset**: Always use `with open(...)` — it guarantees the file is closed even if an error occurs. Opening without `with` and forgetting to call `.close()` leaks file handles and can corrupt files.

---

## 🧱 File Modes — What Do r, w, a Mean?

```python
# 'r' = READ   (file must exist; error if it doesn't)
with open("data.csv", "r") as f:
    content = f.read()

# 'w' = WRITE  (creates file; OVERWRITES if it exists — be careful!)
with open("output.csv", "w") as f:
    f.write("id,name\n1,Alice\n")

# 'a' = APPEND (creates file; adds to end if it exists)
with open("log.txt", "a") as f:
    f.write("2024-01-15: processed 500 rows\n")

# 'x' = CREATE (creates file; FAILS if file already exists — safe creation)
with open("new_file.csv", "x") as f:
    f.write("fresh start\n")

# 'b' suffix = binary mode (for images, zips, etc.)
with open("data.zip", "rb") as f:
    binary_data = f.read()
```

> 🔑 **Always specify `encoding="utf-8"`** to avoid platform-specific encoding differences. On Windows, the default is `cp1252`, which can silently corrupt non-ASCII characters.

---

## 💻 Example 1: Reading Text Files (Three Ways)

```python
# ---- Method 1: Read entire file at once (small files only) ----
with open("notes.txt", "r", encoding="utf-8") as f:
    entire_content = f.read()        # One big string
    print(type(entire_content))      # <class 'str'>

# ---- Method 2: Read all lines into a list (small-medium files) ----
with open("notes.txt", "r", encoding="utf-8") as f:
    lines = f.readlines()            # ['Line 1\n', 'Line 2\n', ...]
    # Note: each line includes the '\n' at the end!
    lines_clean = [line.strip() for line in lines]   # Remove '\n'

# ---- Method 3: Read line by line (BEST for large files — memory efficient) ----
with open("large_file.txt", "r", encoding="utf-8") as f:
    for line in f:            # File object is itself an iterator!
        clean = line.strip()
        if clean:             # Skip empty lines
            process(clean)    # Process one line at a time — never loads full file
```

---

## 💻 Example 2: CSV Files — The ETL Workhorse

CSV is by far the most common ETL file format. Python's `csv` module handles all the edge cases (quoted fields, commas inside values, different delimiters).

```python
import csv

# ---- WRITING a CSV ----
employees = [
    {"id": 1, "name": "Alice Smith",  "dept": "IT",  "salary": 75000},
    {"id": 2, "name": "Bob Jones",    "dept": "HR",  "salary": 60000},
    {"id": 3, "name": "Charlie Brown","dept": "FIN", "salary": 55000},
]

with open("employees.csv", "w", newline="", encoding="utf-8") as f:
    # DictWriter writes dictionaries as rows
    # fieldnames controls the column order
    writer = csv.DictWriter(f, fieldnames=["id", "name", "dept", "salary"])
    writer.writeheader()       # Writes the column name row
    writer.writerows(employees)  # Writes all data rows
# Note: newline="" is required on Windows to prevent extra blank lines!

# ---- READING a CSV ----
with open("employees.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)    # Reads each row as an OrderedDict
    for row in reader:
        # All values are strings by default — convert as needed
        print(f"{row['name']} earns ${int(row['salary']):,}")

# ---- Reading TSV, pipe-delimited, etc. ----
with open("data.tsv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")   # Tab-delimited
    for row in reader:
        print(row)

with open("data.txt", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="|")    # Pipe-delimited
    for row in reader:
        print(row)
```

---

## 💻 Example 3: Processing Large CSVs in Chunks (Generator Pattern)

```python
import csv

def read_csv_chunks(filepath, chunk_size=1000):
    """
    Generator that reads a large CSV in memory-efficient chunks.

    Instead of loading 10 million rows all at once, we process
    chunk_size rows at a time. Only chunk_size rows live in RAM
    at any moment — no matter how large the file is.

    Yields: list of dicts (one chunk at a time)
    """
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        chunk = []

        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                yield chunk     # Hand off this chunk to the caller
                chunk = []      # Clear and start the next chunk

        if chunk:               # Don't forget the last partial chunk!
            yield chunk


# Processing a 10-million-row file without running out of memory:
total_processed = 0
for batch_num, chunk in enumerate(read_csv_chunks("huge_sales.csv", chunk_size=5000)):
    # Transform each chunk
    valid_rows = [r for r in chunk if r.get("status") == "COMPLETE"]

    # Load chunk to database
    # db.insert_many("sales", valid_rows)

    total_processed += len(chunk)
    print(f"Batch {batch_num + 1}: {len(chunk)} rows processed (total: {total_processed:,})")
```

---

## 💻 Example 4: JSON Files

```python
import json

# ---- READING a JSON file ----
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)          # Parse JSON → Python dict/list
print(config["database"]["host"])

# ---- WRITING a JSON file ----
output_data = {
    "pipeline": "sales_etl",
    "records_processed": 15420,
    "status": "SUCCESS",
}
with open("run_report.json", "w", encoding="utf-8") as f:
    json.dump(output_data, f, indent=2)   # indent=2 makes it human-readable
# Produces:
# {
#   "pipeline": "sales_etl",
#   "records_processed": 15420,
#   "status": "SUCCESS"
# }

# ---- JSON Lines format (.jsonl) — one JSON object per line ----
# This format is great for streaming large datasets
records = [{"id": 1, "val": 10}, {"id": 2, "val": 20}]

# Write JSON Lines
with open("data.jsonl", "w", encoding="utf-8") as f:
    for record in records:
        f.write(json.dumps(record) + "\n")   # One dict per line

# Read JSON Lines
with open("data.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line.strip())    # Each line is parsed individually
        print(record)

# json.load(f) vs json.loads(string) — a common source of confusion!
data_from_file   = json.load(open("file.json"))   # load()  ← needs file object
data_from_string = json.loads('{"key": "value"}') # loads() ← needs string
```

---

## 💻 Example 5: `pathlib` — Modern Way to Work with File Paths

```python
from pathlib import Path

# ---- Creating paths the modern way ----
# Old way (string concatenation — error-prone on Windows vs Mac/Linux):
old_path = "/data/etl/" + "input/" + "sales.csv"

# New way with pathlib — works on all operating systems:
data_dir    = Path("/data/etl")
input_dir   = data_dir / "input"     # '/' operator joins paths safely!
output_dir  = data_dir / "output"
sales_file  = input_dir / "sales.csv"

print(sales_file)          # /data/etl/input/sales.csv
print(sales_file.name)     # sales.csv          ← just the filename
print(sales_file.stem)     # sales              ← filename without extension
print(sales_file.suffix)   # .csv               ← just the extension
print(sales_file.parent)   # /data/etl/input    ← parent directory

# ---- Create directories ----
output_dir.mkdir(parents=True, exist_ok=True)
# parents=True  → create intermediate dirs too (like 'mkdir -p')
# exist_ok=True → don't error if dir already exists

# ---- Check existence ----
if sales_file.exists():
    print("Source file found!")
else:
    raise FileNotFoundError(f"Expected: {sales_file}")

# ---- Read and write with pathlib ----
text = Path("notes.txt").read_text(encoding="utf-8")
Path("output.txt").write_text("Done!", encoding="utf-8")

# ---- Find files matching a pattern (glob) ----
# Find all CSVs in /data/input/
for csv_file in Path("/data/input").glob("*.csv"):
    print(f"Found: {csv_file.name}")

# Recursive search (search subdirectories too)
for json_file in Path("/data").rglob("*.json"):
    print(f"Found JSON: {json_file}")
```

---

## 💻 Example 6: Batch Processing Multiple Files

```python
from pathlib import Path
import csv, json

def merge_monthly_csvs(input_folder: str, output_path: str) -> None:
    """
    Read every CSV file in a folder and merge them into one JSON file.
    Common ETL pattern: collect monthly files → single merged dataset.
    """
    all_records = []
    input_path  = Path(input_folder)

    for csv_file in sorted(input_path.glob("*.csv")):  # sorted = consistent order
        print(f"📂 Reading: {csv_file.name}")
        with open(csv_file, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
            all_records.extend(rows)             # Add all rows to master list
            print(f"   → {len(rows)} rows loaded")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    print(f"\n✅ Merged {len(all_records):,} total records → {output_path}")

# merge_monthly_csvs("/data/monthly_sales/", "/data/merged_sales.json")
```

---

## 💻 Example 7: `shutil` — Move, Copy, Archive Files

```python
import shutil
from pathlib import Path
from datetime import datetime

# ---- ETL Archive Pattern ----
# After processing a file, move it to an archive folder with a timestamp.
# This prevents reprocessing and creates an audit trail.

def archive_processed_files(source_dir: str, archive_dir: str) -> None:
    """Move all CSVs from source to archive, adding a timestamp to the name."""
    source  = Path(source_dir)
    archive = Path(archive_dir)
    archive.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for f in source.glob("*.csv"):
        # New name: "sales.csv" → "sales_20240115_103045.csv"
        archived_name = f"{f.stem}_{timestamp}{f.suffix}"
        destination   = archive / archived_name

        shutil.move(str(f), str(destination))    # Move the file
        print(f"  📦 Archived: {f.name} → {archived_name}")

# After your ETL pipeline loads all CSVs:
# archive_processed_files("/data/input/", "/data/archive/")
```

---

## 🏭 ETL Use Cases Summary

| Tool              | Format | ETL Use Case                    |
| ----------------- | ------ | ------------------------------- |
| `csv.DictReader`  | CSV    | Read row-by-row as dicts        |
| `csv.DictWriter`  | CSV    | Write list of dicts to CSV      |
| Chunked generator | CSV    | Process files larger than RAM   |
| `json.load/dump`  | JSON   | Config files, API responses     |
| JSON Lines        | JSONL  | Streaming event data            |
| `pathlib.glob()`  | Any    | Batch-process a folder of files |
| `shutil.move()`   | Any    | Archive after processing        |

---

## 🧠 Must-Remember Points

| Point                           | Remember                                     |
| ------------------------------- | -------------------------------------------- |
| Use `with open(...)`            | Always — guarantees file is closed           |
| `encoding="utf-8"`              | Always specify — avoids platform differences |
| `newline=""`                    | Required for `csv.writer` on Windows         |
| `json.load()` vs `json.loads()` | `load()` for files; `loads()` for strings    |
| `pathlib` over `os.path`        | More readable; works on all OS               |
| Chunk-based reading             | For files > available RAM                    |

---

## ⚠️ Common Mistakes Beginners Make

```python
# ❌ MISTAKE 1: No encoding → platform-dependent corruption
with open("data.csv") as f:      # Default: 'cp1252' on Windows, 'utf-8' on Mac/Linux!
    data = f.read()

# ✅ FIX: Always declare encoding
with open("data.csv", encoding="utf-8") as f:
    data = f.read()


# ❌ MISTAKE 2: Missing newline="" in csv.writer on Windows
with open("out.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows(data)   # Extra blank line between each row on Windows!

# ✅ FIX:
with open("out.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(data)


# ❌ MISTAKE 3: Using 'w' mode instead of 'a' for log/append
with open("etl_log.txt", "w") as f:   # OVERWRITES the log file every run!
    f.write("Loaded 500 rows\n")

# ✅ FIX: Use 'a' to append
with open("etl_log.txt", "a") as f:   # Adds to existing log
    f.write("Loaded 500 rows\n")
```
