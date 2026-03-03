# 🧩 Comprehensions in Python (ETL Context)

---

## 📖 Explanation

Comprehensions provide a concise, readable, and often faster way to create lists, dictionaries, and sets from iterables. They combine a `for` loop and optional filtering into a single expression.

Types:

- **List Comprehension**: `[expression for item in iterable if condition]`
- **Dict Comprehension**: `{key: value for item in iterable if condition}`
- **Set Comprehension**: `{expression for item in iterable if condition}`
- **Generator Expression**: `(expression for item in iterable if condition)`

---

## 🧠 Must-Remember Points

- Comprehensions are more **Pythonic** and generally faster than equivalent `for` loops.
- **Generator expressions** are memory-efficient (lazy) — use for large datasets.
- Avoid **nested comprehensions** deeper than 2 levels — use regular loops for clarity.
- Dict comprehensions are excellent for transforming lookup tables.
- Set comprehensions automatically remove duplicates.
- Comprehensions should remain readable — if too complex, use a regular loop.
- You **cannot** use comprehensions to create tuples directly — use `tuple(gen_expr)`.

---

## 💻 Code Examples

### 1️⃣ List Comprehension

```python
# Classic for-loop
squares = []
for x in range(10):
    squares.append(x ** 2)

# Equivalent list comprehension — concise and faster
squares = [x ** 2 for x in range(10)]

# With condition (filter)
even_squares = [x ** 2 for x in range(10) if x % 2 == 0]
print(even_squares)  # [0, 4, 16, 36, 64]
```

---

### 2️⃣ ETL: Data Cleaning with List Comprehension

```python
raw_data = [
    {"name": " Alice ", "age": 25, "salary": "50000"},
    {"name": "BOB", "age": None, "salary": "60000"},
    {"name": "charlie", "age": 35, "salary": None},
]

# Clean: strip whitespace, lowercase, convert salary to int
cleaned_data = [
    {
        "name": row["name"].strip().title(),
        "age": row["age"],
        "salary": int(row["salary"]) if row["salary"] else 0
    }
    for row in raw_data
]
print(cleaned_data)
# [{'name': 'Alice', 'age': 25, 'salary': 50000}, ...]
```

---

### 3️⃣ ETL: Filtering Invalid Records

```python
records = [
    {"id": 1, "revenue": 1500, "region": "NORTH"},
    {"id": 2, "revenue": -50, "region": "SOUTH"},  # Invalid
    {"id": 3, "revenue": 2000, "region": "EAST"},
    {"id": 4, "revenue": None, "region": "WEST"},  # Invalid
]

# Filter: keep only valid records with positive, non-null revenue
valid_records = [
    rec for rec in records
    if rec["revenue"] is not None and rec["revenue"] > 0
]
print(valid_records)
# [{'id': 1, ...}, {'id': 3, ...}]
```

---

### 4️⃣ Dict Comprehension

```python
# Create a mapping from ID to name
employees = [
    {"id": 101, "name": "Alice"},
    {"id": 102, "name": "Bob"},
    {"id": 103, "name": "Charlie"},
]

# Dict comprehension: {id: name}
id_to_name = {emp["id"]: emp["name"] for emp in employees}
print(id_to_name)  # {101: 'Alice', 102: 'Bob', 103: 'Charlie'}

# Invert a dictionary
name_to_id = {v: k for k, v in id_to_name.items()}
print(name_to_id)  # {'Alice': 101, 'Bob': 102, 'Charlie': 103}
```

---

### 5️⃣ ETL: Dict Comprehension for Column Renaming / Mapping

```python
import pandas as pd

# Column rename mapping
column_mapping = {
    "emp_id": "employee_id",
    "emp_nm": "employee_name",
    "sal_amt": "salary_amount",
    "dept_cd": "department_code",
}

df = pd.DataFrame({
    "emp_id": [1, 2, 3],
    "emp_nm": ["Alice", "Bob", "Charlie"],
    "sal_amt": [50000, 60000, 55000],
    "dept_cd": ["HR", "IT", "FIN"],
})

df_renamed = df.rename(columns=column_mapping)
print(df_renamed.columns.tolist())
# ['employee_id', 'employee_name', 'salary_amount', 'department_code']

# Normalization: uppercase all column names using dict comprehension
normalized = {col.lower(): df[col] for col in df.columns}
```

---

### 6️⃣ Set Comprehension (Deduplication)

```python
# Get unique values from a column
data = [{"country": "US"}, {"country": "UK"}, {"country": "US"}, {"country": "IN"}]

unique_countries = {row["country"] for row in data}
print(unique_countries)  # {'US', 'UK', 'IN'} — order not guaranteed

# Check if all required columns are present
required_cols = {"id", "name", "age", "salary"}
actual_cols = {"id", "name", "salary"}
missing_cols = required_cols - actual_cols
print(f"Missing columns: {missing_cols}")  # Missing columns: {'age'}
```

---

### 7️⃣ Nested Comprehension (ETL: Flatten a Nested Structure)

```python
# Nested data (e.g., from a JSON API response)
departments = [
    {"dept": "HR", "employees": ["Alice", "Bob"]},
    {"dept": "IT", "employees": ["Charlie", "Dave"]},
    {"dept": "FIN", "employees": ["Eve"]},
]

# Flatten: list of all employees
all_employees = [emp for dept in departments for emp in dept["employees"]]
print(all_employees)  # ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve']

# Flatten with dept info
employee_dept = [
    {"emp": emp, "dept": dept["dept"]}
    for dept in departments
    for emp in dept["employees"]
]
```

---

### 8️⃣ Generator Expression (Memory-Efficient for ETL)

```python
import csv

# Sum a specific column without loading entire file
with open("large_sales.csv") as f:
    reader = csv.DictReader(f)
    # Generator expression — doesn't load all rows into memory
    total_revenue = sum(
        float(row["revenue"])
        for row in reader
        if row["status"] == "COMPLETE"
    )
print(f"Total Revenue: ${total_revenue:,.2f}")
```

---

## 🏭 ETL Use Cases

| Comprehension Type   | ETL Use Case                              |
| -------------------- | ----------------------------------------- |
| List comprehension   | Clean, filter, transform rows             |
| Dict comprehension   | Build lookup maps, rename columns         |
| Set comprehension    | Find unique values, validate columns      |
| Generator expression | Process large files without memory issues |
| Nested comprehension | Flatten nested JSON/API responses         |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Overly complex nested comprehension — unreadable
result = [x for row in data for x in row["items"] if x["qty"] > 0 if x["price"] > 10]

# BETTER: Break into steps
all_items = [x for row in data for x in row["items"]]
valid_items = [x for x in all_items if x["qty"] > 0 and x["price"] > 10]

# WRONG: Using list comprehension for large data (memory)
all_data = [process(row) for row in read_100_million_rows()]  # 💥 OOM!

# RIGHT: Use generator expression
result = sum(process(row)["value"] for row in read_100_million_rows())
```
