# 🧩 Comprehensions in Python (ETL Context)

---

## 🤔 What Are Comprehensions and Why Should You Care?

Every ETL pipeline involves transforming lists of data: filtering bad records, converting data types, renaming fields, flattening nested structures.

Without comprehensions, you'd write this:

```python
# Old way — verbose
squares = []
for x in range(10):
    squares.append(x ** 2)
```

With comprehensions, you write this:

```python
# With comprehension — concise and clear
squares = [x ** 2 for x in range(10)]
```

**Comprehensions** are a concise, readable, and typically faster way to create new sequences (lists, dicts, sets) by transforming or filtering existing ones — all in a single expression.

> 💡 **Real-world analogy**: Imagine you have a fruit basket (input data). A comprehension is like a conveyor belt with a filter: fruits go through, bad ones are removed, good ones are processed (e.g., peeled), and the result drops into a new basket. All in one smooth motion.

---

## 🧱 Types of Comprehensions

| Type                 | Syntax                                    | Output             |
| -------------------- | ----------------------------------------- | ------------------ |
| List comprehension   | `[expr for item in iterable if cond]`     | `list`             |
| Dict comprehension   | `{key: val for item in iterable if cond}` | `dict`             |
| Set comprehension    | `{expr for item in iterable if cond}`     | `set`              |
| Generator expression | `(expr for item in iterable if cond)`     | `generator` (lazy) |

**The general formula:**

```
[  what to produce   for  each item  in  source   if  optional condition  ]
```

---

## 💻 Part 1: List Comprehensions

### Basic Structure

```python
# Structure:   [EXPRESSION   for VARIABLE in ITERABLE   if CONDITION]
#               ↑produce       ↑for each     ↑source       ↑filter (optional)

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Example 1: Produce — apply a transformation
doubles = [x * 2 for x in numbers]
print(doubles)   # [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
#                   ↑ each number is doubled

# Example 2: Filter — keep only some items
evens = [x for x in numbers if x % 2 == 0]
print(evens)     # [2, 4, 6, 8, 10]
#                   ↑ keep only even numbers

# Example 3: Transform AND filter together
even_doubles = [x * 2 for x in numbers if x % 2 == 0]
print(even_doubles)  # [4, 8, 12, 16, 20]
#                       ↑ double only the even numbers
```

### ETL: Clean Raw Data Rows

```python
# Imagine you received this raw data from a source system
raw_data = [
    {"name": " Alice Smith ",   "age": 25, "salary": "50000",  "dept": "IT"},
    {"name": "BOB JONES",       "age": 30, "salary": "60000",  "dept": "HR"},
    {"name": " charlie brown ", "age": 35, "salary": "badval", "dept": "FIN"},
    {"name": None,              "age": 28, "salary": "55000",  "dept": "IT"},
]

# STEP 1: Filter out invalid records (name is None)
valid_records = [row for row in raw_data if row["name"] is not None]
print(f"Valid records: {len(valid_records)}")   # 3

# STEP 2: Clean and normalize the valid records
cleaned = [
    {
        "name":    row["name"].strip().title(),              # Remove spaces, proper case
        "age":     row["age"],
        "salary":  int(row["salary"]) if row["salary"].isdigit() else 0,  # Safe conversion
        "dept":    row["dept"],
    }
    for row in valid_records                                  # Iterate over source
    if row["name"].strip() != ""                             # Skip whitespace-only names
]

for record in cleaned:
    print(record)
# {'name': 'Alice Smith', 'age': 25, 'salary': 50000, 'dept': 'IT'}
# {'name': 'Bob Jones',    'age': 30, 'salary': 60000, 'dept': 'HR'}
# {'name': 'Charlie Brown', 'age': 35, 'salary': 0, 'dept': 'FIN'}  ← salary=0 (bad value)
```

### ETL: Filter Bad Records for Quarantine

```python
records = [
    {"id": 1, "status": "COMPLETE",  "amount":  1500.0},
    {"id": 2, "status": "PENDING",   "amount":   200.0},
    {"id": 3, "status": "COMPLETE",  "amount":  -100.0},   # Invalid: negative amount
    {"id": 4, "status": "FAILED",    "amount":   300.0},
    {"id": 5, "status": "COMPLETE",  "amount":  None},     # Invalid: null amount
]

# Valid: COMPLETE status AND positive, non-null amount
valid = [
    r for r in records
    if r["status"] == "COMPLETE"
    and r["amount"] is not None
    and r["amount"] > 0
]

# Invalid: everything else
invalid = [
    r for r in records
    if not (r["status"] == "COMPLETE" and r["amount"] is not None and r["amount"] > 0)
]

print(f"✅ Valid:   {len(valid)} records")    # 1 record
print(f"⚠️  Invalid: {len(invalid)} records")  # 4 records
```

---

## 💻 Part 2: Dict Comprehensions

```python
# Basic: create a dict from a list
names = ["alice", "bob", "charlie"]
name_lengths = {name: len(name) for name in names}
print(name_lengths)   # {'alice': 5, 'bob': 3, 'charlie': 7}

# ETL: Build an ID-to-Name lookup map (very common for JOIN-like operations)
employees = [
    {"emp_id": 101, "name": "Alice Smith",  "dept": "IT"},
    {"emp_id": 102, "name": "Bob Jones",    "dept": "HR"},
    {"emp_id": 103, "name": "Charlie Brown","dept": "FIN"},
]

# Create a fast lookup dictionary: {emp_id: name}
# Instead of searching the whole list every time, look up in O(1)!
id_to_name = {emp["emp_id"]: emp["name"] for emp in employees}
print(id_to_name)
# {101: 'Alice Smith', 102: 'Bob Jones', 103: 'Charlie Brown'}

# Use the lookup to enrich another table
orders = [
    {"order_id": "ORD-001", "emp_id": 101, "amount": 500},
    {"order_id": "ORD-002", "emp_id": 103, "amount": 200},
]
enriched_orders = [
    {**order, "emp_name": id_to_name.get(order["emp_id"], "Unknown")}
    for order in orders
]
print(enriched_orders)
# [{'order_id': 'ORD-001', 'emp_id': 101, 'amount': 500, 'emp_name': 'Alice Smith'}, ...]


# ETL: Rename columns (e.g., normalize legacy column names)
column_rename_map = {
    "emp_id":   "employee_id",
    "emp_nm":   "employee_name",
    "sal_amt":  "salary_amount",
    "dept_cd":  "department_code",
}

raw_row = {"emp_id": 1, "emp_nm": "Alice", "sal_amt": 75000, "dept_cd": "IT"}
# Renamed: {new_name: old_value for each (old, new) pair}
renamed_row = {
    column_rename_map.get(k, k): v   # Use new name if mapped, else keep original
    for k, v in raw_row.items()
}
print(renamed_row)
# {'employee_id': 1, 'employee_name': 'Alice', 'salary_amount': 75000, 'department_code': 'IT'}

# Invert a dictionary (swap keys and values)
inverted = {v: k for k, v in column_rename_map.items()}
print(inverted)   # {'employee_id': 'emp_id', 'employee_name': 'emp_nm', ...}
```

---

## 💻 Part 3: Set Comprehensions (Deduplication & Validation)

```python
# A set stores only UNIQUE values — duplicates are automatically removed.

data = [{"region": "US"}, {"region": "UK"}, {"region": "US"}, {"region": "IN"}, {"region": "UK"}]

# Set comprehension: unique regions
unique_regions = {row["region"] for row in data}
print(unique_regions)   # {'US', 'UK', 'IN'}  — order is NOT guaranteed in sets

# ETL: Validate that all expected columns are present in the source data
required_columns = {"id", "name", "email", "created_date", "status"}
actual_columns   = {"id", "name", "email", "status"}                   # 'created_date' is missing!

missing_columns  = required_columns - actual_columns   # Set subtraction
extra_columns    = actual_columns - required_columns   # Columns not expected

if missing_columns:
    raise ValueError(f"❌ Source file is missing required columns: {missing_columns}")
print("✅ All required columns are present!")

# ETL: Find duplicate IDs in source data
source_ids = [1, 2, 3, 2, 4, 1, 5]   # IDs 1 and 2 appear twice!
unique_ids = set(source_ids)           # {1, 2, 3, 4, 5}
duplicates = {x for x in source_ids if source_ids.count(x) > 1}
print(f"Duplicate IDs found: {duplicates}")   # {1, 2}
```

---

## 💻 Part 4: Generator Expressions (Memory-Efficient)

```python
# Generator expressions look like list comprehensions but use () instead of []
# CRITICAL DIFFERENCE: They are LAZY — they compute values one at a time, on demand.

# List comprehension: computes ALL values immediately, stores ALL in memory
squares_list = [x**2 for x in range(1_000_000)]   # 8 MB stored in RAM right now!

# Generator expression: stores almost nothing — computes one value at a time
squares_gen  = (x**2 for x in range(1_000_000))   # ~112 bytes — barely anything!

# But you can iterate over both the same way
for val in squares_gen:
    pass   # Each value is computed, used, then discarded


# ETL: Sum values from a large file without loading it all into memory
import csv

with open("transactions.csv") as f:
    reader = csv.DictReader(f)
    # This generator processes ONE row at a time — no matter the file size!
    total = sum(
        float(row["amount"])           # Convert this row's amount to float
        for row in reader              # For each row in the file
        if row["status"] == "PAID"     # Only include paid transactions
    )
print(f"Total paid: ${total:,.2f}")


# Generator expressions work with all functions that accept iterables:
data = [3, 1, 4, 1, 5, 9, 2, 6, 5]
print(max(x**2 for x in data if x > 0))   # 81  — max of squares of positive numbers
print(any(x > 8 for x in data))            # True — is any value > 8?
print(all(x > 0 for x in data))            # True — are all values positive?
print(list(x*2 for x in data if x > 4))   # [10, 18, 12, 10] — filtered doubles
```

---

## 💻 Part 5: Nested Comprehensions (Flatten Nested Data)

```python
# Nested data from a JSON API response
departments = [
    {"dept": "Engineering", "employees": ["Alice", "Bob",   "Charlie"]},
    {"dept": "Marketing",   "employees": ["Dave",  "Eve"]},
    {"dept": "Finance",     "employees": ["Frank"]},
]

# APPROACH 1: Double loop comprehension (read inner loop first, outer second)
# [what_to_produce  for outer_item in outer_list  for inner_item in inner_list]
all_employees = [
    emp                            # Produce each employee name
    for dept in departments        # For each department
    for emp in dept["employees"]   # For each employee IN that department
]
print(all_employees)
# ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve', 'Frank']

# APPROACH 2: Flatten with dept info included
employee_records = [
    {"name": emp, "department": dept["dept"]}   # Build a record for each person
    for dept in departments
    for emp in dept["employees"]
]
for r in employee_records:
    print(r)
# {'name': 'Alice', 'department': 'Engineering'}
# {'name': 'Bob',   'department': 'Engineering'}
# ...

# When nesting gets complex, use a regular loop for clarity:
# (Code that others can read is always better than code that's technically shorter!)
result = []
for dept in departments:
    for emp in dept["employees"]:
        if len(emp) > 4:              # Only long-ish names
            result.append({
                "name": emp.upper(),
                "dept": dept["dept"],
            })
```

---

## 🏭 ETL Use Cases Summary

| Comprehension        | ETL Use Case                      | Benefit                 |
| -------------------- | --------------------------------- | ----------------------- |
| List comprehension   | Clean, filter, transform rows     | Concise, readable, fast |
| Dict comprehension   | Build lookup maps; rename columns | O(1) lookup for joins   |
| Set comprehension    | Unique values; validate columns   | Auto-deduplication      |
| Generator expression | Process large files row by row    | Memory efficient        |
| Nested comprehension | Flatten nested JSON/API responses | One-pass unnesting      |

---

## 🧠 Must-Remember Points

| Concept                    | What to Remember                                              |
| -------------------------- | ------------------------------------------------------------- |
| `[expr for x in iterable]` | Core list comprehension — always this shape                   |
| `if condition`             | Optional filter — placed at the END                           |
| `{k: v for ...}`           | Dict comprehension — must produce a key AND a value           |
| `{expr for ...}`           | Set comprehension — curly braces like dict but no colon       |
| `(expr for ...)`           | Generator expression — lazy, memory efficient                 |
| Readability rule           | If it takes >2 lines or has >2 conditions, use a regular loop |

---

## ⚠️ Common Mistakes Beginners Make

```python
# ❌ MISTAKE 1: Confusing {} for set vs dict comprehension
d = {x: x**2 for x in range(5)}   # Dict  ← has key: value
s = {x**2 for x in range(5)}      # Set   ← no colon, just values

# ❌ MISTAKE 2: Using list comprehension for large data (memory issue)
all_rows = [process(row) for row in read_10_million_rows()]  # 💥 OOM crash!
# ✅ FIX: Use generator expression
total = sum(process(row)["amount"] for row in read_10_million_rows())

# ❌ MISTAKE 3: Overly complex nested comprehension (unreadable!)
result = [x for row in data for x in row["items"] if x["qty"] > 0 if x["price"] > 10]
# ✅ FIX: Break into steps with descriptive names
all_items   = [x for row in data for x in row["items"]]
valid_items = [x for x in all_items if x["qty"] > 0 and x["price"] > 10]

# ❌ MISTAKE 4: Forgetting that set order is not guaranteed
ordered   = [x for x in ["c", "a", "b"]]   # ['c', 'a', 'b'] — order preserved
unordered = {x for x in ["c", "a", "b"]}   # Could be {'a', 'b', 'c'} — no order!
```
