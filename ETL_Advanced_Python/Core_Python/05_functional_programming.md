# ⚙️ Functional Programming in Python (ETL Context)

---

## 🤔 What Is Functional Programming?

Functional programming (FP) is a style of writing code where you:

1. **Express transformations** using functions instead of step-by-step loops
2. **Avoid changing existing data** — create new results from old ones
3. **Pass functions as values** — use them as arguments or return values

Python is not a purely functional language, but it offers excellent FP tools:
`lambda`, `map()`, `filter()`, `reduce()`, and `functools.partial`.

In ETL, FP tools help you write **clear, declarative transformation pipelines** — code that says _what_ to do rather than _how_ to do it.

> 💡 **Analogy**: Think of your data as water in a river. FP tools are like filters, pipes, and valves that shape the water's flow. You describe the desired shape, not the exact steps of how to channel each water molecule.

---

## 🧱 Part 1: `lambda` — Anonymous (Inline) Functions

### What Is a Lambda?

A `lambda` is a **small, one-line function** without a name. You use it when you need a simple function just once — like as an argument to `sorted()`, `map()`, or `filter()`.

```python
# Regular function
def double(x):
    return x * 2

# Equivalent lambda (anonymous function)
double = lambda x: x * 2
#                ↑    ↑
#           parameter  expression to return


# Syntax:  lambda parameters : expression_to_return
#          ← always one expression, no 'return' keyword needed →

# Multi-parameter lambda
add = lambda x, y: x + y
print(add(3, 4))    # 7

# With condition (ternary expression)
label = lambda score: "PASS" if score >= 60 else "FAIL"
print(label(75))    # PASS
print(label(45))    # FAIL
```

### ETL: Sorting with `lambda`

```python
# Lambda is most commonly used as the 'key' argument in sorting
employees = [
    {"name": "Charlie", "salary": 60000, "dept": "IT",  "years": 5},
    {"name": "Alice",   "salary": 75000, "dept": "HR",  "years": 3},
    {"name": "Bob",     "salary": 50000, "dept": "IT",  "years": 7},
    {"name": "Dave",    "salary": 75000, "dept": "FIN", "years": 2},
]

# Sort by salary (ascending)
by_salary = sorted(employees, key=lambda e: e["salary"])
print([e["name"] for e in by_salary])   # ['Bob', 'Charlie', 'Alice', 'Dave']

# Sort by salary descending
by_salary_desc = sorted(employees, key=lambda e: e["salary"], reverse=True)

# Sort by DEPARTMENT, then by SALARY descending within each department
# Tuple key: first element sorts first; negative salary flips the order
by_dept_salary = sorted(employees, key=lambda e: (e["dept"], -e["salary"]))
print([(e["dept"], e["name"], e["salary"]) for e in by_dept_salary])
# [('FIN', 'Dave', 75000), ('HR', 'Alice', 75000), ('IT', 'Charlie', 60000), ('IT', 'Bob', 50000)]
```

---

## 🧱 Part 2: `map()` — Apply a Function to Every Item

### What Is `map()`?

`map(function, iterable)` applies a function to **every item** in a sequence and returns a new iterator with the results.

```
map(function, iterable)
     ↑            ↑
  what to do   data source

Returns a 'map object' (lazy iterator) — wrap with list() to see values
```

```python
# Basic example: double every number
numbers = [1, 2, 3, 4, 5]
doubled = list(map(lambda x: x * 2, numbers))
print(doubled)   # [2, 4, 6, 8, 10]

# Using a named function instead of lambda (cleaner for complex logic)
def to_celsius(fahrenheit):
    return (fahrenheit - 32) * 5 / 9

temps_f = [32, 68, 98.6, 212]
temps_c = list(map(to_celsius, temps_f))
print(temps_c)   # [0.0, 20.0, 37.0, 100.0]

# Built-in functions work too
strings = ["10", "20", "30", "40"]
numbers = list(map(int, strings))   # int() applied to each string
print(numbers)   # [10, 20, 30, 40]
```

### ETL: Cleaning Records with `map()`

```python
def clean_record(record):
    """
    Clean and normalize a single raw record.
    Applied to each record individually via map().
    """
    return {
        "id":       int(record["id"]),                    # string → integer
        "name":     record["name"].strip().title(),        # " alice " → "Alice"
        "revenue":  float(record["revenue"] or 0),        # None → 0.0
        "region":   record["region"].strip().upper(),     # "north " → "NORTH"
    }

raw_records = [
    {"id": "101", "name": "  alice ", "revenue": "1500.50", "region": "north"},
    {"id": "102", "name": "BOB",      "revenue": None,       "region": "SOUTH "},
    {"id": "103", "name": " charlie", "revenue": "3000",    "region": " east"},
]

# map() applies clean_record to EVERY item in raw_records
cleaned = list(map(clean_record, raw_records))

for r in cleaned:
    print(r)
# {'id': 101, 'name': 'Alice', 'revenue': 1500.5, 'region': 'NORTH'}
# {'id': 102, 'name': 'Bob',   'revenue': 0.0,    'region': 'SOUTH'}
# {'id': 103, 'name': 'Charlie', 'revenue': 3000.0, 'region': 'EAST'}
```

---

## 🧱 Part 3: `filter()` — Keep Only Matching Items

### What Is `filter()`?

`filter(function, iterable)` applies a function that returns `True`/`False` to each item. Only items where the function returns `True` are kept.

```
filter(function, iterable)
        ↑            ↑
  returns True/False  data source

Keeps items where function returns True
Returns a 'filter object' (lazy iterator) — wrap with list() to see values
```

```python
# Basic example: keep only positive numbers
numbers = [3, -1, 4, -1, 5, -9, 2, -6]
positives = list(filter(lambda x: x > 0, numbers))
print(positives)   # [3, 4, 5, 2]

# Using a named function
def is_valid_email(email):
    return "@" in email and "." in email

emails = ["alice@example.com", "bad-email", "bob@company.org", "not-valid"]
valid_emails = list(filter(is_valid_email, emails))
print(valid_emails)   # ['alice@example.com', 'bob@company.org']

# filter(None, iterable) removes ALL falsy values (None, "", 0, False, [])
messy_data = ["Alice", "", None, "Bob", 0, "Charlie", False]
clean_data = list(filter(None, messy_data))
print(clean_data)     # ['Alice', 'Bob', 'Charlie']
```

### ETL: Filtering Invalid Records

```python
records = [
    {"id": 1, "status": "COMPLETE",  "amount":  500.0},
    {"id": 2, "status": "PENDING",   "amount":  200.0},
    {"id": 3, "status": "COMPLETE",  "amount": -100.0},   # ← negative (invalid)
    {"id": 4, "status": "FAILED",    "amount":  300.0},
    {"id": 5, "status": "COMPLETE",  "amount":  None},    # ← null (invalid)
]

def is_valid_record(rec):
    """Return True only for records we want to process."""
    return (
        rec["status"] == "COMPLETE"    # Must be completed
        and rec["amount"] is not None  # Must have an amount
        and rec["amount"] > 0          # Amount must be positive
    )

valid   = list(filter(is_valid_record, records))
invalid = list(filter(lambda r: not is_valid_record(r), records))

print(f"✅ Valid:   {len(valid)}")    # 1
print(f"⚠️  Invalid: {len(invalid)}")  # 4
```

---

## 🧱 Part 4: `reduce()` — Accumulate to a Single Value

### What Is `reduce()`?

`reduce(function, iterable)` applies a function **cumulatively** to items, reducing the sequence to a single value.

```
reduce(function, [a, b, c, d])
Internally computes: function(function(function(a, b), c), d)

Step 1: result = function(a, b)
Step 2: result = function(result, c)
Step 3: result = function(result, d)
Final: result
```

```python
from functools import reduce   # Not built-in in Python 3 — must import

# Basic: sum a list (like the built-in sum())
numbers = [1, 2, 3, 4, 5]
total = reduce(lambda acc, x: acc + x, numbers)
# Step 1: acc=1, x=2 → result=3
# Step 2: acc=3, x=3 → result=6
# Step 3: acc=6, x=4 → result=10
# Step 4: acc=10, x=5 → result=15
print(total)   # 15

# Product: multiply all numbers together
product = reduce(lambda acc, x: acc * x, numbers)
print(product)   # 120   (1 × 2 × 3 × 4 × 5)

# Find the maximum with reduce
max_val = reduce(lambda a, b: a if a > b else b, [3, 1, 4, 1, 5, 9, 2, 6])
print(max_val)   # 9
```

### ETL: Merge Multiple Config Dicts

```python
from functools import reduce

# In real ETL, config often comes from multiple sources:
# environment variables, config files, command-line args
# Later sources override earlier ones.
default_config = {"host": "localhost",  "port": 5432, "schema": "public", "timeout": 30}
env_config     = {"host": "prod-server", "port": 5433}               # Override host and port
override_args  = {"schema": "etl_staging"}                            # Override schema

all_configs = [default_config, env_config, override_args]

# reduce merges them left-to-right; later dicts override earlier ones
# {**a, **b} creates a NEW dict with all keys from a, then all keys from b (b wins duplicates)
merged = reduce(lambda acc, d: {**acc, **d}, all_configs)
print(merged)
# {'host': 'prod-server', 'port': 5433, 'schema': 'etl_staging', 'timeout': 30}
# ↑ 'host' and 'port' from env_config, 'schema' from override_args, 'timeout' from default
```

---

## 🧱 Part 5: `functools.partial` — Pre-fill Function Arguments

### What Is `partial()`?

`partial(function, arg1, arg2, ...)` creates a **new function** with some arguments already filled in. Like a template version of a function.

```python
from functools import partial

# Generic function with several parameters
def read_csv_file(filepath, delimiter=",", encoding="utf-8", header=True):
    # ... reads and returns a DataFrame ...
    print(f"Reading {filepath} with delimiter='{delimiter}', encoding={encoding}")

# Create specialized versions with some arguments pre-filled
read_tsv   = partial(read_csv_file, delimiter="\t")           # Pre-fills delimiter=tab
read_pipe  = partial(read_csv_file, delimiter="|")            # Pre-fills delimiter=pipe
read_latin = partial(read_csv_file, encoding="latin-1")       # Pre-fills encoding

# Now you can call them as if they only take one argument (the filepath)
read_tsv("employees.tsv")    # delimiter="\t" already set!
read_pipe("orders.txt")       # delimiter="|"  already set!

# ETL: Parameterized validation function
def validate_value(value, min_val, max_val, required):
    if required and value is None:
        return False
    if value is not None and not (min_val <= value <= max_val):
        return False
    return True

# Create specialized validators
validate_age     = partial(validate_value, min_val=0,   max_val=150, required=True)
validate_salary  = partial(validate_value, min_val=0,   max_val=10_000_000, required=False)
validate_score   = partial(validate_value, min_val=0.0, max_val=10.0, required=True)

print(validate_age(25))      # True
print(validate_age(200))     # False (> 150)
print(validate_salary(None)) # True (not required)
print(validate_salary(-100)) # False (< 0)
```

---

## 🧱 Part 6: Function Composition — Building Pipelines

You can chain functions together so the output of one becomes the input of the next.

```python
from functools import reduce

def compose(*functions):
    """
    Compose multiple functions into one.
    compose(f, g, h)(x)  is equivalent to  f(g(h(x)))
    (think: the rightmost function runs first)
    """
    return reduce(lambda f, g: lambda x: f(g(x)), functions)

# Individual transformation functions
remove_whitespace = lambda s: s.strip()
to_lowercase      = lambda s: s.lower()
replace_spaces    = lambda s: s.replace(" ", "_")

# Compose them into one pipeline function
normalize_key = compose(replace_spaces, to_lowercase, remove_whitespace)
# Execution order: strip → lowercase → replace spaces
# "  Employee Name " → "employee name" → "employee_name"

raw_column_names = ["  Employee Name ", "  Dept Code  ", " Hire Date ", "SALARY AMOUNT"]
normalized = list(map(normalize_key, raw_column_names))
print(normalized)
# ['employee_name', 'dept_code', 'hire_date', 'salary_amount']
```

---

## 🏭 ETL Use Cases Summary

| FP Tool     | ETL Use Case                           | Example                                  |
| ----------- | -------------------------------------- | ---------------------------------------- |
| `lambda`    | Inline sort keys and simple transforms | `sorted(data, key=lambda r: r["date"])`  |
| `map()`     | Apply same transform to all records    | Convert types, clean all rows            |
| `filter()`  | Remove invalid/null records            | Keep only `status == "COMPLETE"`         |
| `reduce()`  | Aggregate or merge                     | Sum totals, merge config dicts           |
| `partial()` | Create specialized function versions   | `read_tsv = partial(read_csv, sep="\t")` |
| Composition | Build multi-step transform pipelines   | normalize = strip → lower → replace      |

---

## 🧠 Must-Remember Points

| Concept                       | What to Remember                                                                |
| ----------------------------- | ------------------------------------------------------------------------------- |
| `lambda x: expression`        | One-liner function; no `return` needed; one expression only                     |
| `map()` returns iterator      | Always wrap with `list()` if you need to see/index the result                   |
| `filter()` keeps `True` items | The function must return a boolean (or truthy/falsy)                            |
| `reduce()` needs import       | `from functools import reduce`                                                  |
| `partial()`                   | Creates a new function with some arguments pre-filled                           |
| Prefer comprehensions         | For simple cases, list comprehensions are often more readable than `map/filter` |

---

## ⚠️ Common Mistakes Beginners Make

```python
from functools import reduce

# ❌ MISTAKE 1: Forgetting that map/filter return iterators (not lists!)
result = map(str.upper, ["hello", "world"])
print(result)       # <map object at 0x...> — not what you wanted!
print(result[0])    # TypeError: 'map' object is not subscriptable

# ✅ FIX: Wrap with list() when you need a concrete list
result = list(map(str.upper, ["hello", "world"]))
print(result)   # ['HELLO', 'WORLD']


# ❌ MISTAKE 2: Using map/filter when a comprehension is clearer
result = list(map(lambda x: x * 2, filter(lambda x: x > 0, [-1, 2, -3, 4])))
# ✅ BETTER: Comprehension reads left-to-right, more Pythonic
result = [x * 2 for x in [-1, 2, -3, 4] if x > 0]
# [4, 8]


# ❌ MISTAKE 3: Complex lambda (hard to read / debug)
process = lambda r: {**r, "total": r["price"] * r["qty"], "status": "active" if r["qty"] > 0 else "inactive"}
# ✅ BETTER: Use a named function for complex logic
def process_record(r):
    return {
        **r,
        "total":  r["price"] * r["qty"],
        "status": "active" if r["qty"] > 0 else "inactive",
    }


# ❌ MISTAKE 4: reduce() on empty sequence without initializer
data = []
total = reduce(lambda a, b: a + b, data)  # TypeError: reduce() of empty sequence!

# ✅ FIX: Provide an initial value as the third argument
total = reduce(lambda a, b: a + b, data, 0)   # Returns 0 instead of crashing
```
