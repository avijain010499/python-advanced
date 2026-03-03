# ⚙️ Functional Programming in Python (ETL Context)

---

## 📖 Explanation

Functional programming (FP) treats computation as the evaluation of mathematical functions. Python supports FP concepts with:

- **`lambda`**: Anonymous (inline) functions
- **`map()`**: Apply a function to every element
- **`filter()`**: Select elements satisfying a condition
- **`reduce()`**: Accumulate elements into a single value
- **`functools`** and **`operator`** modules

In ETL, FP tools enable concise, declarative data transformation pipelines.

---

## 🧠 Must-Remember Points

- `lambda` functions are limited to a single expression — use `def` for complex logic.
- `map()` and `filter()` return **lazy iterators** — wrap with `list()` to materialize.
- `reduce()` is in `functools` module (not built-in in Python 3).
- Prefer comprehensions over `map()`/`filter()` when readability matters.
- Use `functools.partial` to create specialized versions of functions.
- `operator` module provides efficient function equivalents of operators (`add`, `mul`, etc.).
- Pure functions (no side effects) make ETL pipelines testable and predictable.

---

## 💻 Code Examples

### 1️⃣ `lambda` — Anonymous Functions

```python
# Basic lambda
double = lambda x: x * 2
print(double(5))  # 10

# Lambda with multiple arguments
add = lambda x, y: x + y
print(add(3, 4))  # 7

# Lambda in sorting (very common in ETL!)
employees = [
    {"name": "Charlie", "salary": 60000},
    {"name": "Alice", "salary": 75000},
    {"name": "Bob", "salary": 50000},
]

# Sort by salary descending
sorted_employees = sorted(employees, key=lambda e: e["salary"], reverse=True)
print(sorted_employees[0])  # {'name': 'Alice', 'salary': 75000}

# Sort by multiple fields (dept, then salary)
data = [
    {"dept": "IT", "salary": 80000},
    {"dept": "HR", "salary": 60000},
    {"dept": "IT", "salary": 70000},
]
sorted_data = sorted(data, key=lambda r: (r["dept"], -r["salary"]))
```

---

### 2️⃣ `map()` — Apply a Function to Every Element

```python
# Basic map
nums = [1, 2, 3, 4, 5]
squares = list(map(lambda x: x ** 2, nums))
print(squares)  # [1, 4, 9, 16, 25]

# ETL: Convert data types across multiple records
raw_prices = ["10.5", "20.0", "15.75", "8.99"]
prices = list(map(float, raw_prices))
print(prices)  # [10.5, 20.0, 15.75, 8.99]

# ETL: Clean multiple columns
def clean_record(record):
    return {
        "id": int(record["id"]),
        "name": record["name"].strip().title(),
        "revenue": float(record["revenue"] or 0),
    }

raw_records = [
    {"id": "1", "name": "  ALICE ", "revenue": "1500.50"},
    {"id": "2", "name": "bob", "revenue": ""},
]
cleaned = list(map(clean_record, raw_records))
print(cleaned)
```

---

### 3️⃣ `filter()` — Select Elements Matching a Condition

```python
# Basic filter
numbers = [1, -2, 3, -4, 5, 0, -6]
positives = list(filter(lambda x: x > 0, numbers))
print(positives)  # [1, 3, 5]

# ETL: Filter valid records
records = [
    {"id": 1, "status": "COMPLETE", "amount": 500},
    {"id": 2, "status": "PENDING", "amount": 200},
    {"id": 3, "status": "COMPLETE", "amount": 0},
    {"id": 4, "status": "FAILED", "amount": 300},
]

def is_valid(record):
    return record["status"] == "COMPLETE" and record["amount"] > 0

valid = list(filter(is_valid, records))
print(valid)  # [{'id': 1, 'status': 'COMPLETE', 'amount': 500}]

# filter(None, iterable) removes falsy values
data = ["Alice", "", None, "Bob", 0, "Charlie"]
non_empty = list(filter(None, data))
print(non_empty)  # ['Alice', 'Bob', 'Charlie']
```

---

### 4️⃣ `reduce()` — Accumulate/Aggregate

```python
from functools import reduce

# Sum of a list (equivalent to sum())
nums = [1, 2, 3, 4, 5]
total = reduce(lambda acc, x: acc + x, nums)
print(total)  # 15

# ETL: Merge multiple dicts into one
config_overrides = [
    {"host": "localhost"},
    {"port": 5432},
    {"dbname": "etl_db"},
    {"user": "admin"},
]
merged_config = reduce(lambda acc, d: {**acc, **d}, config_overrides)
print(merged_config)
# {'host': 'localhost', 'port': 5432, 'dbname': 'etl_db', 'user': 'admin'}

# Find maximum value with reduce
max_val = reduce(lambda a, b: a if a > b else b, [3, 1, 4, 1, 5, 9, 2, 6])
print(max_val)  # 9
```

---

### 5️⃣ `functools.partial` — Pre-fill Function Arguments

```python
from functools import partial

# Generic CSV reader with configurable delimiter
def read_csv(filepath, delimiter=",", encoding="utf-8"):
    with open(filepath, encoding=encoding) as f:
        return f.read()

# Create specialized versions
read_tsv = partial(read_csv, delimiter="\t")
read_pipe = partial(read_csv, delimiter="|")
read_latin = partial(read_csv, encoding="latin-1")

# Use them like normal functions
# data = read_tsv("data.tsv")
# data = read_pipe("data.txt")

# Another common use: parameterized transformations
def scale_value(value, factor, offset=0):
    return value * factor + offset

normalize = partial(scale_value, factor=1/100, offset=0)
print(list(map(normalize, [100, 200, 300])))  # [1.0, 2.0, 3.0]
```

---

### 6️⃣ Function Composition — Building ETL Pipelines

```python
from functools import reduce

def compose(*functions):
    """Compose multiple functions: compose(f, g, h)(x) = f(g(h(x)))"""
    return reduce(lambda f, g: lambda x: f(g(x)), functions)

# ETL transformation functions
remove_whitespace = lambda s: s.strip()
to_uppercase = lambda s: s.upper()
replace_spaces = lambda s: s.replace(" ", "_")

# Compose into a pipeline
normalize_column_name = compose(replace_spaces, to_uppercase, remove_whitespace)

raw_names = ["  employee name ", "  dept code  ", " hire date "]
clean_names = list(map(normalize_column_name, raw_names))
print(clean_names)  # ['EMPLOYEE_NAME', 'DEPT_CODE', 'HIRE_DATE']
```

---

### 7️⃣ `operator` Module — Efficient Functional Operations

```python
import operator
from functools import reduce

# operator equivalents are faster than lambdas for simple ops
nums = [1, 2, 3, 4, 5]

total = reduce(operator.add, nums)      # sum
product = reduce(operator.mul, nums)    # product: 120

# Sorting by key using attrgetter / itemgetter (faster than lambda)
from operator import itemgetter

employees = [
    {"name": "Bob", "salary": 60000, "dept": "IT"},
    {"name": "Alice", "salary": 75000, "dept": "HR"},
    {"name": "Charlie", "salary": 60000, "dept": "IT"},
]

# Sort by dept then salary (descending)
sorted_emps = sorted(employees, key=itemgetter("dept", "salary"))
print(sorted_emps)
```

---

## 🏭 ETL Use Cases

| FP Tool              | ETL Use Case                                         |
| -------------------- | ---------------------------------------------------- |
| `lambda`             | Inline transformations for `sort()`, `map()`, Pandas |
| `map()`              | Apply type conversions, cleaning to every record     |
| `filter()`           | Remove invalid/null/duplicate records                |
| `reduce()`           | Aggregate data, merge configs, compute totals        |
| `partial()`          | Parameterize DB connectors, formatters, readers      |
| Function composition | Build declarative multi-step transform pipelines     |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Using map/filter when comprehension is clearer
result = list(map(lambda x: x * 2, filter(lambda x: x > 0, data)))

# BETTER: Use a comprehension
result = [x * 2 for x in data if x > 0]

# WRONG: Forgetting map/filter return iterators
doubled = map(lambda x: x * 2, [1, 2, 3])
print(doubled)       # <map object at 0x...> — NOT a list!
print(doubled[0])    # TypeError!

# RIGHT: Materialize when needed
doubled = list(map(lambda x: x * 2, [1, 2, 3]))
```
