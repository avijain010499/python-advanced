# 🏗️ Type Hints & Dataclasses in Python (ETL Context)

---

## 🤔 What Are Type Hints?

Python is dynamically typed — you don't _have_ to declare variable types. But in large ETL codebases, this leads to bugs like passing a string where a number was expected.

**Type hints** let you _annotate_ what types you expect, making code self-documenting and enabling tools like `mypy` to catch bugs before runtime.

> 💡 **Key point**: Type hints are **not enforced at runtime** — Python doesn't check them. They are for _you_, your teammates, and static analysis tools.

---

## 🧱 Basic Type Hint Syntax

```python
# Variable annotations
name: str = "Alice"
age: int  = 25
salary: float = 75000.0
is_active: bool = True

# Function annotations: parameters and return type
def greet(name: str, times: int = 1) -> str:
    return (f"Hello, {name}! " * times).strip()

result: str = greet("Bob", 3)
print(result)   # "Hello, Bob! Hello, Bob! Hello, Bob!"
```

---

## 🧱 Common Type Hint Patterns

```python
from typing import Optional, Union, List, Dict, Any, Tuple

# Optional: the value can be this type OR None
def get_salary(emp_id: int) -> Optional[float]:
    data = {1: 75000.0, 2: 60000.0}
    return data.get(emp_id)   # Returns float or None

# Union: one of several types (Python 3.10+ can write: int | str)
def parse_id(value: Union[str, int]) -> int:
    return int(value)

# List, Dict — containers with typed contents
def load_records(path: str) -> List[Dict[str, Any]]:
    ...

# Python 3.9+: use lowercase list, dict, tuple directly
def process(rows: list[dict[str, str]]) -> list[str]:
    return [r["name"] for r in rows]
```

---

## 🤔 What Are Dataclasses?

A `@dataclass` automatically generates `__init__`, `__repr__`, and `__eq__` for a class that represents structured data — like a database row or ETL record.

```python
from dataclasses import dataclass

# WITHOUT dataclass — lots of boilerplate
class EmployeeOld:
    def __init__(self, emp_id, name, salary):
        self.emp_id = emp_id
        self.name   = name
        self.salary = salary
    def __repr__(self):
        return f"Employee(emp_id={self.emp_id}, name={self.name}, salary={self.salary})"
    def __eq__(self, other):
        return (self.emp_id, self.name, self.salary) == (other.emp_id, other.name, other.salary)

# WITH @dataclass — same thing, 3 lines!
@dataclass
class Employee:
    emp_id: int
    name:   str
    salary: float

emp = Employee(1, "Alice", 75000.0)
print(emp)           # Employee(emp_id=1, name='Alice', salary=75000.0)
print(emp.emp_id)    # 1
```

---

## 💻 Example 1: ETL Record Schema

```python
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

@dataclass
class SalesRecord:
    """Represents one row in the sales source table."""
    order_id:    str
    customer_id: int
    product:     str
    quantity:    int
    unit_price:  float
    order_date:  date
    region:      Optional[str] = None   # Nullable field — defaults to None

    @property
    def total_amount(self) -> float:
        """Computed property — not stored, calculated on access."""
        return self.quantity * self.unit_price

    def is_valid(self) -> bool:
        """Business rule validation."""
        return self.quantity > 0 and self.unit_price > 0

# Create a record
record = SalesRecord(
    order_id    = "ORD-001",
    customer_id = 101,
    product     = "Widget A",
    quantity    = 5,
    unit_price  = 199.99,
    order_date  = date(2024, 1, 15),
    region      = "NORTH",
)
print(record.total_amount)   # 999.95
print(record.is_valid())     # True
print(record)
# SalesRecord(order_id='ORD-001', customer_id=101, ...)
```

---

## 💻 Example 2: Frozen (Immutable) Config Dataclass

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)   # Immutable — can't change fields after creation
class DBConfig:
    host:      str
    port:      int
    dbname:    str
    user:      str
    password:  str
    pool_size: int = 5

# All ETL jobs share this config — no accidental mutation!
config = DBConfig("prod-server", 5432, "warehouse", "etl_user", "secret")
print(config.host)     # prod-server
# config.host = "new"  # ❌ FrozenInstanceError — immutable!
```

---

## 💻 Example 3: Batch Container with Mutable Defaults

```python
from dataclasses import dataclass, field

@dataclass
class ETLBatch:
    """Tracks results of processing one batch of records."""
    job_name:    str
    # ⚠️ For mutable defaults (list, dict), ALWAYS use field(default_factory=...)
    records:     list = field(default_factory=list)   # NOT records: list = []
    errors:      list = field(default_factory=list)
    processed:   int  = field(default=0, init=False)  # Not exposed in __init__

    def add_record(self, r: dict) -> None:
        self.records.append(r)
        self.processed += 1

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def summary(self) -> dict:
        success  = self.processed - len(self.errors)
        return {"job": self.job_name, "total": self.processed,
                "success": success, "failed": len(self.errors)}

batch = ETLBatch("daily_sales_load")
batch.add_record({"id": 1})
batch.add_record({"id": 2})
batch.add_error("Row 3: invalid amount")
print(batch.summary())
# {'job': 'daily_sales_load', 'total': 2, 'success': 1, 'failed': 1}
```

---

## 💻 Example 4: `dataclasses.asdict()` — Convert to Dict for DB Write

```python
from dataclasses import dataclass, asdict, astuple
from datetime import date

@dataclass
class SalesRecord:
    order_id:    str
    customer_id: int
    amount:      float
    order_date:  date

record = SalesRecord("ORD-001", 101, 1500.0, date(2024, 1, 15))

# Convert to plain dict (e.g., for json.dumps or DB insert)
d = asdict(record)
print(d)
# {'order_id': 'ORD-001', 'customer_id': 101, 'amount': 1500.0, 'order_date': datetime.date(2024, 1, 15)}

# Convert to tuple (e.g., for cursor.executemany)
t = astuple(record)
print(t)   # ('ORD-001', 101, 1500.0, datetime.date(2024, 1, 15))
```

---

## 🏭 ETL Use Cases Summary

| Feature                       | ETL Use Case                                             |
| ----------------------------- | -------------------------------------------------------- |
| Function type hints           | Self-documenting ETL functions; IDE autocomplete         |
| `Optional[T]`                 | Nullable source columns                                  |
| `@dataclass`                  | Row/record schema definition with auto-generated methods |
| `frozen=True`                 | Immutable pipeline config                                |
| `field(default_factory=list)` | Batch result containers                                  |
| `asdict()`                    | Convert records to dicts for DB writes or JSON           |

---

## ⚠️ Common Mistakes

```python
# ❌ Mutable default in dataclass — all instances share the SAME list!
@dataclass
class Bad:
    rows: list = []   # SyntaxError in Python 3.7+, or shared object!

# ✅ Use field(default_factory=list)
@dataclass
class Good:
    rows: list = field(default_factory=list)

# ❌ Thinking type hints enforce types at runtime
def add(x: int, y: int) -> int:
    return x + y

result = add("hello", " world")  # No error! Returns "hello world" (str)
# ✅ Use mypy for static type checking: mypy etl_script.py
```
