# 🏗️ Type Hints & Dataclasses in Python (ETL Context)

---

## 📖 Explanation

**Type Hints** (PEP 484) let you annotate variables, function arguments, and return types, making code self-documenting and enabling static analysis tools like `mypy`.

**Dataclasses** (PEP 557, Python 3.7+) automatically generate boilerplate methods (`__init__`, `__repr__`, `__eq__`) for classes that primarily store data — perfect for ETL records and configs.

---

## 🧠 Must-Remember Points

### Type Hints

- Type hints are **not enforced at runtime** — use `mypy` for static checking.
- `from __future__ import annotations` enables forward references in older Python.
- `Optional[X]` = `X | None` — use for nullable fields.
- `Union[X, Y]` = `X | Y` (Python 3.10+ supports `X | Y` directly).
- `List`, `Dict`, `Tuple`, `Set` from `typing` are replaced by lowercase `list`, `dict`, `tuple`, `set` in Python 3.9+.
- `Any` turns off type checking for that variable — use sparingly.
- `TypeVar` is used for generic functions.

### Dataclasses

- `@dataclass` auto-generates `__init__`, `__repr__`, `__eq__`.
- `field(default_factory=list)` for mutable default values — NEVER use `default=[]`.
- `frozen=True` makes instances immutable (like namedtuples but with type hints).
- `@dataclass(order=True)` enables `<`, `>` comparisons.
- `post_init()` runs after `__init__` for validation/derived fields.
- Use `dataclasses.asdict()` and `dataclasses.astuple()` to convert to dict/tuple.

---

## 💻 Code Examples

### 1️⃣ Basic Type Hints

```python
from typing import Optional, List, Dict, Union, Any, Tuple

# Function annotations
def calculate_revenue(price: float, quantity: int) -> float:
    return price * quantity

# Optional (nullable)
def get_employee_name(emp_id: int) -> Optional[str]:
    employees = {1: "Alice", 2: "Bob"}
    return employees.get(emp_id)  # Returns str or None

# Union types (Python 3.10+: int | str)
def parse_value(val: Union[str, int]) -> float:
    return float(val)

# List, Dict type hints
def load_records(filepath: str) -> List[Dict[str, Any]]:
    ...  # Implementation

# Python 3.9+ — simpler syntax without 'from typing import'
def process(records: list[dict[str, str]]) -> list[str]:
    return [r["name"] for r in records]
```

---

### 2️⃣ ETL Config with Type Hints

```python
from typing import Optional, Literal

def connect_database(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
    schema: Optional[str] = None,
    ssl_mode: Literal["disable", "require", "verify-full"] = "require",
    pool_size: int = 5,
) -> dict:
    """Connect to a PostgreSQL database for ETL operations."""
    config = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }
    if schema:
        config["options"] = f"-c search_path={schema}"
    return config

# Type hints document the interface clearly
conn = connect_database("localhost", 5432, "etl_db", "admin", "secret", schema="public")
```

---

### 3️⃣ Basic Dataclass

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Employee:
    emp_id: int
    name: str
    department: str
    salary: float
    email: Optional[str] = None

    def __post_init__(self):
        """Validate and normalize after init."""
        if self.salary < 0:
            raise ValueError(f"Salary cannot be negative: {self.salary}")
        self.name = self.name.strip().title()

# Auto-generated __init__, __repr__, __eq__
emp1 = Employee(1, "  alice smith  ", "IT", 75000.0, "alice@example.com")
emp2 = Employee(1, "Alice Smith", "IT", 75000.0, "alice@example.com")
print(emp1)  # Employee(emp_id=1, name='Alice Smith', department='IT', ...)
print(emp1 == emp2)  # True (compares all fields)
```

---

### 4️⃣ ETL Record as Dataclass

```python
from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Optional, List

@dataclass
class SalesRecord:
    order_id: str
    customer_id: int
    product_code: str
    quantity: int
    unit_price: float
    order_date: date
    region: Optional[str] = None

    @property
    def total_amount(self) -> float:
        return self.quantity * self.unit_price

    def to_dict(self) -> dict:
        d = asdict(self)
        d["order_date"] = self.order_date.isoformat()  # Make JSON-serializable
        d["total_amount"] = self.total_amount
        return d

# Usage
record = SalesRecord(
    order_id="ORD-001",
    customer_id=101,
    product_code="PROD-A",
    quantity=5,
    unit_price=199.99,
    order_date=date(2024, 1, 15),
    region="NORTH",
)
print(record.total_amount)  # 999.95
print(record.to_dict())
```

---

### 5️⃣ ETL Pipeline Config as Frozen Dataclass

```python
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass(frozen=True)  # Immutable — config should not change mid-pipeline
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str = "public"
    pool_size: int = 5

@dataclass(frozen=True)
class ETLPipelineConfig:
    name: str
    source_db: DatabaseConfig
    target_db: DatabaseConfig
    batch_size: int = 1000
    enable_logging: bool = True
    source_tables: tuple = ()  # Use tuple (not list) for frozen dataclasses

source = DatabaseConfig("localhost", 5432, "source_db", "user", "pass")
target = DatabaseConfig("prod-server", 5432, "warehouse", "etl_user", "secret_pass")

config = ETLPipelineConfig(
    name="sales_etl",
    source_db=source,
    target_db=target,
    batch_size=500,
    source_tables=("sales", "customers", "products"),
)

print(config.batch_size)        # 500
print(config.source_db.host)    # localhost

# config.batch_size = 999  # ❌ FrozenInstanceError — Immutable!
```

---

### 6️⃣ Dataclass with Mutable Defaults and Validation

```python
from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class ETLBatch:
    job_name: str
    records: List[Dict[str, Any]] = field(default_factory=list)  # NOT default=[]!
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    processed_count: int = field(default=0, init=False)  # Not set by user

    def add_record(self, record: Dict[str, Any]) -> None:
        self.records.append(record)
        self.processed_count += 1

    def add_error(self, error: str) -> None:
        self.errors.append(error)

    def summary(self) -> Dict[str, Any]:
        return {
            "job": self.job_name,
            "total": self.processed_count,
            "errors": len(self.errors),
            "success_rate": (self.processed_count - len(self.errors)) / max(self.processed_count, 1),
        }

batch = ETLBatch(job_name="daily_sales_load")
batch.add_record({"id": 1, "amount": 100})
batch.add_record({"id": 2, "amount": 200})
batch.add_error("Record 3: Invalid amount")
print(batch.summary())
```

---

## 🏭 ETL Use Cases

| Feature                       | ETL Use Case                                     |
| ----------------------------- | ------------------------------------------------ |
| Type hints on functions       | Self-documenting ETL functions; IDE support      |
| `Optional[T]`                 | Nullable fields (common in raw data)             |
| `@dataclass`                  | ETL row/record schema definition                 |
| `frozen=True`                 | Immutable pipeline config objects                |
| `field(default_factory=list)` | Batch results container                          |
| `post_init` validation        | Enforce business rules on record loading         |
| `asdict()`                    | Convert dataclass records to dicts for DB writes |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Mutable default in dataclass — shared across instances!
@dataclass
class BadBatch:
    records: list = []  # ❌ All instances share the same list!

# RIGHT: Use field(default_factory=list)
@dataclass
class GoodBatch:
    records: list = field(default_factory=list)  # ✅ Each instance gets its own list

# WRONG: Forgetting type hints are not enforced
def add(x: int, y: int) -> int:
    return x + y

result = add("hello", " world")  # str + str — no runtime error!
# RIGHT: Use mypy for static analysis: `mypy etl_script.py`
```
