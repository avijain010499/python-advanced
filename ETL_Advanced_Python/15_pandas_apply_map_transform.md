# 🔄 Pandas Apply, Map & Transform (ETL Context)

---

## 📖 Explanation

`apply()`, `map()`, and `transform()` are the three main ways to apply custom functions element-wise or group-wise in Pandas:

- **`df["col"].map()`** — Element-wise on a **Series** (mapping or function).
- **`df.apply(func, axis=...)`** — Apply a function across **rows** or **columns** of a DataFrame.
- **`df["col"].apply(func)`** — Element-wise function on a **Series** column.
- **`groupby().transform()`** — Apply function per group, return original-shape result.

---

## 🧠 Must-Remember Points

- `map()` is the fastest for element-wise Series operations (vectorized where possible).
- `apply(axis=0)` operates column-wise (default); `apply(axis=1)` operates row-wise.
- Row-wise `apply(axis=1)` is slow — prefer vectorized operations (`np.where`, arithmetic) or `assign()`.
- `Series.map(dict)` is a fast lookup substitution — great for code-to-value mapping.
- `applymap()` / `map()` on DataFrame applies element-wise across all cells (deprecated `applymap` in Pandas 2.1+, use `map()` instead).
- `transform()` after `groupby()` preserves the original DataFrame shape.
- For performance: **NumPy vectorized ops > `map()` > `apply()` > Python loops**.
- `np.vectorize()` can wrap Python functions for better performance.

---

## 💻 Code Examples

### 1️⃣ `Series.map()` — Element-wise Substitution

```python
import pandas as pd

df = pd.DataFrame({
    "status_code": ["A", "I", "P", "A", "D"],
    "dept_code": [10, 20, 30, 10, 20],
    "amount": [1500, 200, 800, 3000, 500],
})

# Map codes to human-readable labels (ETL decode)
status_map = {"A": "Active", "I": "Inactive", "P": "Pending", "D": "Deleted"}
df["status"] = df["status_code"].map(status_map)

# Map with a function
df["amount_log"] = df["amount"].map(lambda x: round(x / 1000, 2))

# map() with dict — unmatched keys become NaN
dept_map = {10: "IT", 20: "HR"}  # 30 is unmapped
df["dept_name"] = df["dept_code"].map(dept_map)  # 30 → NaN
print(df)
```

---

### 2️⃣ `Series.apply()` — Custom Function on a Column

```python
import re

def parse_phone(phone_str):
    """Normalize phone numbers to digits only."""
    if pd.isna(phone_str):
        return None
    digits = re.sub(r"\D", "", str(phone_str))
    return digits if len(digits) == 10 else None

phone_df = pd.DataFrame({
    "phone": ["(555) 123-4567", "800-555-9999", "1234", None, "555.123.4567"],
})

phone_df["phone_clean"] = phone_df["phone"].apply(parse_phone)
print(phone_df)
```

---

### 3️⃣ `DataFrame.apply(axis=1)` — Row-wise Processing

```python
df = pd.DataFrame({
    "first_name": ["Alice", "Bob", "Charlie"],
    "last_name": ["Smith", "Jones", "Brown"],
    "salary": [75000, 60000, 90000],
    "bonus_pct": [10, 5, 15],
})

# Row-wise: combine values from multiple columns
def compute_full_info(row):
    full_name = f"{row['first_name']} {row['last_name']}"
    total_comp = row["salary"] * (1 + row["bonus_pct"] / 100)
    return pd.Series({"full_name": full_name, "total_compensation": total_comp})

enriched = df.apply(compute_full_info, axis=1)
df = pd.concat([df, enriched], axis=1)
print(df[["full_name", "total_compensation"]])
```

---

### 4️⃣ ETL: Vectorized Alternative to Row-wise `apply()`

```python
import numpy as np

df = pd.DataFrame({
    "revenue": [1000, -50, 2000, 0, 1500],
    "cost": [600, 100, 1200, 0, 900],
    "region": ["NORTH", "SOUTH", "EAST", "WEST", "NORTH"],
})

# SLOW — row-wise apply
df["profit_slow"] = df.apply(lambda r: r["revenue"] - r["cost"], axis=1)

# FAST — vectorized arithmetic (preferred in ETL)
df["profit"] = df["revenue"] - df["cost"]

# FAST — np.where for conditional column
df["status"] = np.where(df["profit"] > 0, "Profitable", "Loss")

# FAST — np.select for multiple conditions (like CASE WHEN in SQL)
conditions = [
    df["profit"] > 1000,
    df["profit"] > 0,
    df["profit"] == 0,
]
choices = ["High Profit", "Low Profit", "Break Even"]
df["profit_tier"] = np.select(conditions, choices, default="Loss")
print(df)
```

---

### 5️⃣ `groupby().transform()` — Broadcast Group Stats

```python
df = pd.DataFrame({
    "dept": ["IT", "HR", "IT", "FIN", "HR"],
    "emp": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
    "salary": [75000, 60000, 80000, 65000, 62000],
})

# Add group-level stats back to each row (same length as df)
df["dept_avg"] = df.groupby("dept")["salary"].transform("mean")
df["dept_pct_rank"] = df.groupby("dept")["salary"].transform(
    lambda x: x.rank(pct=True)
)
df["above_avg"] = df["salary"] > df["dept_avg"]
print(df)
```

---

### 6️⃣ ETL: Applying Complex Transformation with `apply()`

```python
def validate_and_clean_record(row: pd.Series) -> pd.Series:
    """Multi-field validation and normalization."""
    errors = []

    # Clean name
    name = str(row["name"]).strip().title() if pd.notna(row["name"]) else ""
    if not name:
        errors.append("missing_name")

    # Validate amount
    amount = pd.to_numeric(row["amount"], errors="coerce")
    if pd.isna(amount) or amount < 0:
        errors.append("invalid_amount")
        amount = None

    # Derive region from country
    region_map = {"US": "NORTH_AMERICA", "GB": "EUROPE", "IN": "ASIA"}
    region = region_map.get(str(row.get("country", "")), "UNKNOWN")

    return pd.Series({
        "name_clean": name,
        "amount_clean": amount,
        "region": region,
        "is_valid": len(errors) == 0,
        "validation_errors": "|".join(errors) if errors else None,
    })

df = pd.DataFrame({
    "name": ["  alice ", None, "BOB"],
    "amount": ["1500", "bad", "-100"],
    "country": ["US", "GB", "IN"],
})

result = df.apply(validate_and_clean_record, axis=1)
final = pd.concat([df, result], axis=1)
print(final[["name", "name_clean", "amount_clean", "is_valid"]])
```

---

## 🏭 ETL Use Cases

| Function                        | ETL Use Case                                         |
| ------------------------------- | ---------------------------------------------------- |
| `Series.map(dict)`              | Decode codes: status codes, dept codes, region codes |
| `Series.apply(func)`            | Complex single-column cleaning (phone, email, dates) |
| `DataFrame.apply(func, axis=1)` | Row-wise multi-column validation/enrichment          |
| `np.where()` / `np.select()`    | Fast conditional column creation (CASE WHEN)         |
| `groupby().transform()`         | Normalize salaries, compute % of group total         |

---

## ⚠️ Common Pitfalls

```python
import pandas as pd, numpy as np

# WRONG: Row-wise apply for simple arithmetic (very slow)
df["profit"] = df.apply(lambda r: r["revenue"] - r["cost"], axis=1)  # Slow!

# RIGHT: Vectorized arithmetic
df["profit"] = df["revenue"] - df["cost"]  # 50-100x faster

# WRONG: Using applymap() in Pandas 2.1+
df = df.applymap(str)  # FutureWarning! Deprecated!

# RIGHT: Use map() in Pandas 2.1+
df = df.map(str)

# WRONG: Ignoring NaN in map()
category_map = {"A": 1, "B": 2}
df["code"] = df["raw_code"].map(category_map)
# NaN if 'raw_code' has values not in map — check for unexpected NaNs!
missing = df["code"].isna().sum()
if missing:
    print(f"Warning: {missing} unmapped values!")
```
