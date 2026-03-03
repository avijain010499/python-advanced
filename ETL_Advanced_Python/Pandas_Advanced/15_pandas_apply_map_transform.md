# 🔄 Pandas Apply, Map & Transform (ETL Context)

---

## 🤔 What Are apply, map, and transform?

These three functions let you run custom logic on parts of a DataFrame:

| Function                      | Works On        | Returns          | When to Use                         |
| ----------------------------- | --------------- | ---------------- | ----------------------------------- |
| `.map()`                      | Series (column) | Same-size Series | Simple element-wise mapping/replace |
| `.apply()` on Series          | Series          | Any shape        | Custom function per element         |
| `.apply(axis=1)` on DataFrame | Each row        | Series/scalar    | Multi-column per-row logic          |
| `groupby().transform()`       | Group           | Same-size Series | Add group stat to each row          |

> ⚠️ **Performance Rule**: Prefer vectorized operations (arithmetic, `.str`, `.dt`) over `.apply()`. Use `.apply()` only when no vectorized alternative exists.

---

## 💻 Example 1: Series `.map()` — Recode / Replace Values

```python
import pandas as pd

df = pd.DataFrame({
    "status_code": ["A", "I", "P", "A", "S"],
    "region_code": ["N", "S", "E", "W", "N"],
})

# Map codes to human-readable labels (dict lookup per element)
status_map = {"A": "Active", "I": "Inactive", "P": "Pending", "S": "Suspended"}
region_map = {"N": "North",  "S": "South",    "E": "East",    "W": "West"}

df["status"] = df["status_code"].map(status_map)
df["region"] = df["region_code"].map(region_map)
# Unmapped values become NaN — check df["status"].isna().sum() after!

print(df)
```

---

## 💻 Example 2: `.apply()` on a Column

```python
import re

def clean_phone(phone_raw):
    """Remove all non-digit characters, then format as XXX-XXX-XXXX."""
    if pd.isna(phone_raw):
        return None
    digits = re.sub(r"\D", "", str(phone_raw))  # Keep only digits
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return None  # Invalid format

df = pd.DataFrame({"phone": ["(555) 123-4567", "800.555.9999", "12345", None]})
df["phone_clean"] = df["phone"].apply(clean_phone)
print(df)
# (555) 123-4567 → 555-123-4567
# 800.555.9999  → 800-555-9999
# 12345         → None  (not 10 digits)
```

---

## 💻 Example 3: `.apply(axis=1)` on Rows — Multi-Column Logic

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({
    "base_price": [100, 200, 150, 300],
    "qty":        [10,   5,   8,  12],
    "discount":   [0.0, 0.1, 0.05, 0.2],
    "region":     ["NORTH", "SOUTH", "EAST", "NORTH"],
})

# ---- Fast vectorized equivalent (PREFER THIS!) ----
df["revenue"] = df["base_price"] * df["qty"] * (1 - df["discount"])
# North gets a 5% bonus:
df["final_revenue"] = np.where(
    df["region"] == "NORTH",
    df["revenue"] * 1.05,
    df["revenue"]
)

# ---- apply(axis=1) when logic is too complex for vectorization ----
def calculate_commission(row):
    """Multi-column logic: commission rate depends on revenue AND region."""
    revenue = row["base_price"] * row["qty"] * (1 - row["discount"])
    if row["region"] == "NORTH" and revenue > 1000:
        rate = 0.15
    elif revenue > 500:
        rate = 0.10
    else:
        rate = 0.05
    return revenue * rate

df["commission"] = df.apply(calculate_commission, axis=1)
print(df[["region", "revenue", "commission"]])
```

---

## 💻 Example 4: `groupby().transform()` — Group Stats Per Row

```python
# Add group-level statistics to individual rows
df = pd.DataFrame({
    "region":  ["NORTH","NORTH","SOUTH","SOUTH","NORTH"],
    "sales":   [1000,    500,   800,    1200,   700],
})

df["region_total"]   = df.groupby("region")["sales"].transform("sum")
df["region_avg"]     = df.groupby("region")["sales"].transform("mean")
df["region_rank"]    = df.groupby("region")["sales"].transform("rank", ascending=False)
df["pct_of_region"]  = (df["sales"] / df["region_total"] * 100).round(1)

#   region  sales  region_total  region_avg  region_rank  pct_of_region
#    NORTH   1000          2200       733.33          1.0           45.5
#    NORTH    500          2200       733.33          3.0           22.7
#    SOUTH    800          2000      1000.00          2.0           40.0
#    SOUTH   1200          2000      1000.00          1.0           60.0
#    NORTH    700          2200       733.33          2.0           31.8
```

---

## 💻 Example 5: `np.where` and `np.select` — Vectorized Conditionals

Always prefer these over `.apply()` for if/else logic:

```python
import numpy as np

df = pd.DataFrame({"revenue": [100, 500, 1200, 3000, 150]})

# np.where: single condition
df["tier"] = np.where(df["revenue"] >= 1000, "High", "Low")

# np.select: multiple conditions (like a CASE WHEN in SQL)
conditions = [
    df["revenue"] >= 3000,
    df["revenue"] >= 1000,
    df["revenue"] >= 500,
]
choices = ["Platinum", "Gold", "Silver"]

df["tier"] = np.select(conditions, choices, default="Bronze")
print(df)
```

---

## 🏭 ETL Use Cases Summary

| Method                   | ETL Use Case                             |
| ------------------------ | ---------------------------------------- |
| `.map(dict)`             | Recode category codes → labels           |
| `.apply()` on column     | Custom cleaning (phone, address parsing) |
| `.apply(axis=1)`         | Multi-column business rules              |
| `groupby().transform()`  | Percentages, ranks within groups         |
| `np.where` / `np.select` | Fast vectorized conditional columns      |

---

## ⚠️ Common Mistakes

```python
# ❌ Using apply for simple arithmetic (100x slower!)
df["revenue"] = df.apply(lambda r: r["price"] * r["qty"], axis=1)
# ✅
df["revenue"] = df["price"] * df["qty"]

# ❌ Forgetting to handle NaN in apply functions
df["clean"] = df["name"].apply(lambda x: x.strip())  # Crashes if x is NaN!
# ✅
df["clean"] = df["name"].apply(lambda x: x.strip() if pd.notna(x) else None)
# Or better:
df["clean"] = df["name"].str.strip()   # .str methods handle NaN automatically
```
