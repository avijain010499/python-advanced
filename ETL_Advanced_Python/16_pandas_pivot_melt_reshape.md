# 🔀 Pandas Pivot, Melt & Reshape (ETL Context)

---

## 📖 Explanation

Reshaping data is a core ETL task — converting between **wide** (one column per variable) and **long** (one row per observation) formats, transposing, and pivoting for reporting.

Key functions: `pivot_table()`, `melt()`, `stack()`, `unstack()`, `crosstab()`.

---

## 🧠 Must-Remember Points

- **`pivot_table()`** is the most powerful — supports aggregation when there are duplicate values.
- **`pivot()`** is simpler but **fails if there are duplicate (index, columns) pairs**.
- **`melt()`** converts **wide → long** format (un-pivoting). Essential for normalizing denormalized source data.
- **`stack()`** pivots the innermost column level to the innermost row level (wide → long).
- **`unstack()`** is the inverse of `stack()` (long → wide).
- **`pd.crosstab()`** creates frequency tables.
- `fill_value=0` in `pivot_table()` replaces NaN with 0 in sparse pivots.
- `margins=True` adds totals row/column in `pivot_table()`.
- `aggfunc` in `pivot_table()` can be a function, list, or dict.

---

## 💻 Code Examples

### 1️⃣ `pivot_table()` — SQL-style Pivot

```python
import pandas as pd
import numpy as np

sales = pd.DataFrame({
    "region": ["NORTH", "SOUTH", "NORTH", "EAST", "SOUTH", "EAST", "NORTH"],
    "product": ["Widget A", "Widget A", "Widget B", "Widget A", "Widget B", "Widget B", "Widget A"],
    "month": ["Jan", "Jan", "Jan", "Feb", "Feb", "Feb", "Feb"],
    "sales": [1500, 1200, 800, 900, 1100, 750, 2000],
})

# Pivot: rows=region, columns=product, values=sum(sales)
pivot = pd.pivot_table(
    sales,
    index="region",
    columns="product",
    values="sales",
    aggfunc="sum",
    fill_value=0,
    margins=True,       # Add 'All' totals row and column
    margins_name="Total",
)
print(pivot)
# product  Widget A  Widget B  Total
# region
# EAST          900       750   1650
# NORTH        3500       800   4300
# SOUTH        1200      1100   2300
# Total        5600      2650   8250
```

---

### 2️⃣ `pivot_table()` — Multiple Values and Aggregations

```python
df = pd.DataFrame({
    "department": ["IT", "HR", "IT", "FIN", "HR", "FIN"],
    "gender": ["M", "F", "F", "M", "M", "F"],
    "salary": [75000, 60000, 80000, 65000, 62000, 70000],
    "bonus": [10000, 8000, 12000, 9000, 7000, 11000],
})

# Multiple values, multiple aggfuncs
pt = pd.pivot_table(
    df,
    index="department",
    columns="gender",
    values=["salary", "bonus"],
    aggfunc={"salary": "mean", "bonus": "sum"},
    fill_value=0,
)
print(pt)
```

---

### 3️⃣ `melt()` — Wide to Long (Normalize Denormalized Data)

```python
# ETL Source: wide format (one column per month)
wide_df = pd.DataFrame({
    "product_id": [101, 102, 103],
    "product_name": ["Widget A", "Widget B", "Widget C"],
    "jan_sales": [1500, 800, 1200],
    "feb_sales": [1800, 900, 1100],
    "mar_sales": [2000, 750, 1300],
})

# Convert to long format (one row per product-month combination)
long_df = pd.melt(
    wide_df,
    id_vars=["product_id", "product_name"],    # Keep these columns
    value_vars=["jan_sales", "feb_sales", "mar_sales"],  # Melt these
    var_name="month",               # Name of the new 'variable' column
    value_name="sales_amount",      # Name of the new 'value' column
)
long_df["month"] = long_df["month"].str.replace("_sales", "").str.upper()
print(long_df)
# product_id product_name month  sales_amount
#        101     Widget A   JAN          1500
#        102     Widget B   JAN           800
#        ...
```

---

### 4️⃣ ETL: Melt + Pivot Roundtrip

```python
# melt() then pivot_table() to reshape and aggregate
monthly_by_region = pd.DataFrame({
    "region": ["NORTH", "SOUTH", "EAST"],
    "Q1": [5000, 4000, 3000],
    "Q2": [5500, 4200, 3300],
    "Q3": [6000, 4500, 3100],
    "Q4": [7000, 5000, 3800],
})

# Melt to long
long = monthly_by_region.melt(
    id_vars="region",
    var_name="quarter",
    value_name="revenue",
)

# Re-pivot: rows=quarter, columns=region
repivot = long.pivot_table(
    index="quarter",
    columns="region",
    values="revenue",
    aggfunc="sum",
).reset_index()
print(repivot)
```

---

### 5️⃣ `stack()` and `unstack()` — MultiIndex Reshape

```python
# Create DataFrame with MultiIndex columns
df = pd.DataFrame({
    ("IT", "salary"): [75000, 80000],
    ("IT", "bonus"): [10000, 12000],
    ("HR", "salary"): [60000, 65000],
    ("HR", "bonus"): [8000, 9000],
}, index=["Alice", "Bob"])
df.columns = pd.MultiIndex.from_tuples(df.columns)

# Stack: move innermost column level to row index
stacked = df.stack(level=0)  # Wide → Long
print(stacked)

# Unstack: move innermost row level to column
unstacked = stacked.unstack(level=1)  # Long → Wide
print(unstacked)
```

---

### 6️⃣ `pd.crosstab()` — Frequency / Contingency Table

```python
# ETL: Understand data distribution for quality checks
df = pd.DataFrame({
    "region": ["NORTH", "SOUTH", "NORTH", "EAST", "SOUTH"],
    "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE", "INACTIVE"],
})

# Count of each status per region
ct = pd.crosstab(df["region"], df["status"], margins=True)
print(ct)
# status   ACTIVE  INACTIVE  All
# region
# EAST          1         0    1
# NORTH         1         1    2
# SOUTH         1         1    2
# All           3         2    5

# Normalize to percentages
ct_pct = pd.crosstab(df["region"], df["status"], normalize="index") * 100
print(ct_pct.round(1))
```

---

## 🏭 ETL Use Cases

| Function                    | ETL Use Case                                             |
| --------------------------- | -------------------------------------------------------- |
| `pivot_table()`             | Aggregate data for reporting / data mart tables          |
| `pivot_table(margins=True)` | Generate totals/subtotals for finance reports            |
| `melt()`                    | Normalize wide source files (ERP exports) to long format |
| `stack()` / `unstack()`     | Reshape MultiIndex DataFrames                            |
| `crosstab()`                | Data quality checks, frequency distribution analysis     |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Using pivot() when duplicates exist
df = pd.DataFrame({
    "region": ["NORTH", "NORTH"],
    "product": ["A", "A"],  # Duplicate!
    "sales": [100, 200],
})
df.pivot(index="region", columns="product", values="sales")  # ValueError!

# RIGHT: Use pivot_table() which handles duplicates via aggregation
df.pivot_table(index="region", columns="product", values="sales", aggfunc="sum")

# WRONG: Forgetting to reset_index() after pivot for clean DataFrames
result = df.pivot_table(index="region", columns="product", values="sales")
# result has 'product' as column name and 'region' as index — messy!

# RIGHT:
result = df.pivot_table(...).reset_index()
result.columns.name = None  # Remove the column-axis name
```
