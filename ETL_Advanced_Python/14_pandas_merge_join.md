# 🔗 Pandas Merge & Join (ETL Context)

---

## 📖 Explanation

Merging and joining DataFrames is the Pandas equivalent of SQL joins — essential for combining data from multiple sources in ETL. Key functions: `pd.merge()`, `df.join()`, `pd.concat()`.

---

## 🧠 Must-Remember Points

- **`pd.merge()`** is the most flexible — works on columns or index.
- **`df.join()`** joins on the **index** by default (faster for index-based joins).
- **`pd.concat()`** stacks DataFrames vertically (union) or horizontally.
- Join types: `'inner'` (default), `'left'`, `'right'`, `'outer'`, `'cross'`.
- Use `on=` for same-named key column, `left_on=` / `right_on=` for different names.
- `suffixes=('_left', '_right')` handles duplicate column names post-merge.
- `validate='one_to_many'` (or `'1:m'`, `'m:1'`, `'1:1'`) checks join cardinality.
- `indicator=True` adds a `_merge` column showing where each row came from.
- Watch out for **cardinality explosions** with many-to-many joins.
- `pd.concat(ignore_index=True)` resets the index after stacking.

---

## 💻 Code Examples

### 1️⃣ Basic `pd.merge()` — Inner Join

```python
import pandas as pd

employees = pd.DataFrame({
    "emp_id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "Dave"],
    "dept_id": [10, 20, 10, 30],
})

departments = pd.DataFrame({
    "dept_id": [10, 20, 40],
    "dept_name": ["IT", "HR", "FIN"],
})

# INNER JOIN — only emp with matching dept
inner = pd.merge(employees, departments, on="dept_id", how="inner")
print(inner)
#    emp_id     name  dept_id dept_name
# 0       1    Alice       10        IT
# 1       3  Charlie       10        IT
# 2       2      Bob       20        HR
```

---

### 2️⃣ All Join Types

```python
# LEFT JOIN — all employees, NaN for unmatched depts
left = pd.merge(employees, departments, on="dept_id", how="left")
print(left)
# Dave has dept_id=30 which has no match → dept_name = NaN

# RIGHT JOIN — all departments, NaN for depts with no employees
right = pd.merge(employees, departments, on="dept_id", how="right")

# OUTER JOIN — all rows from both sides
outer = pd.merge(employees, departments, on="dept_id", how="outer")

# CROSS JOIN — every row × every row (Cartesian product)
cross = pd.merge(employees.assign(key=1), departments.assign(key=1), on="key").drop("key", axis=1)
```

---

### 3️⃣ ETL: Merge on Different Column Names

```python
orders = pd.DataFrame({
    "order_id": ["ORD-001", "ORD-002", "ORD-003"],
    "customer_ref": [101, 102, 999],  # FK to customers
    "amount": [1500, 250, 800],
})

customers = pd.DataFrame({
    "cust_id": [101, 102, 103],  # PK
    "cust_name": ["Alice Corp", "Bob Ltd", "Charlie Inc"],
    "region": ["NORTH", "SOUTH", "EAST"],
})

# left_on / right_on when key column names differ
merged = pd.merge(
    orders, customers,
    left_on="customer_ref",
    right_on="cust_id",
    how="left",
)
# Drop redundant key column
merged.drop("cust_id", axis=1, inplace=True)
print(merged)
```

---

### 4️⃣ ETL: Using `indicator=True` to Audit Joins

```python
# Find records that didn't match (ETL validation)
merged_with_indicator = pd.merge(
    orders, customers,
    left_on="customer_ref",
    right_on="cust_id",
    how="left",
    indicator=True,  # Adds '_merge' column
)

# Separate matched from unmatched
matched = merged_with_indicator[merged_with_indicator["_merge"] == "both"]
unmatched = merged_with_indicator[merged_with_indicator["_merge"] == "left_only"]

print(f"Matched: {len(matched)}, Unmatched/Orphaned: {len(unmatched)}")
print(unmatched[["order_id", "customer_ref"]])
```

---

### 5️⃣ ETL: `validate` — Enforce Cardinality

```python
# Validate that merge is Many-to-One (many orders per customer, one customer per order)
try:
    validated = pd.merge(
        orders, customers,
        left_on="customer_ref",
        right_on="cust_id",
        how="left",
        validate="many_to_one",  # Raises if cardinality is wrong
    )
    print("Join cardinality validated: many-to-one")
except pd.errors.MergeError as e:
    print(f"Cardinality violation: {e}")
```

---

### 6️⃣ `pd.concat()` — Stack Multiple DataFrames (ETL Union)

```python
# Simulate monthly files
jan = pd.DataFrame({"date": ["2024-01-01"], "sales": [1000], "region": ["NORTH"]})
feb = pd.DataFrame({"date": ["2024-02-01"], "sales": [1200], "region": ["SOUTH"]})
mar = pd.DataFrame({"date": ["2024-03-01"], "sales": [900],  "region": ["EAST"]})

# Vertical stack (UNION ALL equivalent)
combined = pd.concat([jan, feb, mar], ignore_index=True)
print(combined)

# Horizontal concat (add columns side by side)
df_ids = pd.DataFrame({"id": [1, 2, 3]})
df_vals = pd.DataFrame({"value": [10, 20, 30]})
combined_cols = pd.concat([df_ids, df_vals], axis=1)

# Stack many files from a folder
from pathlib import Path
all_dfs = [pd.read_csv(f) for f in Path("monthly_sales/").glob("*.csv")]
full_df = pd.concat(all_dfs, ignore_index=True)
print(f"Total records: {len(full_df)}")
```

---

### 7️⃣ ETL: Multi-Table Lookup Chain (Star Schema Join)

```python
# Simulate star schema join: fact table + multiple dimension tables
fact_sales = pd.DataFrame({
    "sale_id": [1, 2, 3],
    "product_id": [101, 102, 101],
    "customer_id": [201, 202, 203],
    "date_id": [20240101, 20240102, 20240101],
    "quantity": [5, 3, 8],
    "amount": [500, 300, 800],
})

dim_products = pd.DataFrame({
    "product_id": [101, 102],
    "product_name": ["Widget A", "Widget B"],
    "category": ["Electronics", "Accessories"],
})

dim_customers = pd.DataFrame({
    "customer_id": [201, 202, 203],
    "customer_name": ["Alice Corp", "Bob Ltd", "Charlie Inc"],
    "region": ["NORTH", "SOUTH", "EAST"],
})

# Chain merges to build enriched fact table
enriched = (
    fact_sales
    .merge(dim_products, on="product_id", how="left")
    .merge(dim_customers, on="customer_id", how="left")
)
print(enriched.columns.tolist())
print(enriched.head())
```

---

## 🏭 ETL Use Cases

| Join Type                | ETL Use Case                                    |
| ------------------------ | ----------------------------------------------- |
| Inner join               | Validated fact-dim joins (only matched data)    |
| Left join                | Keep all facts, enrich with optional dimensions |
| Left join + `indicator`  | Identify orphaned records / data quality check  |
| `validate='many_to_one'` | Enforce referential integrity before loading    |
| `pd.concat()`            | Stack monthly/daily files, UNION ALL            |
| Multi-join chain         | Star schema enrichment (fact + multiple dims)   |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Many-to-many join causes row explosion
orders = pd.DataFrame({"key": [1, 1, 2], "val": ["a", "b", "c"]})
prods  = pd.DataFrame({"key": [1, 1, 2], "val": ["x", "y", "z"]})
result = pd.merge(orders, prods, on="key")
# key=1 has 2 rows each → 4 combined rows (2×2)! Original had 2+2=4, now 5!

# RIGHT: Always check shape before and after merge
print(f"Before: {len(orders)} rows")
merged = pd.merge(orders, prods, on="key")
print(f"After: {len(merged)} rows")  # Alert if unexpected increase!

# WRONG: Concatenating DataFrames with mismatched columns
df1 = pd.DataFrame({"a": [1], "b": [2]})
df2 = pd.DataFrame({"a": [3], "c": [4]})  # Different columns!
result = pd.concat([df1, df2])
# b=NaN for df2 rows, c=NaN for df1 rows — silently introduces nulls

# RIGHT: Align columns before concat
all_cols = sorted(set(df1.columns) | set(df2.columns))
df1 = df1.reindex(columns=all_cols, fill_value=0)
df2 = df2.reindex(columns=all_cols, fill_value=0)
result = pd.concat([df1, df2], ignore_index=True)
```
