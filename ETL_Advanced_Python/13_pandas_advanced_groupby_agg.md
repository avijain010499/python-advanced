# 📊 Pandas Advanced — GroupBy & Aggregation (ETL Context)

---

## 📖 Explanation

`GroupBy` is one of the most powerful operations in Pandas — it splits data into groups, applies a function to each group, and combines the results (Split-Apply-Combine pattern). It's the Pandas equivalent of SQL's `GROUP BY`.

---

## 🧠 Must-Remember Points

- **Split-Apply-Combine**: `groupby()` splits, `.agg()/.transform()/.apply()` applies, Pandas combines.
- `groupby()` returns a `DataFrameGroupBy` object — not a DataFrame until you aggregate.
- `agg()` applies different functions to different columns in one call.
- `transform()` returns a DataFrame with the **same shape** as the original — used for broadcasting aggregates back.
- `apply()` is the most flexible but slowest GroupBy method.
- `as_index=False` in `groupby()` prevents the group keys from becoming the index.
- `observed=True` for categorical groupby to only show observed categories.
- `groupby(level=...)` groups on MultiIndex levels.
- `named aggregations` (Python keyword syntax) give cleaner output column names.
- `pd.Grouper(freq='M')` groups datetime columns by time period.

---

## 💻 Code Examples

### 1️⃣ Basic GroupBy & Aggregation

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({
    "department": ["IT", "HR", "IT", "FIN", "HR", "FIN", "IT"],
    "employee": ["Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace"],
    "salary": [75000, 60000, 80000, 65000, 62000, 70000, 90000],
    "performance_score": [4.5, 3.8, 4.2, 4.0, 3.5, 4.1, 4.8],
})

# Basic aggregation
dept_stats = df.groupby("department")["salary"].mean()
print(dept_stats)
# department
# FIN     67500.0
# HR      61000.0
# IT      81666.7

# Multiple aggregations on one column
df.groupby("department")["salary"].agg(["mean", "min", "max", "count"])
```

---

### 2️⃣ Multi-Column Aggregation with Named Aggregations

```python
# Named aggregations — cleaner output column names
dept_summary = df.groupby("department", as_index=False).agg(
    avg_salary=("salary", "mean"),
    max_salary=("salary", "max"),
    min_salary=("salary", "min"),
    headcount=("employee", "count"),
    avg_performance=("performance_score", "mean"),
)
print(dept_summary)
#   department  avg_salary  max_salary  min_salary  headcount  avg_performance
# 0        FIN     67500.0       70000       65000          2            4.050
# 1         HR     61000.0       62000       60000          2            3.650
# 2         IT     81666.7       90000       75000          3            4.500
```

---

### 3️⃣ Custom Aggregation Functions

```python
# Custom function in agg
def salary_range(series):
    return series.max() - series.min()

df.groupby("department")["salary"].agg(
    avg="mean",
    spread=salary_range,
    pct_75=lambda x: x.quantile(0.75),
)

# Multiple columns, different functions per column
result = df.groupby("department").agg({
    "salary": ["mean", "std"],
    "performance_score": ["mean", "max"],
    "employee": "count",
})
# Flatten multi-level column names
result.columns = ["_".join(col).strip() for col in result.columns]
result.reset_index(inplace=True)
```

---

### 4️⃣ ETL: `transform()` — Broadcast Aggregate Back to Original Shape

```python
# Add department average salary as a new column (same length as original)
df["dept_avg_salary"] = df.groupby("department")["salary"].transform("mean")
df["salary_vs_dept_avg"] = df["salary"] - df["dept_avg_salary"]
df["dept_rank"] = df.groupby("department")["salary"].transform(
    lambda x: x.rank(method="dense", ascending=False)
)
print(df[["employee", "department", "salary", "dept_avg_salary", "dept_rank"]])
```

---

### 5️⃣ ETL: GroupBy with Time Period (`pd.Grouper`)

```python
import pandas as pd

sales_df = pd.DataFrame({
    "order_date": pd.date_range("2024-01-01", periods=365, freq="D"),
    "revenue": [abs(x) * 100 for x in range(365)],
    "region": ["NORTH" if i % 2 == 0 else "SOUTH" for i in range(365)],
})
sales_df.set_index("order_date", inplace=True)

# Monthly revenue by region
monthly = sales_df.groupby(["region", pd.Grouper(freq="ME")])["revenue"].sum()
print(monthly.head(6))

# Weekly moving sum
weekly = sales_df.groupby(pd.Grouper(freq="W"))["revenue"].sum()
print(weekly.head(4))
```

---

### 6️⃣ ETL: Filter Groups Based on Aggregate Condition

```python
# filter() keeps groups where the condition is True
# Keep only departments with avg salary > 65000
high_salary_depts = df.groupby("department").filter(
    lambda g: g["salary"].mean() > 65000
)
print(high_salary_depts["department"].unique())  # ['IT', 'FIN'] or similar

# Keep departments with more than 2 employees
large_depts = df.groupby("department").filter(lambda g: len(g) > 2)
```

---

### 7️⃣ ETL: nlargest/nsmallest per Group

```python
# Top 1 earner per department
top_earners = df.sort_values("salary", ascending=False).groupby("department").head(1)
print(top_earners[["department", "employee", "salary"]])

# Top 2 scores per department using nlargest
top2_scores = df.groupby("department").apply(
    lambda g: g.nlargest(2, "performance_score")
).reset_index(drop=True)
```

---

## 🏭 ETL Use Cases

| Operation              | ETL Use Case                            |
| ---------------------- | --------------------------------------- |
| `groupby().agg()`      | Fact table aggregation, summary metrics |
| Named aggregations     | Clean dimension table creation          |
| `transform()`          | Enrich records with group-level stats   |
| `pd.Grouper(freq="M")` | Monthly/weekly/daily partitioning       |
| `filter()`             | Remove low-quality data segments        |
| `nlargest` per group   | Row selection for fact table loading    |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Iterating over groups (slow for large data)
for name, group in df.groupby("department"):
    result = group["salary"].mean()  # Vectorized agg is much faster

# RIGHT: Use .agg() directly
result = df.groupby("department")["salary"].mean()

# WRONG: Forgetting reset_index after groupby
result = df.groupby("department")["salary"].mean()
result["extra_col"] = 1  # department becomes index — can cause issues

# RIGHT: Use as_index=False or reset_index()
result = df.groupby("department", as_index=False)["salary"].mean()
```
