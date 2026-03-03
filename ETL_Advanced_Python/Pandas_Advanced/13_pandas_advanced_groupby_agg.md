# 📊 Pandas GroupBy & Aggregation (ETL Context)

---

## 🤔 What Is GroupBy?

GroupBy is the split-apply-combine pattern:

1. **Split**: Divide the DataFrame into groups (e.g., by region)
2. **Apply**: Apply a function to each group (e.g., sum all amounts)
3. **Combine**: Merge the results back into one DataFrame

> 💡 **Analogy**: Imagine sorting receipts by month into separate piles (split), totalling each pile (apply), and then arranging the monthly totals into a report (combine).

---

## 🧱 Basic GroupBy Syntax

```python
import pandas as pd

df = pd.DataFrame({
    "region":  ["NORTH", "SOUTH", "NORTH", "EAST", "SOUTH", "NORTH"],
    "product": ["Widget", "Gadget", "Widget", "Widget", "Gadget", "Gadget"],
    "qty":     [10, 5, 8, 12, 3, 7],
    "revenue": [1500, 800, 1200, 1800, 450, 1050],
})

# Basic: GroupBy one column, aggregate one column
total_by_region = df.groupby("region")["revenue"].sum()
print(total_by_region)
# region
# EAST     1800
# NORTH    3750
# SOUTH    1250
# Name: revenue, dtype: int64

# GroupBy multiple columns
multi_group = df.groupby(["region", "product"])["revenue"].sum()
print(multi_group)

# GroupBy → multiple aggregations
summary = df.groupby("region").agg(
    total_revenue=("revenue", "sum"),
    total_qty=("qty", "sum"),
    avg_revenue=("revenue", "mean"),
    order_count=("revenue", "count"),
).reset_index()    # reset_index() turns the group keys back into columns
print(summary)
```

---

## 💻 Example 1: Named Aggregations (Pandas 0.25+)

Named aggregations give your result columns clear, meaningful names immediately:

```python
result = df.groupby("region").agg(
    total_revenue  = ("revenue", "sum"),    # new_col_name = (source_col, function)
    avg_revenue    = ("revenue", "mean"),
    min_qty        = ("qty", "min"),
    max_qty        = ("qty", "max"),
    num_orders     = ("revenue", "count"),
).round(2).reset_index()

print(result)
#   region  total_revenue  avg_revenue  min_qty  max_qty  num_orders
# 0   EAST           1800       1800.0       12       12           1
# 1  NORTH           3750       1250.0        7       10           3
# 2  SOUTH           1250        625.0        3        5           2
```

---

## 💻 Example 2: Custom Aggregation Functions

```python
import numpy as np

# Custom function: apply any function to the group
def revenue_range(series):
    """Max - Min revenue within a group."""
    return series.max() - series.min()

result = df.groupby("region")["revenue"].agg(
    total="sum",
    spread=revenue_range,   # Pass function object (not call!)
    p75=lambda x: x.quantile(0.75),
)
print(result)
```

---

## 💻 Example 3: `transform` — Add Group Metric Back to Original DataFrame

`agg()` reduces groups to one row per group. `transform()` returns a value per original row — great for adding a group-level summary to each record:

```python
# Use case: Add "region total" column to each row
df["region_total"]   = df.groupby("region")["revenue"].transform("sum")
df["region_mean"]    = df.groupby("region")["revenue"].transform("mean")
df["pct_of_region"]  = (df["revenue"] / df["region_total"] * 100).round(1)

print(df[["region", "revenue", "region_total", "pct_of_region"]])
# Each row keeps its own data AND gains the group total/percentage
```

---

## 💻 Example 4: `pd.Grouper` — Time-Based Grouping

```python
import pandas as pd

df_sales = pd.DataFrame({
    "date":    pd.date_range("2024-01-01", periods=365, freq="D"),
    "revenue": range(365),
})
df_sales = df_sales.set_index("date")

# Group by month
monthly = df_sales.groupby(pd.Grouper(freq="ME"))["revenue"].sum()
print(monthly.head())

# Group by quarter
quarterly = df_sales.groupby(pd.Grouper(freq="QE"))["revenue"].sum()
print(quarterly)
```

---

## 💻 Example 5: Filter Groups by Group-Level Condition

```python
# Keep only groups where total revenue > 2000
high_revenue_regions = df.groupby("region").filter(
    lambda grp: grp["revenue"].sum() > 2000
)
print(high_revenue_regions)     # Only NORTH rows remain (total=3750)
```

---

## 💻 Example 6: Full ETL Aggregation Pipeline

```python
import pandas as pd

raw = pd.read_csv("sales.csv", parse_dates=["order_date"])

summary = (
    raw[raw["status"] == "COMPLETE"]             # Filter: only completed orders
    .assign(year_month=raw["order_date"].dt.to_period("M"))  # Add year-month column
    .groupby(["year_month", "region"])               # Group
    .agg(
        total_revenue = ("amount",      "sum"),
        avg_deal_size = ("amount",      "mean"),
        num_orders    = ("order_id",    "nunique"),  # Count unique orders
        total_qty     = ("qty",         "sum"),
    )
    .round(2)
    .reset_index()
    .sort_values(["year_month", "total_revenue"], ascending=[True, False])
)
summary.to_csv("monthly_region_summary.csv", index=False)
```

---

## 🏭 ETL Use Cases Summary

| Operation               | Use Case                                          |
| ----------------------- | ------------------------------------------------- |
| `groupby().agg()`       | KPI summaries: revenue, counts per region/product |
| Named aggregations      | Clean, readable result column names               |
| `transform()`           | Add group-level metrics to individual rows        |
| `pd.Grouper(freq="ME")` | Monthly/quarterly time-series aggregation         |
| `.filter(lambda)`       | Discard low-volume groups before reporting        |

---

## 🧠 Must-Remember Points

| Rule                             | Remember                                             |
| -------------------------------- | ---------------------------------------------------- |
| `reset_index()`                  | Always after `groupby().agg()` to flatten MultiIndex |
| `transform` vs `agg`             | `agg` → fewer rows; `transform` → same shape         |
| `nunique`                        | Count unique values (use for order IDs, not `count`) |
| `pd.Grouper`                     | For datetime grouping by period                      |
| Avoid `apply` for built-in funcs | Use `agg("sum")` not `apply(lambda g: g.sum())`      |

---

## ⚠️ Common Mistakes

```python
# ❌ Forgetting reset_index — result has MultiIndex columns
result = df.groupby("region")["revenue"].sum()
print(result["NORTH"])   # Works but awkward

# ✅ Use reset_index for flat, usable DataFrame
result = df.groupby("region")["revenue"].sum().reset_index()
print(result[result["region"] == "NORTH"])

# ❌ Using count() when you need nunique()
raw.groupby("region")["order_id"].count()    # Counts rows (includes duplicates)
# ✅
raw.groupby("region")["order_id"].nunique()  # Counts distinct order IDs
```
