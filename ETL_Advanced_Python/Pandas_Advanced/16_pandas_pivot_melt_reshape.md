# 🔀 Pandas Pivot, Melt & Reshape (ETL Context)

---

## 🤔 What Is Data Reshaping?

Data arrives in two shapes:

- **Wide format**: One row per entity, each measurement is its own column
- **Long format**: Multiple rows per entity, one column for measurement name, one for value

ETL often needs to **convert between them**. Pivot goes long→wide; melt goes wide→long.

```
LONG FORMAT:           WIDE FORMAT:
name   month  sales    name   Jan  Feb  Mar
Alice  Jan    1000     Alice 1000 1500  800
Alice  Feb    1500     Bob    500  700  900
Alice  Mar     800
Bob    Jan     500
Bob    Feb     700
Bob    Mar     900
```

---

## 💻 Example 1: `pivot_table` — Long → Wide (Like Excel Pivot)

```python
import pandas as pd

df_long = pd.DataFrame({
    "region":  ["NORTH","NORTH","SOUTH","SOUTH","EAST","EAST"],
    "product": ["Widget","Gadget","Widget","Gadget","Widget","Gadget"],
    "revenue": [1500,    800,    1200,    600,    900,    400],
})

# pivot_table: index=rows, columns=cols, values=cell content, aggfunc=how to aggregate
pivot = pd.pivot_table(
    df_long,
    index   = "region",       # One row per region
    columns = "product",      # One column per product
    values  = "revenue",      # Cell = revenue
    aggfunc = "sum",          # Sum if multiple rows match
    fill_value = 0,           # Replace NaN with 0
)
print(pivot)
# product  Gadget  Widget
# region
# EAST        400     900
# NORTH       800    1500
# SOUTH       600    1200

# Flatten multi-level column names if needed
pivot.columns = [str(c) for c in pivot.columns]
pivot = pivot.reset_index()
```

---

## 💻 Example 2: `melt` — Wide → Long (Normalize Columns)

```python
df_wide = pd.DataFrame({
    "employee": ["Alice", "Bob"],
    "Q1_sales": [10000,   8000],
    "Q2_sales": [12000,   9000],
    "Q3_sales": [ 9000,   7500],
})

# melt: unpivot Q1/Q2/Q3 columns into rows
df_long = pd.melt(
    df_wide,
    id_vars    = ["employee"],   # Keep this column unchanged
    value_vars = ["Q1_sales", "Q2_sales", "Q3_sales"],  # Columns to unpivot
    var_name   = "quarter",      # New column for the old column names
    value_name = "sales",        # New column for the values
)
print(df_long)
# employee quarter  sales
#    Alice Q1_sales  10000
#    Alice Q2_sales  12000
#    Alice Q3_sales   9000
#      Bob Q1_sales   8000
#    ...

# Clean up the quarter column
df_long["quarter"] = df_long["quarter"].str.replace("_sales", "")
```

---

## 💻 Example 3: `stack` and `unstack` — MultiIndex Reshaping

```python
import pandas as pd

# Create a MultiIndex DataFrame
df = pd.DataFrame({
    ("NORTH", "Q1"): [1000, 800],
    ("NORTH", "Q2"): [1200, 900],
    ("SOUTH", "Q1"): [ 700, 600],
}, index=["Alice", "Bob"])
df.columns = pd.MultiIndex.from_tuples(df.columns, names=["Region", "Quarter"])

# stack(): move innermost column level into rows
df_stacked = df.stack("Quarter")
print(df_stacked.head())

# unstack(): move innermost row level into columns (inverse of stack)
df_unstacked = df_stacked.unstack("Quarter")
print(df_unstacked)
```

---

## 💻 Example 4: ETL Use Case — Normalize Monthly Report

```python
import pandas as pd

# Source: wide format (one column per month) — from Excel report
wide = pd.DataFrame({
    "dept":     ["IT",  "HR",  "FIN"],
    "Jan_2024": [50000, 40000, 60000],
    "Feb_2024": [52000, 41000, 61000],
    "Mar_2024": [55000, 43000, 63000],
})

# Transform: normalize to long format for data warehouse
long = pd.melt(
    wide,
    id_vars   = ["dept"],
    var_name  = "month_label",
    value_name= "budget",
)

# Parse the month column into a proper date
long["report_month"] = pd.to_datetime(long["month_label"], format="%b_%Y")
long = long.drop(columns=["month_label"])  # Remove the raw string column

# Sort and clean
long = long.sort_values(["report_month", "dept"]).reset_index(drop=True)
print(long)

# Load to warehouse in long format
# long.to_sql("dept_monthly_budget", engine, if_exists="append", index=False)
```

---

## 💻 Example 5: `pd.crosstab` — Quick Contingency Table

```python
import pandas as pd

df = pd.DataFrame({
    "region": ["NORTH","NORTH","SOUTH","EAST","NORTH","SOUTH"],
    "status": ["Active","Inactive","Active","Active","Active","Inactive"],
    "sales":  [1000,    500,      800,    900,    1200,   600],
})

# Count occurrences (like value_counts for two dimensions)
ct = pd.crosstab(df["region"], df["status"])
print(ct)
# status  Active  Inactive
# region
# EAST         1         0
# NORTH        3         1
# SOUTH        1         1

# With values and aggregation
ct_total = pd.crosstab(df["region"], df["status"],
                        values=df["sales"], aggfunc="sum")
print(ct_total)
```

---

## 🏭 ETL Use Cases Summary

| Method            | Direction   | Use Case                            |
| ----------------- | ----------- | ----------------------------------- |
| `pivot_table`     | Long → Wide | Quarterly KPIs per region/product   |
| `melt`            | Wide → Long | Normalize Excel reports for DB load |
| `stack / unstack` | Either      | MultiIndex reshaping                |
| `crosstab`        | Count       | Frequency tables for data profiling |

---

## ⚠️ Common Mistakes

```python
# ❌ Not handling NaN in pivot — missing combinations become NaN
pivot = pd.pivot_table(df, index="region", columns="product", values="revenue")
# ✅ Fill with a sensible default
pivot = pd.pivot_table(df, index="region", columns="product",
                        values="revenue", fill_value=0)

# ❌ Forgetting reset_index after pivot — result has region in index
# ✅
pivot = pivot.reset_index()
```
