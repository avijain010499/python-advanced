# 🔗 Pandas Merge & Join (ETL Context)

---

## 🤔 What Is Merging?

In ETL, data almost never comes from one place. You need to combine:

- Sales orders with customer info
- Transactions with product details
- Facts with dimension tables

`pd.merge()` is the pandas equivalent of a SQL JOIN.

---

## 🧱 Types of Joins — Visual Guide

```
Left Table (A):          Right Table (B):
  id  name               id  score
   1  Alice               1    90
   2  Bob                 3    85
   3  Charlie

INNER JOIN (keep only matches):        → rows: 1 (Alice,90), 3 (Charlie,85)
LEFT JOIN  (keep all left + matches):  → rows: 1 (Alice,90), 2 (Bob,NaN), 3 (Charlie,85)
RIGHT JOIN (keep all right + matches): → rows: 1 (Alice,90), 3 (Charlie,85)
OUTER JOIN (keep ALL rows):            → rows: 1,2,3 (Bob gets NaN for score)
```

---

## 💻 Example 1: `pd.merge()` Basics

```python
import pandas as pd

orders = pd.DataFrame({
    "order_id":   ["ORD-1", "ORD-2", "ORD-3", "ORD-4"],
    "customer_id":[101,      102,      101,      103],
    "amount":     [500,       200,      800,      300],
})

customers = pd.DataFrame({
    "customer_id": [101,          102,         104],
    "name":        ["Alice",      "Bob",       "Dave"],
    "region":      ["NORTH",      "SOUTH",     "EAST"],
})

# INNER JOIN — only orders with a matching customer (loses ORD-4, customer 103 missing)
inner = pd.merge(orders, customers, on="customer_id", how="inner")

# LEFT JOIN — all orders, NaN where customer not found
left = pd.merge(orders, customers, on="customer_id", how="left")

# Different column names in each table
orders2 = orders.rename(columns={"customer_id": "cust_id"})
merged = pd.merge(
    orders2, customers,
    left_on="cust_id",    # Column name in left table
    right_on="customer_id",  # Column name in right table
    how="left"
)
```

---

## 💻 Example 2: Multi-Key Join (Composite Key)

```python
sales = pd.DataFrame({
    "product": ["Widget","Widget","Gadget"],
    "region":  ["NORTH", "SOUTH", "NORTH"],
    "revenue": [1500,     800,    1200],
})

targets = pd.DataFrame({
    "product": ["Widget","Widget","Gadget"],
    "region":  ["NORTH", "SOUTH", "NORTH"],
    "target":  [2000,     1000,   1000],
})

# Join on multiple columns — both must match
result = pd.merge(sales, targets, on=["product", "region"])
result["pct_achieved"] = (result["revenue"] / result["target"] * 100).round(1)
print(result)
```

---

## 💻 Example 3: `indicator=True` — Debug Missing Matches

```python
result = pd.merge(orders, customers, on="customer_id", how="outer", indicator=True)
#  _merge column shows: 'both', 'left_only', 'right_only'

orphan_orders    = result[result["_merge"] == "left_only"]    # Orders with no customer
orphan_customers = result[result["_merge"] == "right_only"]   # Customers with no orders

print(f"Orders with no matching customer: {len(orphan_orders)}")
print(f"Customers with no orders: {len(orphan_customers)}")
```

---

## 💻 Example 4: `pd.concat()` — Stack DataFrames Vertically

```python
jan = pd.DataFrame({"date": ["2024-01-01"], "sales": [1000]})
feb = pd.DataFrame({"date": ["2024-02-01"], "sales": [1500]})
mar = pd.DataFrame({"date": ["2024-03-01"], "sales": [1200]})

# Stack rows from all months
all_months = pd.concat([jan, feb, mar], ignore_index=True)
# ignore_index=True gives clean 0,1,2,... index instead of repeating 0,0,0

# Keys: add a label for which source each row came from
labeled = pd.concat([jan, feb, mar], keys=["Jan", "Feb", "Mar"])
print(labeled)   # MultiIndex with month + original row index
```

---

## 💻 Example 5: Full ETL Fact-Dimension Join (Star Schema)

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://user:pass@localhost/etl_dw")

# Extract
fact_sales     = pd.read_sql("SELECT * FROM fact_sales",     engine)
dim_customer   = pd.read_sql("SELECT * FROM dim_customer",   engine)
dim_product    = pd.read_sql("SELECT * FROM dim_product",    engine)

# Transform: enrich fact with dimensions
enriched = (
    fact_sales
    .merge(dim_customer[["customer_id","name","region","segment"]],
           on="customer_id", how="left")
    .merge(dim_product[["product_id","product_name","category"]],
           on="product_id", how="left")
)
# Check for unexpected missing dimension data
missing_cust = enriched["name"].isna().sum()
if missing_cust > 0:
    print(f"⚠️  {missing_cust} orders with no customer match!")

# Load
enriched.to_sql("fact_sales_enriched", engine, if_exists="replace",
                index=False, method="multi", chunksize=5000)
print(f"✅ Loaded {len(enriched):,} enriched rows")
```

---

## 🏭 ETL Use Cases Summary

| Method                  | Use Case                                        |
| ----------------------- | ----------------------------------------------- |
| `merge(how="inner")`    | Strict join — only matched records              |
| `merge(how="left")`     | Keep all source records, enrich where possible  |
| `merge(indicator=True)` | Debug unmatched rows from both sides            |
| Multi-key merge         | Composite primary keys (product+region)         |
| `pd.concat()`           | Stack monthly/regional files into one DataFrame |

---

## 🧠 Must-Remember Points

| Rule                          | Remember                                                    |
| ----------------------------- | ----------------------------------------------------------- |
| Default `how="inner"`         | Silently drops rows with no match! Use `"left"` if unsure   |
| `indicator=True`              | Debug merge — shows which rows matched                      |
| `ignore_index=True` in concat | Clean integer index for combined DataFrame                  |
| Validate after merge          | Check `len(result)` vs `len(left)` — unexpected duplicates? |

---

## ⚠️ Common Mistakes

```python
# ❌ Row count explodes — many-to-many join (duplicate keys on both sides)
# orders has 1000 rows of order_id=1, customers has 500 rows of customer_id=1
# Result: 1000 × 500 = 500,000 rows!
merged = pd.merge(orders, customers, on="customer_id")
assert len(merged) == len(orders), f"Unexpected row explosion: {len(merged)}"

# ✅ FIX: Deduplicate the dimension table first
customers_unique = customers.drop_duplicates("customer_id")
merged = pd.merge(orders, customers_unique, on="customer_id", how="left")
```
