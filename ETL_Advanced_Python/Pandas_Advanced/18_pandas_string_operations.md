# 🔤 Pandas String Operations (ETL Context)

---

## 🤔 What Are String Operations in Pandas?

Source data is full of messy text: names with extra spaces, emails in wrong case, phone numbers in different formats, mixed values in category columns.

Pandas `.str` accessor applies string methods to **entire columns at once** — no loops needed.

> 💡 **Key rule**: `.str` methods handle `NaN` silently (NaN stays NaN, no crash). But always pass `na=False` to `.str.contains()` and `.str.match()` to avoid errors.

---

## 🧱 Most Used `.str` Methods Quick Reference

| Method                               | What It Does                        |
| ------------------------------------ | ----------------------------------- |
| `.str.strip()`                       | Remove leading/trailing whitespace  |
| `.str.lower()` / `.str.upper()`      | Change case                         |
| `.str.title()`                       | Title Case Each Word                |
| `.str.replace(old, new, regex=True)` | Replace pattern or literal          |
| `.str.contains(pattern, na=False)`   | Boolean mask — does it contain?     |
| `.str.match(pattern, na=False)`      | Boolean mask — does it start with?  |
| `.str.extract(r"(group)")`           | Extract regex group into new column |
| `.str.split(sep, expand=True)`       | Split into multiple columns         |
| `.str[start:end]`                    | Slice (for fixed-width data)        |

---

## 💻 Example 1: Basic Cleaning

```python
import pandas as pd

df = pd.DataFrame({
    "name":  ["  ALICE SMITH  ", "bob jones", " Charlie Brown "],
    "email": ["ALICE@EXAMPLE.COM", " bob@company.org  ", "charlie@test.COM"],
    "dept":  ["  IT ", "hr", " Finance "],
})

# Chain operations — always strip FIRST, then change case
df["name_clean"]  = df["name"].str.strip().str.title()
df["email_clean"] = df["email"].str.strip().str.lower()
df["dept_clean"]  = df["dept"].str.strip().str.upper()

print(df[["name_clean", "email_clean", "dept_clean"]])
```

---

## 💻 Example 2: Validation with `.str.match()` and `.str.contains()`

```python
import pandas as pd

df = pd.DataFrame({
    "email":  ["alice@example.com", "bad-email", "bob@org.io", None],
    "phone":  ["555-123-4567", "800 555 9999", "1234", None],
    "postal": ["12345", "1234", "90210-1234", None],
})

EMAIL_RE  = r"^[\w.+\-]+@[\w\-]+\.[a-zA-Z]{2,}$"
PHONE_RE  = r"^\d{3}[-.\s]?\d{3}[-.\s]?\d{4}$"
POSTAL_RE = r"^\d{5}(-\d{4})?$"

df["email_ok"]  = df["email"].str.match(EMAIL_RE,  na=False)
df["phone_ok"]  = df["phone"].str.match(PHONE_RE,  na=False)
df["postal_ok"] = df["postal"].str.match(POSTAL_RE, na=False)
df["all_valid"] = df["email_ok"] & df["phone_ok"] & df["postal_ok"]

print(df[["email","email_ok","phone_ok","all_valid"]])
```

---

## 💻 Example 3: Extraction with `.str.extract()`

```python
import pandas as pd

df = pd.DataFrame({
    "full_name":    ["Alice M. Smith", "Bob O'Brien", "Charlie Jr. Brown"],
    "product_code": ["PROD-IT-001-LARGE", "PROD-HR-002-SMALL", "PROD-FIN-003-MED"],
    "raw_date":     ["Date: 2024-01-15", "Date: 2024-07-22", "Date: 2024-12-31"],
})

# Split name into first + last (n=1: only split once)
name_parts = df["full_name"].str.split(" ", n=1, expand=True)
df["first"] = name_parts[0]
df["last"]  = name_parts[1]

# Extract named groups from product code
code_parts = df["product_code"].str.extract(
    r"PROD-(?P<dept>[A-Z]+)-(?P<id>\d+)-(?P<size>\w+)"
)
df = pd.concat([df, code_parts], axis=1)

# Extract date from mixed-text column
df["date"] = pd.to_datetime(
    df["raw_date"].str.extract(r"(\d{4}-\d{2}-\d{2})")[0]
)

print(df[["full_name","first","last","dept","size","date"]])
```

---

## 💻 Example 4: Cleaning Text with `.str.replace()`

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({
    "amount":  ["$1,500.00", "€2,000.50", " £ 300.00 "],
    "notes":   ["Revenue (provisional)", "Cost  Centre:  HR", "N/A"],
    "category":["  Technology ", "Human  Resources", "finance"],
})

# Clean currency: remove $, €, £, commas, spaces → numeric
df["amount_clean"] = (
    df["amount"]
    .str.replace(r"[$€£,\s]", "", regex=True)
    .pipe(pd.to_numeric, errors="coerce")
)

# Normalize whitespace
df["notes_clean"] = df["notes"].str.replace(r"\s+", " ", regex=True).str.strip()

# Replace "N/A" text → real NaN
df["notes_clean"] = df["notes_clean"].replace(r"^N/A$", np.nan, regex=True)

# Normalize category
df["category_clean"] = (
    df["category"].str.replace(r"\s+", " ", regex=True).str.strip().str.title()
)
print(df[["amount_clean","notes_clean","category_clean"]])
```

---

## 💻 Example 5: Parse Fixed-Width Data

```python
import pandas as pd

# Legacy mainframe data: columns at fixed positions
df = pd.DataFrame({"raw": [
    "ALICE    IT  07500020240115",
    "BOB      HR  06000020240116",
    "CHARLIE  FIN 08000020240117",
]})

df["name"]   = df["raw"].str[0:9].str.strip()
df["dept"]   = df["raw"].str[9:13].str.strip()
df["salary"] = df["raw"].str[13:19].astype(int)
df["date"]   = pd.to_datetime(df["raw"].str[19:27], format="%Y%m%d")

print(df[["name","dept","salary","date"]])
```

---

## 🏭 ETL Use Cases Summary

| Method                          | ETL Use Case                                        |
| ------------------------------- | --------------------------------------------------- |
| `.str.strip().str.lower()`      | Normalize before joins — prevents "NORTH" ≠ "north" |
| `.str.match(regex, na=False)`   | Validate format before loading to DB                |
| `.str.extract(r"(?P<col>...)")` | Parse structured data from free text                |
| `.str.replace(regex)`           | Clean currency, special chars, whitespace           |
| `.str[start:end]` slicing       | Parse fixed-width file formats                      |

---

## ⚠️ Common Mistakes

```python
# ❌ .str.contains without na=False — crashes if column has NaN
mask = df["email"].str.contains("@")    # ValueError if any email is NaN!
# ✅
mask = df["email"].str.contains("@", na=False)

# ❌ Using apply(lambda) for simple string ops (slow!)
df["name"] = df["name"].apply(lambda x: x.strip().lower() if x else x)
# ✅ Vectorized — 10-100x faster
df["name"] = df["name"].str.strip().str.lower()

# ❌ Calling .str on non-string dtype
df["id"].str.strip()   # AttributeError if 'id' column is int!
# ✅
df["id"].astype(str).str.strip()
```
