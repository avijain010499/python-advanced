# 🔤 Pandas String Operations (ETL Context)

---

## 📖 Explanation

Pandas provides vectorized string methods via the **`.str` accessor** on string columns. These are essential for cleaning, validating, parsing, and transforming text data in ETL pipelines — far faster than row-by-row Python loops.

---

## 🧠 Must-Remember Points

- `.str` accessor vectorizes string operations over a Series — no need for `.apply(lambda)`.
- All `.str` methods handle `NaN` gracefully — they propagate `NaN` silently.
- Use `.str.strip()` immediately when reading data — trailing whitespace causes silent merge failures.
- `.str.contains()` with `na=False` prevents NaN-related errors.
- `.str.extract()` uses regex and returns captured groups as columns.
- `.str.extractall()` returns all matches per row (MultiIndex result).
- `.str.split(expand=True)` returns a DataFrame from split result.
- `case=False` in `.str.contains()` for case-insensitive matching.
- `.str.cat(sep=",")` concatenates strings in a Series.
- String columns stored as `pd.StringDtype()` (or `"string"`) handle NA better than `object` dtype.

---

## 💻 Code Examples

### 1️⃣ Basic String Cleaning

```python
import pandas as pd

df = pd.DataFrame({
    "name": ["  alice smith  ", "BOB JONES", "  Charlie Brown", None],
    "email": ["Alice@EXAMPLE.COM", "bob@company.org", "  charlie@test.com  ", None],
    "dept_code": ["  IT-001 ", "HR-002", "FIN-003  ", " IT-001"],
})

# Strip whitespace
df["name"] = df["name"].str.strip()
df["email"] = df["email"].str.strip().str.lower()
df["dept_code"] = df["dept_code"].str.strip()

# Change case
df["name_standard"] = df["name"].str.title()   # Title Case
df["name_upper"] = df["name"].str.upper()

print(df[["name_standard", "email", "dept_code"]])
```

---

### 2️⃣ String Splitting and Extraction

```python
df = pd.DataFrame({
    "full_name": ["Alice Smith", "Bob O'Brien", "Charlie Jr. Brown Jr."],
    "phone_raw": ["555-123-4567", "(800) 555-9999", "+1-800-555-0001"],
    "product_code": ["PROD-IT-001-LARGE", "PROD-HR-002-SMALL", "PROD-FIN-003-MED"],
})

# Split full name into first and last
name_split = df["full_name"].str.split(" ", n=1, expand=True)
df["first_name"] = name_split[0]
df["last_name"] = name_split[1]

# Extract parts of product code using regex
code_parts = df["product_code"].str.extract(
    r"PROD-(?P<dept>[A-Z]+)-(?P<id>\d+)-(?P<size>\w+)"
)
df = pd.concat([df, code_parts], axis=1)
print(df[["product_code", "dept", "id", "size"]])
```

---

### 3️⃣ ETL: Data Validation with `.str.match()` / `.str.contains()`

```python
import pandas as pd

df = pd.DataFrame({
    "email": ["alice@example.com", "bad-email", "bob@company.org", None, "invalid@"],
    "postal_code": ["12345", "1234", "90210", "AB123", None],
    "phone": ["555-123-4567", "800-555-9999", "1234567", None],
})

EMAIL_REGEX = r"^[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}$"
POSTAL_REGEX = r"^\d{5}(-\d{4})?$"
PHONE_REGEX = r"^\d{3}[-.]?\d{3}[-.]?\d{4}$"

df["email_valid"] = df["email"].str.match(EMAIL_REGEX, na=False)
df["postal_valid"] = df["postal_code"].str.match(POSTAL_REGEX, na=False)
df["phone_valid"] = df["phone"].str.match(PHONE_REGEX, na=False)
df["all_valid"] = df["email_valid"] & df["postal_valid"] & df["phone_valid"]

print(df[["email", "email_valid", "postal_valid", "phone_valid", "all_valid"]])
```

---

### 4️⃣ ETL: String Replacement and Normalization

```python
df = pd.DataFrame({
    "description": [
        "Revenue: $1,500.00 (provisional)",
        "Cost  Centre:  HR-20",
        "Amount = 2000.00 USD",
        "N/A - not available",
    ],
    "category": ["  Technology ", "Human  Resources", "Finance", "UNKNOWN"],
})

# Remove special characters
df["desc_clean"] = (
    df["description"]
    .str.replace(r"[\$\(\)]", "", regex=True)   # Remove $, (, )
    .str.replace(r"\s+", " ", regex=True)        # Normalize whitespace
    .str.strip()
)

# Normalize category: multiple spaces, title case
df["category_clean"] = (
    df["category"]
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
    .str.title()
)

# Replace N/A text with actual NaN
import numpy as np
df["desc_clean"] = df["desc_clean"].replace(
    r"^N/A.*", np.nan, regex=True
)
print(df[["description", "desc_clean", "category_clean"]])
```

---

### 5️⃣ ETL: Parse Fixed-Width / Delimited String Columns

```python
# Raw data from legacy system (fixed column positions)
df = pd.DataFrame({
    "raw_record": [
        "ALICE    IT  07500020240115",
        "BOB      HR  06000020240116",
        "CHARLIE  FIN 08000020240117",
    ],
})

# Slice fixed-width columns
df["name"] = df["raw_record"].str[0:9].str.strip()
df["dept"] = df["raw_record"].str[9:13].str.strip()
df["salary"] = df["raw_record"].str[13:19].astype(int)
df["date"] = pd.to_datetime(df["raw_record"].str[19:27], format="%Y%m%d")
print(df[["name", "dept", "salary", "date"]])
```

---

### 6️⃣ ETL: String Aggregation with `.str.cat()`

```python
df = pd.DataFrame({
    "first": ["Alice", "Bob", "Charlie"],
    "middle": ["M", None, "P"],
    "last": ["Smith", "Jones", "Brown"],
    "dept": ["IT", "HR", "FIN"],
    "level": ["Sr", "Jr", "Sr"],
})

# Concatenate with separator (NaN in middle → skipped)
df["full_name"] = (
    df["first"].str.cat(df["middle"], sep=" ", na_rep="")
    .str.cat(df["last"], sep=" ")
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

# Create composite key
df["role_key"] = df["dept"] + "_" + df["level"]
print(df[["full_name", "role_key"]])
```

---

## 🏭 ETL Use Cases

| `.str` Method                    | ETL Use Case                                       |
| -------------------------------- | -------------------------------------------------- |
| `.str.strip()`                   | Remove whitespace from source data (always first!) |
| `.str.lower()` / `.upper()`      | Normalize case for consistent joins/lookups        |
| `.str.split(expand=True)`        | Parse composite columns (name→first+last)          |
| `.str.extract(regex)`            | Extract structured data from free-text columns     |
| `.str.contains(regex, na=False)` | Validate/filter records on pattern                 |
| `.str.replace(regex)`            | Clean currency, special chars, whitespace          |
| `.str[start:end]`                | Parse fixed-width file formats                     |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Using .apply(lambda) for simple string ops (slow!)
df["name"] = df["name"].apply(lambda x: x.strip().lower() if x else x)

# RIGHT: Vectorized .str methods (10-100x faster)
df["name"] = df["name"].str.strip().str.lower()

# WRONG: .str.contains() without na=False crashes on NaN
mask = df["email"].str.contains("@")   # Raises error if email is NaN!

# RIGHT: Always pass na=False for safety
mask = df["email"].str.contains("@", na=False)

# WRONG: Chaining .str. on non-string dtype
df["id"].str.strip()  # AttributeError if 'id' is int dtype!

# RIGHT: Convert to string first if needed
df["id"].astype(str).str.strip()
```
