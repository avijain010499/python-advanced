# 🔎 Regular Expressions in Python (ETL Context)

---

## 📖 Explanation

Regular expressions (regex) are patterns used to search, match, extract, and transform text. In ETL, they're essential for data validation, cleaning, and parsing unstructured or semi-structured data.

Python's `re` module provides full regex support.

---

## 🧠 Must-Remember Points

- `re.match()` — matches at the **beginning** of the string only.
- `re.search()` — searches the **entire** string for the first match.
- `re.findall()` — returns a **list** of all matches.
- `re.finditer()` — returns an **iterator** of match objects.
- `re.sub()` — substitutes matches with a replacement string.
- `re.compile()` — pre-compiles a regex for repeated use (much faster!).
- Use **raw strings** `r"pattern"` to avoid backslash confusion.
- **Groups** `(pattern)` capture sub-parts of a match.
- **Named groups** `(?P<name>pattern)` make code readable.
- `re.IGNORECASE` (`re.I`) and `re.MULTILINE` (`re.M`) are common flags.
- In Pandas, use `df["col"].str.extract()`, `.str.match()`, `.str.replace()` for vectorized regex.

### Quick Reference

| Pattern         | Meaning                      |
| --------------- | ---------------------------- |
| `.`             | Any character except newline |
| `^`             | Start of string              |
| `$`             | End of string                |
| `\d`            | Digit [0-9]                  |
| `\w`            | Word character [a-zA-Z0-9_]  |
| `\s`            | Whitespace                   |
| `*`             | 0 or more                    |
| `+`             | 1 or more                    |
| `?`             | 0 or 1 (optional)            |
| `{n,m}`         | Between n and m times        |
| `[abc]`         | Character class              |
| `(a\|b)`        | a or b                       |
| `(?P<name>...)` | Named capture group          |

---

## 💻 Code Examples

### 1️⃣ Basic `re` Functions

```python
import re

text = "Order ID: ORD-12345, Amount: $1,500.00, Date: 2024-01-15"

# re.search: find first occurrence anywhere
match = re.search(r"\d{4}-\d{2}-\d{2}", text)
if match:
    print(f"Date found: {match.group()}")  # 2024-01-15

# re.findall: all matches
amounts = re.findall(r"\d[\d,]*\.\d{2}", text)
print(amounts)  # ['1,500.00']

# re.match: from the beginning only
result = re.match(r"Order", text)
print(result.group() if result else "No match")  # Order
```

---

### 2️⃣ ETL: Data Validation with Regex

```python
import re

# Pre-compile patterns for reuse (much faster in loops!)
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
PHONE_PATTERN = re.compile(r"^\+?1?\s?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{4}$")
DATE_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")

def validate_record(record):
    errors = []
    if not EMAIL_PATTERN.match(record.get("email", "")):
        errors.append(f"Invalid email: {record['email']}")
    if not PHONE_PATTERN.match(str(record.get("phone", ""))):
        errors.append(f"Invalid phone: {record['phone']}")
    if not DATE_PATTERN.match(str(record.get("date", ""))):
        errors.append(f"Invalid date: {record['date']}")
    return errors

records = [
    {"email": "alice@example.com", "phone": "555-123-4567", "date": "2024-01-15"},
    {"email": "invalid-email", "phone": "1234", "date": "15-01-2024"},
]
for r in records:
    errors = validate_record(r)
    print(f"Record {r}: {errors if errors else 'VALID'}")
```

---

### 3️⃣ ETL: Data Extraction with Named Groups

```python
import re

log_line = "[2024-01-15 10:30:45] ERROR - ETL Job: sales_transform | Duration: 45.2s | Records: 10500"

# Named groups for readable extraction
LOG_PATTERN = re.compile(
    r"\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] "
    r"(?P<level>\w+) - ETL Job: (?P<job>\w+) \| "
    r"Duration: (?P<duration>[\d.]+)s \| "
    r"Records: (?P<records>\d+)"
)

match = LOG_PATTERN.match(log_line)
if match:
    data = match.groupdict()
    print(data)
    # {'timestamp': '2024-01-15 10:30:45', 'level': 'ERROR',
    #  'job': 'sales_transform', 'duration': '45.2', 'records': '10500'}
```

---

### 4️⃣ ETL: Data Cleaning with `re.sub()`

```python
import re

# Remove special characters from names
def clean_name(name):
    # Allow letters, spaces, hyphens, apostrophes only
    return re.sub(r"[^a-zA-Z\s'\-]", "", name).strip()

# Normalize whitespace
def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()

# Remove currency symbols and commas from amounts
def parse_amount(amount_str):
    cleaned = re.sub(r"[$,€£\s]", "", amount_str)
    return float(cleaned) if cleaned else 0.0

print(clean_name("Alice123 O'Brien!"))   # Alice O'Brien
print(normalize_whitespace("  Sales   Report  2024  "))  # Sales Report 2024
print(parse_amount("$1,500.75"))          # 1500.75
print(parse_amount("€ 2,000.00"))         # 2000.0
```

---

### 5️⃣ ETL: Parse Semi-Structured Log Files

```python
import re
from pathlib import Path

def parse_etl_logs(log_file):
    """Extract structured data from ETL log files."""
    ENTRY_PATTERN = re.compile(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) "
        r"\| (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL) "
        r"\| (?P<module>\w+) "
        r"\| (?P<message>.+)"
    )

    records = []
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            match = ENTRY_PATTERN.match(line.strip())
            if match:
                records.append(match.groupdict())

    errors = [r for r in records if r["level"] in ("ERROR", "CRITICAL")]
    print(f"Total log entries: {len(records)}")
    print(f"Errors found: {len(errors)}")
    return records, errors
```

---

### 6️⃣ Pandas: Vectorized Regex Operations

```python
import pandas as pd

df = pd.DataFrame({
    "email": ["alice@example.com", "bad-email", "bob@company.org", "INVALID"],
    "phone": ["555-123-4567", "(800) 555-9999", "1234", "+1-800-555-0001"],
    "amount": ["$1,500.00", "$200.50", "N/A", "$3,000.00"],
})

# Validate emails (boolean mask)
email_valid = df["email"].str.match(r"^[\w.+\-]+@[\w\-]+\.[a-zA-Z]{2,}$")
df["email_valid"] = email_valid

# Extract area code from phone
df["area_code"] = df["phone"].str.extract(r"[\(]?(\d{3})[\)]?")

# Extract numeric amount
df["amount_clean"] = df["amount"].str.replace(r"[$,]", "", regex=True)
df["amount_clean"] = pd.to_numeric(df["amount_clean"], errors="coerce")

print(df[["email", "email_valid", "area_code", "amount_clean"]])
```

---

## 🏭 ETL Use Cases

| Regex Use               | ETL Application                             |
| ----------------------- | ------------------------------------------- |
| Validation patterns     | Email, phone, date, postal code validation  |
| Data extraction         | Parse log files, unstructured text          |
| Data cleaning           | Remove special chars, normalize whitespace  |
| Named groups            | Parse complex log/data formats              |
| Pandas `.str.extract()` | Column-level regex extraction at scale      |
| `re.compile()`          | Precompile heavy patterns used in row loops |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Not using raw strings — backslash confusion
pattern = "\d+"  # \d is not interpreted as regex digit in some contexts

# RIGHT: Always use raw strings
pattern = r"\d+"

# WRONG: Not compiling repeated patterns (slow in loops!)
for record in million_records:
    if re.match(r"^\d{4}-\d{2}-\d{2}$", record["date"]):  # Recompiles each time!
        pass

# RIGHT: Compile once
DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")
for record in million_records:
    if DATE_REGEX.match(record["date"]):  # Fast — uses pre-compiled object
        pass

# WRONG: Using .match() when you want to search the whole string
result = re.match(r"\d+", "abc 123")  # None — match only checks from START

# RIGHT: Use .search() to search anywhere
result = re.search(r"\d+", "abc 123")  # Match: '123'
```
