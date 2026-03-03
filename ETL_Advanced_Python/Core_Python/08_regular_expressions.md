# 🔎 Regular Expressions (Regex) in Python (ETL Context)

---

## 🤔 What Are Regular Expressions?

A **regular expression** (regex) is a pattern that describes a set of strings. You use it to search, validate, extract, or transform text.

In ETL, raw data is messy — phone numbers in 10 different formats, emails that aren't emails, dates written as strings. Regex is your tool to tame all of it.

> 💡 **Analogy**: A regex is like a **search query on steroids**. Instead of searching for an exact word like "sale", you search for a _pattern_ like "any 4-digit year followed by a dash followed by 2-digit month".

Python's `re` module provides all regex functionality.

---

## 🧱 Quick Reference: Common Patterns

| Pattern         | Meaning                               | Example Match                  |
| --------------- | ------------------------------------- | ------------------------------ |
| `.`             | Any single character (except newline) | `a`, `5`, `!`                  |
| `\d`            | A digit (0–9)                         | `5`                            |
| `\D`            | Not a digit                           | `a`, `!`                       |
| `\w`            | Word character (a-z, A-Z, 0-9, \_)    | `hello`, `var_1`               |
| `\s`            | Whitespace (space, tab, newline)      | ` `, `\t`                      |
| `^`             | Start of string                       | —                              |
| `$`             | End of string                         | —                              |
| `*`             | 0 or more of previous                 | `ab*` matches `a`, `ab`, `abb` |
| `+`             | 1 or more of previous                 | `ab+` matches `ab`, `abb`      |
| `?`             | 0 or 1 of previous (optional)         | `ab?` matches `a`, `ab`        |
| `{3}`           | Exactly 3 of previous                 | `\d{4}` — exactly 4 digits     |
| `{2,5}`         | Between 2 and 5 of previous           | `\d{2,5}`                      |
| `[abc]`         | Any one of a, b, c                    | `[aeiou]` — a vowel            |
| `(abc)`         | Capture group                         | `(\d{4})` — capture 4 digits   |
| `(?P<name>...)` | Named capture group                   | `(?P<year>\d{4})`              |
| `a\|b`          | a or b                                | `cat\|dog`                     |

> 🔑 **Always use raw strings**: `r"\d+"` not `"\d+"`. Raw strings prevent Python from interpreting backslashes.

---

## 🧱 Four Key Functions

```python
import re

text = "Order ORD-12345 placed on 2024-01-15, total: $1,500.00"

# 1. re.search() — find FIRST match ANYWHERE in the string
match = re.search(r"\d{4}-\d{2}-\d{2}", text)  # Date pattern
if match:
    print(match.group())   # '2024-01-15'  (the matched text)

# 2. re.match() — match only at the START of the string
m = re.match(r"Order", text)
print(m.group() if m else "No match")   # 'Order' — matched at start

m2 = re.match(r"\d+", text)
print(m2)   # None — text doesn't START with a digit

# 3. re.findall() — return ALL matches as a list of strings
all_numbers = re.findall(r"\d[\d,]*\.?\d*", text)
print(all_numbers)   # ['12345', '2024', '01', '15', '1,500.00']

# 4. re.sub() — substitute matches with a replacement
cleaned = re.sub(r"[$,]", "", "$1,500.00")  # Remove $ and commas
print(cleaned)   # '1500.00'
```

---

## 💻 Example 1: Pre-compile for Performance

When using the same pattern many times (e.g., inside a loop over 1M rows), **compile it once**:

```python
import re

# ❌ SLOW: recompiles the pattern for EVERY record
for record in million_records:
    if re.match(r"^\d{4}-\d{2}-\d{2}$", record["date"]):
        pass

# ✅ FAST: compile once, use millions of times
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
for record in million_records:
    if DATE_PATTERN.match(record["date"]):
        pass
```

---

## 💻 Example 2: ETL Validation

```python
import re

# Pre-compile all patterns at module level (fast, reusable)
EMAIL_RE   = re.compile(r"^[\w.+\-]+@[\w\-]+\.[a-zA-Z]{2,}$")
PHONE_RE   = re.compile(r"^\+?1?\s?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}$")
DATE_RE    = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
AMOUNT_RE  = re.compile(r"^\$?[\d,]+(\.\d{1,2})?$")

def validate_record(record: dict) -> list:
    """
    Returns a list of validation errors.
    Empty list = record is valid.
    """
    errors = []

    if not EMAIL_RE.match(str(record.get("email", ""))):
        errors.append(f"Invalid email: '{record.get('email')}'")

    if not PHONE_RE.match(str(record.get("phone", ""))):
        errors.append(f"Invalid phone: '{record.get('phone')}'")

    if not DATE_RE.match(str(record.get("date", ""))):
        errors.append(f"Invalid date format: '{record.get('date')}' (expected YYYY-MM-DD)")

    return errors


records = [
    {"email": "alice@example.com", "phone": "555-123-4567", "date": "2024-01-15"},
    {"email": "bad-email",         "phone": "1234",          "date": "15-01-2024"},
]
for r in records:
    errs = validate_record(r)
    print("✅ VALID" if not errs else f"❌ INVALID: {errs}")
```

---

## 💻 Example 3: Data Extraction with Named Groups

Named groups `(?P<name>...)` let you extract multiple pieces of data from one pattern:

```python
import re

# Log line from an ETL run:
log_line = "[2024-01-15 10:30:45] ERROR - Job: sales_transform | Records: 10500 | Duration: 45.2s"

# Named groups make the extracted data readable
LOG_PATTERN = re.compile(
    r"\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] "
    r"(?P<level>\w+) - Job: (?P<job>\w+) \| "
    r"Records: (?P<records>\d+) \| Duration: (?P<duration>[\d.]+)s"
)

match = LOG_PATTERN.search(log_line)
if match:
    data = match.groupdict()   # Returns a dict with group names as keys
    print(data)
    # {
    #   'timestamp': '2024-01-15 10:30:45',
    #   'level': 'ERROR',
    #   'job': 'sales_transform',
    #   'records': '10500',
    #   'duration': '45.2'
    # }
    print(f"Job '{data['job']}' processed {int(data['records']):,} records in {data['duration']}s")
```

---

## 💻 Example 4: Data Cleaning with `re.sub()`

```python
import re

def clean_amount(raw: str) -> float:
    """Convert '$1,500.75' or '€ 2,000' or '1500' to a float."""
    # Remove currency symbols, commas, spaces
    cleaned = re.sub(r"[$€£,\s]", "", raw)
    return float(cleaned) if cleaned else 0.0

def clean_name(raw: str) -> str:
    """Allow only letters, spaces, hyphens, and apostrophes."""
    letters_only = re.sub(r"[^a-zA-Z\s'\-]", "", raw)
    # Normalize multiple spaces to one
    return re.sub(r"\s+", " ", letters_only).strip()

def normalize_whitespace(text: str) -> str:
    """Replace any run of whitespace with a single space."""
    return re.sub(r"\s+", " ", text).strip()

print(clean_amount("$1,500.75"))           # 1500.75
print(clean_amount("€ 2,000.00"))          # 2000.0
print(clean_name("Alice123 O'Brien!!"))    # Alice O'Brien
print(normalize_whitespace("  Sales   Q1  2024  "))  # Sales Q1 2024
```

---

## 💻 Example 5: Pandas Vectorized Regex

Pandas `.str` accessor works with regex on entire columns at once:

```python
import pandas as pd

df = pd.DataFrame({
    "email":  ["alice@example.com", "bad-email", "bob@org.io", None],
    "phone":  ["555-123-4567", "800 555 9999", "1234", "+1-800-555-0001"],
    "amount": ["$1,500.00", "$200.50", "N/A", "$3,000.99"],
})

# Validate emails — returns boolean column
df["email_valid"] = df["email"].str.match(
    r"^[\w.+\-]+@[\w\-]+\.[a-zA-Z]{2,}$",
    na=False    # Treat NaN as False (not an error)
)

# Extract area code from phone — returns new column
df["area_code"] = df["phone"].str.extract(r"(\d{3})")  # First 3-digit group

# Clean amounts — remove $ and commas, then convert to numeric
df["amount_clean"] = (
    df["amount"]
    .str.replace(r"[$,]", "", regex=True)   # Remove $ and comma
    .pipe(pd.to_numeric, errors="coerce")    # Convert; N/A → NaN
)

print(df[["email", "email_valid", "area_code", "amount_clean"]])
```

---

## 🏭 ETL Use Cases Summary

| Regex Use Case                        | ETL Application                                            |
| ------------------------------------- | ---------------------------------------------------------- |
| Validation                            | Email, phone, date, postal code format checks              |
| Extraction with named groups          | Parse structured data from log lines                       |
| `re.sub()`                            | Clean currency, remove special chars, normalize whitespace |
| `re.compile()` at module level        | Precompile for high-volume row-by-row processing           |
| Pandas `.str.match/.extract/.replace` | Column-level regex at scale                                |

---

## 🧠 Must-Remember Points

| Rule                      | Remember                                               |
| ------------------------- | ------------------------------------------------------ |
| Raw strings               | Always `r"\d+"` not `"\d+"`                            |
| Compile for loops         | `re.compile()` once at module level                    |
| `re.match` vs `re.search` | `match` = start only; `search` = anywhere              |
| `re.findall`              | Returns list of all matches                            |
| Named groups              | `(?P<name>...)` → readable extraction                  |
| Pandas `na=False`         | Always pass this to `.str.contains()` / `.str.match()` |

---

## ⚠️ Common Mistakes Beginners Make

```python
import re

# ❌ MISTAKE 1: Not using raw strings
pattern = "\d+"   # In Python, \d is not a recognized escape sequence here
# ✅ FIX:
pattern = r"\d+"  # Raw string — backslash is literal


# ❌ MISTAKE 2: Using match() when you meant search()
text = "Error on line 42: invalid value"
m = re.match(r"\d+", text)   # None! Text doesn't START with a digit
# ✅ FIX:
m = re.search(r"\d+", text)  # Finds '42' anywhere in the string
print(m.group())   # '42'


# ❌ MISTAKE 3: Pandas .str.contains() crashing on NaN
mask = df["email"].str.contains("@")   # TypeError/ValueError if email has NaN!
# ✅ FIX: Always pass na=False
mask = df["email"].str.contains("@", na=False)
```
