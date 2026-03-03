# 📅 Pandas Datetime Handling (ETL Context)

---

## 📖 Explanation

Datetime handling is critical in ETL — virtually every data pipeline deals with timestamps, date ranges, time zones, and temporal aggregations. Pandas provides powerful tools through `pd.Timestamp`, `pd.DatetimIndex`, and the `.dt` accessor.

---

## 🧠 Must-Remember Points

- `pd.to_datetime()` converts strings/columns to datetime — always specify `format=` for speed and reliability.
- `errors='coerce'` in `pd.to_datetime()` converts unparseable values to `NaT` (Not a Timestamp).
- `df["col"].dt` accessor provides datetime properties: `.year`, `.month`, `.day`, `.hour`, `.dayofweek`, `.quarter`.
- `pd.Timedelta` and `pd.DateOffset` are used for date arithmetic.
- Use `tz_localize()` to set a timezone; `tz_convert()` to convert between timezones.
- `pd.date_range()` generates a range of dates — great for creating date dimension tables.
- `pd.Grouper(freq="M")` enables group-by time period.
- `resample()` re-indexes and aggregates time-series data.
- `NaT` ("Not a Time") is the datetime equivalent of `NaN`.

### Key Date Frequency Aliases

| Alias  | Frequency     |
| ------ | ------------- |
| `"D"`  | Calendar day  |
| `"B"`  | Business day  |
| `"W"`  | Week (Sunday) |
| `"ME"` | Month end     |
| `"MS"` | Month start   |
| `"QE"` | Quarter end   |
| `"YE"` | Year end      |
| `"H"`  | Hour          |

---

## 💻 Code Examples

### 1️⃣ `pd.to_datetime()` — Parsing Dates

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({
    "order_date": ["2024-01-15", "15/02/2024", "2024-03-01", "INVALID", None],
    "ship_timestamp": ["2024-01-16 08:30:00", "2024-02-17 09:00:00",
                       "2024-03-02 14:00:00", "2024-03-05 00:00:00", "2024-01-01 12:00:00"],
})

# Parse with automatic detection (slower)
df["order_dt"] = pd.to_datetime(df["order_date"], errors="coerce", dayfirst=False)

# Always prefer specifying format (much faster on large data)
# df["order_dt"] = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")

# Parse timestamps
df["ship_dt"] = pd.to_datetime(df["ship_timestamp"], format="%Y-%m-%d %H:%M:%S")

print(df[["order_dt", "ship_dt"]].dtypes)
# order_dt    datetime64[ns]
# ship_dt     datetime64[ns]

# Check for parse failures
null_dates = df["order_dt"].isna()
print(f"Unparseable dates: {null_dates.sum()}")  # 2 (INVALID + None)
```

---

### 2️⃣ `.dt` Accessor — Extract Datetime Components

```python
df = pd.DataFrame({
    "order_date": pd.date_range("2024-01-01", periods=100, freq="D"),
    "amount": range(1000, 11000, 100),
})

# Extract components
df["year"] = df["order_date"].dt.year
df["month"] = df["order_date"].dt.month
df["month_name"] = df["order_date"].dt.strftime("%B")   # "January"
df["quarter"] = df["order_date"].dt.quarter
df["day_of_week"] = df["order_date"].dt.dayofweek       # 0=Monday, 6=Sunday
df["day_name"] = df["order_date"].dt.strftime("%A")     # "Monday"
df["week_of_year"] = df["order_date"].dt.isocalendar().week
df["is_weekend"] = df["order_date"].dt.dayofweek >= 5   # Boolean

# Format back to string
df["date_str"] = df["order_date"].dt.strftime("%Y-%m-%d")
df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)  # For date dim

print(df[["order_date", "quarter", "day_name", "is_weekend", "date_key"]].head())
```

---

### 3️⃣ Date Arithmetic with Timedelta & DateOffset

```python
df = pd.DataFrame({
    "order_date": pd.to_datetime(["2024-01-15", "2024-02-20", "2024-03-10"]),
    "ship_date": pd.to_datetime(["2024-01-17", "2024-02-25", "2024-03-15"]),
})

# Days between dates
df["days_to_ship"] = (df["ship_date"] - df["order_date"]).dt.days

# Add fixed duration
df["due_date"] = df["order_date"] + pd.Timedelta(days=30)
df["net_30_date"] = df["order_date"] + pd.DateOffset(months=1)

# Business days to ship
def business_days_between(start, end):
    return pd.bdate_range(start, end).size - 1

df["biz_days_to_ship"] = df.apply(
    lambda r: business_days_between(r["order_date"], r["ship_date"]),
    axis=1
)
print(df)
```

---

### 4️⃣ ETL: Timezone Handling

```python
import pytz

df = pd.DataFrame({
    "event_time_utc": pd.to_datetime([
        "2024-01-15 10:30:00",
        "2024-01-16 14:00:00",
    ]),
})

# Localize to UTC (add timezone info)
df["event_time_utc"] = df["event_time_utc"].dt.tz_localize("UTC")

# Convert to Eastern Time
df["event_time_est"] = df["event_time_utc"].dt.tz_convert("US/Eastern")

# Convert to IST
df["event_time_ist"] = df["event_time_utc"].dt.tz_convert("Asia/Kolkata")

print(df[["event_time_utc", "event_time_est", "event_time_ist"]])
```

---

### 5️⃣ ETL: Resample Time-Series Data

```python
daily_sales = pd.DataFrame({
    "date": pd.date_range("2024-01-01", periods=365, freq="D"),
    "revenue": [abs(i * 100 + 500) for i in range(365)],
    "orders": [max(1, abs(i // 10)) for i in range(365)],
})
daily_sales.set_index("date", inplace=True)

# Resample to monthly (sum by default)
monthly = daily_sales.resample("ME").sum()
monthly.index = monthly.index.strftime("%Y-%m")
print(monthly.head())

# Quarterly average
quarterly_avg = daily_sales.resample("QE").agg({
    "revenue": "sum",
    "orders": ["sum", "mean"],
})
print(quarterly_avg)

# Business day frequency fill (forward fill missing dates)
biz_day = daily_sales.resample("B").ffill()
```

---

### 6️⃣ ETL: Date Dimension Table Generation

```python
def create_date_dimension(start_date: str, end_date: str) -> pd.DataFrame:
    """Generate a complete date dimension (dim_date) table for a data warehouse."""
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    dim = pd.DataFrame({"full_date": dates})

    dim["date_key"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["full_date"].dt.year
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["quarter_name"] = "Q" + dim["quarter"].astype(str)
    dim["month_num"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.strftime("%B")
    dim["month_abbr"] = dim["full_date"].dt.strftime("%b")
    dim["week_of_year"] = dim["full_date"].dt.isocalendar().week.astype(int)
    dim["day_of_month"] = dim["full_date"].dt.day
    dim["day_of_week"] = dim["full_date"].dt.dayofweek + 1  # 1=Monday, 7=Sunday
    dim["day_name"] = dim["full_date"].dt.strftime("%A")
    dim["is_weekend"] = dim["full_date"].dt.dayofweek >= 5
    dim["is_weekday"] = ~dim["is_weekend"]
    dim["year_month"] = dim["full_date"].dt.strftime("%Y-%m")
    dim["year_quarter"] = dim["year"].astype(str) + "-" + dim["quarter_name"]

    return dim.set_index("date_key")

date_dim = create_date_dimension("2020-01-01", "2025-12-31")
print(f"Date dimension: {len(date_dim)} rows")
print(date_dim.head())
```

---

## 🏭 ETL Use Cases

| Feature                       | ETL Use Case                                          |
| ----------------------------- | ----------------------------------------------------- |
| `pd.to_datetime(format=...)`  | Parse dates from various source formats               |
| `.dt.year/.month/.quarter`    | Extract date parts for dimension tables               |
| `pd.Timedelta` / `DateOffset` | Calculate SLA breaches, due dates, aging              |
| `tz_localize / tz_convert`    | Normalize timestamps to UTC for data warehouse        |
| `resample()`                  | Convert daily data to weekly/monthly for reporting    |
| Date dimension generator      | Build `dim_date` table for data warehouse star schema |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Not specifying format — slow and may misparse (e.g., 01/02/2024)
df["date"] = pd.to_datetime(df["date_str"])  # Guesses format — slow + ambiguous!

# RIGHT: Always specify format
df["date"] = pd.to_datetime(df["date_str"], format="%Y-%m-%d")
# or dayfirst=True for DD/MM/YYYY European formats:
df["date"] = pd.to_datetime(df["date_str"], format="%d/%m/%Y")

# WRONG: Arithmetic on different timezone-aware and naive datetimes
utc_time = pd.Timestamp("2024-01-15 10:00:00", tz="UTC")
naive_time = pd.Timestamp("2024-01-15 05:00:00")  # No timezone
diff = utc_time - naive_time  # TypeError: cannot mix tz-aware and tz-naive!

# RIGHT: Both must have the same timezone
naive_localized = naive_time.tz_localize("UTC")
diff = utc_time - naive_localized  # Now works

# WRONG: Ignoring NaT after date parsing
df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
mean_date = df["date"].mean()  # May silently ignore NaT — check first!
print(f"Null dates: {df['date'].isna().sum()}")  # Always check this!
```
