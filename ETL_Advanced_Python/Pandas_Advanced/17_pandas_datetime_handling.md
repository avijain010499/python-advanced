# 📅 Pandas Datetime Handling (ETL Context)

---

## 🤔 Why Is Datetime Handling Important?

In ETL, temporal data is everywhere: order dates, event timestamps, fiscal periods, SLA deadlines. Python's `datetime` + Pandas `.dt` accessor give you powerful tools for parsing, calculating, and grouping by time.

> 💡 **Key mindset**: Dates stored as strings are useless for time arithmetic. Always convert to `datetime64` early in your pipeline.

---

## 🧱 The Three Levels of Datetime in Pandas

| Type                         | When to Use                                    |
| ---------------------------- | ---------------------------------------------- |
| `datetime64[ns]` (timestamp) | Records with exact time: `2024-01-15 10:30:00` |
| `Period`                     | Time spans: month 2024-01, quarter Q1-2024     |
| `Timedelta`                  | Durations: 3 days, 2 hours                     |

---

## 💻 Example 1: Parsing Dates

```python
import pandas as pd

df = pd.DataFrame({
    "date_iso":  ["2024-01-15", "2024-02-20", "2024-03-05"],
    "date_eu":   ["15/01/2024", "20/02/2024", "05/03/2024"],
    "date_us":   ["01-15-2024", "02-20-2024", "03-05-2024"],
    "timestamp": ["2024-01-15 10:30:00", "2024-02-20 14:45:00", "2024-03-05 09:00:00"],
    "unix_ts":   [1705312200, 1708429500, 1709628000],   # Unix epoch (seconds)
})

# Convert each format — always specify format for speed + accuracy
df["date"] = pd.to_datetime(df["date_iso"], format="%Y-%m-%d")
df["date_eu_parsed"] = pd.to_datetime(df["date_eu"], format="%d/%m/%Y")
df["date_us_parsed"] = pd.to_datetime(df["date_us"], format="%m-%d-%Y")
df["ts"]   = pd.to_datetime(df["timestamp"])
df["from_unix"] = pd.to_datetime(df["unix_ts"], unit="s")

# errors="coerce": bad dates → NaT (not a crash!)
df["safe_parse"] = pd.to_datetime(
    ["2024-01-15", "not-a-date", "2024-03-05"],
    errors="coerce"
)
print(df["safe_parse"])   # ..., NaT, ...
```

---

## 💻 Example 2: The `.dt` Accessor — Extract Date Parts

```python
df = pd.DataFrame({"ts": pd.to_datetime([
    "2024-01-15 10:30:45",
    "2024-07-22 14:05:00",
    "2024-12-31 23:59:59",
])})

# Extract individual components
df["year"]        = df["ts"].dt.year
df["month"]       = df["ts"].dt.month
df["day"]         = df["ts"].dt.day
df["hour"]        = df["ts"].dt.hour
df["weekday"]     = df["ts"].dt.day_name()       # 'Monday', 'Tuesday', ...
df["quarter"]     = df["ts"].dt.quarter           # 1, 2, 3, 4
df["week_number"] = df["ts"].dt.isocalendar().week.astype(int)
df["is_month_end"]= df["ts"].dt.is_month_end

# Date arithmetic
df["days_since"] = (pd.Timestamp.now() - df["ts"]).dt.days
df["next_month"] = df["ts"] + pd.DateOffset(months=1)

print(df[["ts","year","month","weekday","quarter","days_since"]])
```

---

## 💻 Example 3: Timezone Handling

```python
import pandas as pd

df = pd.DataFrame({
    "event_time": pd.to_datetime(["2024-01-15 05:30:00", "2024-06-15 18:00:00"])
})

# Localize (assign timezone to naive timestamps)
df["utc_time"] = df["event_time"].dt.tz_localize("UTC")

# Convert to another timezone
df["ist_time"]  = df["utc_time"].dt.tz_convert("Asia/Kolkata")
df["pst_time"]  = df["utc_time"].dt.tz_convert("US/Pacific")
df["est_time"]  = df["utc_time"].dt.tz_convert("US/Eastern")

# Strip timezone for DB storage (many DBs don't support tz-aware)
df["naive_ist"] = df["ist_time"].dt.tz_localize(None)

print(df[["event_time","utc_time","ist_time"]])
```

---

## 💻 Example 4: Resample — Time-Based Aggregation

```python
import pandas as pd, numpy as np

df = pd.DataFrame({
    "ts":    pd.date_range("2024-01-01", periods=365, freq="D"),
    "sales": np.random.randint(1000, 5000, 365),
}).set_index("ts")

# Weekly totals (frequency: "W" = week ending Sunday)
weekly  = df.resample("W").sum()

# Monthly totals
monthly = df.resample("ME").agg(
    total_sales = ("sales", "sum"),
    avg_sales   = ("sales", "mean"),
    peak_day    = ("sales", "max"),
)

# Quarterly
quarterly = df.resample("QE").sum()
print(monthly.head())
```

---

## 💻 Example 5: Build a Date Dimension Table (ETL Classic)

```python
import pandas as pd

def build_date_dimension(start: str, end: str) -> pd.DataFrame:
    """
    Generate a complete date dimension table — standard in data warehouses.
    Contains one row per day with pre-calculated fiscal/calendar attributes.
    """
    dates = pd.date_range(start=start, end=end, freq="D")
    dim   = pd.DataFrame({"full_date": dates})

    dim["date_key"]       = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"]           = dim["full_date"].dt.year
    dim["month"]          = dim["full_date"].dt.month
    dim["month_name"]     = dim["full_date"].dt.strftime("%B")
    dim["day"]            = dim["full_date"].dt.day
    dim["weekday"]        = dim["full_date"].dt.day_name()
    dim["is_weekend"]     = dim["full_date"].dt.dayofweek >= 5  # 5=Sat, 6=Sun
    dim["quarter"]        = dim["full_date"].dt.quarter
    dim["week_of_year"]   = dim["full_date"].dt.isocalendar().week.astype(int)
    dim["is_month_end"]   = dim["full_date"].dt.is_month_end
    dim["fiscal_year"]    = dim["year"].where(dim["month"] < 4, dim["year"] + 1)
    dim["fiscal_quarter"] = ((dim["month"] - 4) % 12 // 3) + 1

    return dim

dim_date = build_date_dimension("2020-01-01", "2029-12-31")
print(f"Date dimension: {len(dim_date):,} rows")
print(dim_date.head())
```

---

## 🏭 ETL Use Cases Summary

| Operation                    | Use Case                                        |
| ---------------------------- | ----------------------------------------------- |
| `pd.to_datetime(format=...)` | Parse source dates (always specify format)      |
| `errors="coerce"`            | Silently null invalid dates instead of crashing |
| `.dt.year/.month/.day`       | Extract components for date dimension           |
| `tz_localize / tz_convert`   | Handle multi-timezone source data               |
| `resample("ME")`             | Monthly/quarterly aggregation                   |
| Date dimension table         | Standard DWH pattern for time-based analysis    |

---

## ⚠️ Common Mistakes

```python
# ❌ No format string — pandas guesses (slow + may be wrong!)
pd.to_datetime(df["date"])   # Guesses format per row

# ✅ Always specify
pd.to_datetime(df["date"], format="%Y-%m-%d")

# ❌ Not handling NaT after coerce
df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
df["year"] = df["date"].dt.year   # NaT becomes NaN — OK, but might surprise you

# ✅ Check and handle NaT before downstream steps
missing_dates = df["date"].isna().sum()
if missing_dates:
    print(f"⚠️  {missing_dates} rows have unparseable dates — check source!")
```
