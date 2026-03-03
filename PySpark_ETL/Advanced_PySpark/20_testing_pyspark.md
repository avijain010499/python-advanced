# 🧪 Testing PySpark Code (ETL Context)

---

## 🤔 Why Test PySpark ETL Code?

ETL bugs are expensive — bad data silently corrupts dashboards and reports for days before anyone notices. Unit and integration tests catch these before they reach production.

Common things to test:

- Transformation logic (business rules, type conversions)
- Edge cases: nulls, empty DataFrames, bad input
- Row counts after each step
- Schema contracts

---

## 🧱 Setup: `pytest` + `pyspark`

```bash
pip install pytest pyspark chispa
# chispa: a library for asserting PySpark DataFrame equality
```

---

## 💻 Example 1: SparkSession Fixture for Tests

```python
# conftest.py — shared fixtures for all test files

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Create ONE SparkSession shared across all tests.
    scope="session" means it's created once and reused — fast!
    """
    spark = (
        SparkSession.builder
        .appName("ETL_Tests")
        .master("local[2]")   # 2 cores — enough for testing
        .config("spark.sql.shuffle.partitions", "2")  # Faster for small test data
        .config("spark.ui.enabled", "false")           # Disable UI for cleaner test output
        .getOrCreate()
    )
    yield spark   # Provide the session to tests
    spark.stop()  # Teardown after all tests finish
```

---

## 💻 Example 2: Testing Transformations

```python
# tests/test_transforms.py

import pytest
from pyspark.sql.functions import col
from chispa.dataframe_comparer import assert_df_equality

# Import your actual ETL transform function
from etl.transforms import clean_sales_record, calculate_revenue

class TestCleanSalesRecord:

    def test_basic_cleaning(self, spark):
        """Names and regions are trimmed and title-cased."""
        input_df = spark.createDataFrame([
            ("ORD-001", "  alice smith  ", " NORTH ", 100.0, "COMPLETE"),
        ], ["order_id","name","region","amount","status"])

        result = clean_sales_record(input_df)

        expected = spark.createDataFrame([
            ("ORD-001", "Alice Smith", "North", 100.0, "COMPLETE"),
        ], ["order_id","name","region","amount","status"])

        assert_df_equality(result, expected, ignore_nullable=True)

    def test_null_name_becomes_unknown(self, spark):
        """Null names are filled with 'Unknown'."""
        input_df = spark.createDataFrame([
            ("ORD-002", None, "SOUTH", 200.0, "COMPLETE"),
        ], ["order_id","name","region","amount","status"])

        result = clean_sales_record(input_df)

        assert result.filter(col("name").isNull()).count() == 0
        assert result.first()["name"] == "Unknown"

    def test_empty_dataframe(self, spark):
        """Empty input returns empty output (no crash)."""
        schema = "order_id STRING, name STRING, region STRING, amount DOUBLE, status STRING"
        empty  = spark.createDataFrame([], schema)
        result = clean_sales_record(empty)
        assert result.count() == 0

class TestCalculateRevenue:

    def test_revenue_calculation(self, spark):
        input_df = spark.createDataFrame([
            ("ORD-001", 5, 199.99),
            ("ORD-002", 2, 499.99),
        ], ["order_id","qty","unit_price"])

        result = calculate_revenue(input_df)

        assert result.filter(col("order_id") == "ORD-001").first()["revenue"] == pytest.approx(999.95)
        assert result.filter(col("order_id") == "ORD-002").first()["revenue"] == pytest.approx(999.98)

    def test_zero_qty_produces_zero_revenue(self, spark):
        input_df = spark.createDataFrame([("ORD-003", 0, 199.99)], ["order_id","qty","unit_price"])
        result   = calculate_revenue(input_df)
        assert result.first()["revenue"] == 0.0
```

---

## 💻 Example 3: Testing Schema Contracts

```python
# tests/test_schema.py

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

EXPECTED_OUTPUT_SCHEMA = StructType([
    StructField("order_id",    StringType(),  False),
    StructField("customer_id", IntegerType(), True),
    StructField("revenue",     DoubleType(),  True),
    StructField("region",      StringType(),  True),
])

def test_output_schema_matches_contract(spark, transformed_df):
    """Output schema must match the downstream table contract."""
    actual_fields = {f.name: f.dataType for f in transformed_df.schema.fields}
    for field in EXPECTED_OUTPUT_SCHEMA.fields:
        assert field.name in actual_fields, f"Missing column: {field.name}"
        assert type(actual_fields[field.name]) == type(field.dataType), \
            f"Type mismatch on {field.name}: {actual_fields[field.name]} != {field.dataType}"
```

---

## 💻 Example 4: Testing Data Quality Checks

```python
# tests/test_dq.py

def test_no_duplicate_order_ids(spark, clean_df):
    """Each order_id must be unique in the output."""
    total = clean_df.count()
    unique = clean_df.dropDuplicates(["order_id"]).count()
    assert total == unique, f"Duplicate order_ids found: {total - unique} duplicates"

def test_no_negative_amounts(spark, clean_df):
    """All amounts must be non-negative after transformation."""
    from pyspark.sql.functions import col
    negative_count = clean_df.filter(col("amount") < 0).count()
    assert negative_count == 0, f"Found {negative_count} negative amounts!"

def test_required_columns_not_null(spark, clean_df):
    """order_id and customer_id must not be null."""
    from pyspark.sql.functions import col
    for required_col in ["order_id", "customer_id"]:
        nulls = clean_df.filter(col(required_col).isNull()).count()
        assert nulls == 0, f"Found {nulls} nulls in required column '{required_col}'"
```

---

## 💻 Example 5: Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=etl --cov-report=term-missing

# Run only fast unit tests (skip slow integration tests)
pytest tests/ -m "not integration" -v

# Sample output:
# tests/test_transforms.py::TestCleanSalesRecord::test_basic_cleaning PASSED
# tests/test_transforms.py::TestCleanSalesRecord::test_null_name_becomes_unknown PASSED
# tests/test_transforms.py::TestCleanSalesRecord::test_empty_dataframe PASSED
# tests/test_transforms.py::TestCalculateRevenue::test_revenue_calculation PASSED
# =================== 4 passed in 8.3s ===================
```

---

## 🏭 ETL Testing Summary

| Test Type             | What to Test                               |
| --------------------- | ------------------------------------------ |
| Unit tests            | Individual transform functions             |
| Schema contract tests | Output matches downstream table definition |
| DQ tests              | Nulls, duplicates, value ranges            |
| Edge case tests       | Empty DF, single row, all-null column      |
| `assert_df_equality`  | Exact row-by-row DataFrame equality        |

---

## ⚠️ Testing Best Practices

```python
# ❌ Creating a new SparkSession per test — very slow!
def test_something():
    spark = SparkSession.builder.getOrCreate()
    ...

# ✅ Use a shared session fixture (scope="session" in conftest.py)
def test_something(spark):   # spark injected by pytest fixture
    ...

# ❌ Testing on production-sized data — tests take hours
def test_revenue(spark):
    df = spark.read.parquet("s3://prod-bucket/all_sales/")  # 10TB!
    ...

# ✅ Use minimal representative test data
def test_revenue(spark):
    df = spark.createDataFrame([
        ("ORD-1", 5, 100.0),   # Normal case
        ("ORD-2", 0, 100.0),   # Zero qty edge case
        ("ORD-3", -1, 100.0),  # Negative qty edge case
    ],["order_id","qty","unit_price"])
    ...
```
