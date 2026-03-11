# 🔐 Secrets, Blocks & Connections in Prefect

---

## 🤔 What Are Blocks?

**Blocks** are Prefect's way to store and reuse infrastructure config and credentials:

- Database connections
- Cloud storage credentials
- API keys and secrets
- Docker registries, GCS buckets, S3 paths

> 💡 **Why blocks?** Store credentials **once** in Prefect's encrypted storage, reference them by name. No hardcoded passwords in code!

---

## 💻 Example 1: Secret Block — Secure Credential Storage

```python
from prefect.blocks.system import Secret

# Create a secret (run once to register in Prefect):
# Secret(value="my_super_secret_api_key").save(name="openai-api-key")

# Retrieve in any flow:
# secret_block = Secret.load("openai-api-key")
# api_key = secret_block.get()

# Example: use in a task
from prefect import flow, task

@task(name="Call External API")
def call_api() -> dict:
    secret_block = Secret.load("my-api-key")
    api_key      = secret_block.get()
    # Use api_key to call your API
    return {"status": "success"}

@flow
def etl_with_secret():
    result = call_api()
    print(result)

print("Secrets are encrypted at rest in Prefect Cloud/Server")
print("CLI: prefect block create secret  → interactive setup")
```

---

## 💻 Example 2: Database Connection Block (SQLAlchemy)

```python
# pip install prefect-sqlalchemy
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect import flow, task

# Register block once:
# SqlAlchemyConnector(
#     connection_info=ConnectionComponents(
#         driver="postgresql+psycopg2",
#         host="db.company.com",
#         port=5432,
#         database="etl_db",
#         username="etl_user",
#         password=SecretStr("password"),
#     )
# ).save("prod-postgres")

@task(name="Extract from Postgres")
def extract_from_db(query: str) -> list:
    with SqlAlchemyConnector.load("prod-postgres") as conn:
        result = conn.fetch_many(query, size=10000)
    print(f"Extracted {len(result)} rows")
    return result

@flow(name="DB ETL")
def db_etl():
    records = extract_from_db("SELECT * FROM sales WHERE date = CURRENT_DATE")
    print(f"Processing {len(records)} records")

print("The connection URL and password stay in Prefect — never in your code!")
```

---

## 💻 Example 3: S3 / GCS / Azure Storage Blocks

```python
# AWS S3:
# pip install prefect-aws
from prefect_aws.s3 import S3Bucket

# Register the block (run once):
# S3Bucket(
#     bucket_name="my-etl-bucket",
#     credentials=AwsCredentials(
#         aws_access_key_id=SecretStr("..."),
#         aws_secret_access_key=SecretStr("...")
#     )
# ).save("prod-s3-bucket")

@task(name="Write to S3")
def write_to_s3(df, filename: str) -> str:
    import io
    s3 = S3Bucket.load("prod-s3-bucket")
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_from_file_object(buffer, f"output/{filename}")
    return f"s3://my-etl-bucket/output/{filename}"

print("Blocks for S3, GCS, Azure Blob, Snowflake, BigQuery all available")
print("Find them: prefect block ls")
```

---

## 💻 Example 4: Environment Variables vs Blocks

```python
import os
from prefect import flow, task

# Option A: Environment Variables (simple, works everywhere)
@task
def connect_via_env():
    db_url = os.environ["DATABASE_URL"]   # Set in shell or .env file
    print(f"Connecting to: {db_url[:20]}...")

# Option B: Prefect Blocks (preferred for production)
@task
def connect_via_block():
    from prefect_sqlalchemy import SqlAlchemyConnector
    with SqlAlchemyConnector.load("prod-postgres") as conn:
        conn.execute("SELECT 1")

# When to use which:
guidance = {
    "Env variables": "Quick dev setup, simple scripts, works without Prefect server",
    "Prefect Blocks": "Production — encrypted, auditable, shareable across flows",
    "Never":          "Hardcode credentials directly in Python files or YAML",
}
for where, when in guidance.items():
    print(f"  {where}: {when}")

@flow(name="Config Example")
def config_flow():
    connect_via_env()   # Or connect_via_block() in production
```

---

## 🏭 Summary

| Block Type                  | Package              | Use For                             |
| --------------------------- | -------------------- | ----------------------------------- |
| `Secret`                    | `prefect` builtin    | API keys, passwords, tokens         |
| `SqlAlchemyConnector`       | `prefect-sqlalchemy` | Postgres, MySQL, SQLite connections |
| `S3Bucket`                  | `prefect-aws`        | AWS S3 read/write                   |
| `GcsBucket`                 | `prefect-gcp`        | Google Cloud Storage                |
| `AzureBlobStorageContainer` | `prefect-azure`      | Azure Blob Storage                  |
| `SnowflakeConnector`        | `prefect-snowflake`  | Snowflake queries                   |

---

## ⚠️ Common Mistakes

```python
mistakes = [
    ("Hardcoding passwords in flow code", "Use Secret blocks — never put credentials in Python files"),
    ("Re-creating blocks in every flow run", "Register blocks once, load by name in flows"),
    ("Using prod credentials in dev", "Create separate dev/prod blocks with different names"),
]
for m, fix in mistakes:
    print(f"❌ {m}")
    print(f"✅ {fix}\n")
```
