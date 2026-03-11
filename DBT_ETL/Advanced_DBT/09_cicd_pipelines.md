# 🔄 dbt in CI/CD Pipelines

---

## 🤔 Why CI/CD for dbt?

Without CI/CD, a developer can push broken SQL directly to production. With CI/CD:

- Every PR runs `dbt build` on a dev schema — catches errors before merge
- Only tested, approved code reaches production
- Production runs are automated and consistent

---

## 💻 Example 1: GitHub Actions CI Pipeline for dbt

```python
# File: .github/workflows/dbt_ci.yml
github_actions_ci = """
name: dbt CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt_ci:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-snowflake==1.6.0

      - name: Configure profiles.yml
        run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml << EOF
          my_etl_project:
            target: ci
            outputs:
              ci:
                type: snowflake
                account: ${{ secrets.SNOWFLAKE_ACCOUNT }}
                user: ${{ secrets.SNOWFLAKE_USER }}
                password: ${{ secrets.SNOWFLAKE_PASSWORD }}
                database: DEV_DB
                schema: PR_${{ github.event.pull_request.number }}   # unique schema per PR!
                warehouse: CI_WH
          EOF

      - name: Install dbt packages
        run: dbt deps

      - name: Check source freshness
        run: dbt source freshness

      - name: Run only changed models + downstream
        run: dbt run --select state:modified+   # Only changed models!
        env:
          DBT_ARTIFACT_STATE_PATH: ./prod_artifacts/   # Compare to prod state

      - name: Test all changed models
        run: dbt test --select state:modified+

      - name: Generate docs
        run: dbt docs generate
"""
print(github_actions_ci)
```

---

## 💻 Example 2: dbt Production Run (Scheduled)

```python
# File: .github/workflows/dbt_prod.yml
prod_workflow = """
name: dbt Production Run

on:
  schedule:
    - cron: '0 5 * * *'   # Run at 5am UTC daily
  workflow_dispatch:       # Also allow manual trigger

jobs:
  dbt_prod:
    runs-on: ubuntu-latest
    environment: production   # Uses production secrets/environment

    steps:
      - uses: actions/checkout@v3

      - run: pip install dbt-snowflake==1.6.0

      - name: Run full dbt pipeline
        run: |
          dbt source freshness       # Check sources aren't stale
          dbt deps                   # Install packages
          dbt seed                   # Reload any changed seeds
          dbt snapshot               # Capture dimension changes (SCD)
          dbt run                    # Build all models
          dbt test                   # Run all quality tests
          dbt docs generate          # Refresh documentation
        env:
          SNOWFLAKE_ACCOUNT:   ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER:      ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD:  ${{ secrets.SNOWFLAKE_PASSWORD }}
"""
print(prod_workflow)
```

---

## 💻 Example 3: `dbt build` — The One Command to Rule Them All

```python
dbt_build = """
# dbt build runs seeds + snapshots + run + test in dependency order
# It's the recommended single command for production pipelines

dbt build
# Equivalent to:
# dbt seed && dbt snapshot && dbt run && dbt test (in the right order)

# Build only specific models and their tests:
dbt build --select +fct_sales

# Build with full refresh:
dbt build --full-refresh --select staging.*
"""
print(dbt_build)
```

---

## 💻 Example 4: Slim CI — Only Test Changed Models

```python
slim_ci = """
# Slim CI (dbt's recommended CI pattern):
# 1. Download production manifest.json (artifacts from last prod run)
# 2. Compare PR changes to prod state
# 3. Only run/test models that CHANGED or are downstream of changes
# This makes CI 10x faster — only tests what changed!

# Download prod manifest to compare against:
# aws s3 cp s3://my-bucket/dbt/manifest.json ./prod_manifest/manifest.json

# Run only changed models + their downstream dependencies:
# dbt run  --select state:modified+ --state ./prod_manifest/
# dbt test --select state:modified+ --state ./prod_manifest/

example_output = [
    "Found 47 models, 12 modified",
    "Running 18 models (12 modified + 6 downstream)",
    "Running 36 tests on 18 models",
    "All tests passed!",
    "Total runtime: 3m 22s (vs 28m for full run)",
]
for line in example_output:
    print(f"  {line}")
"""
print(slim_ci)
```

---

## 🏭 CI/CD Best Practices Summary

| Practice                          | Why                                         |
| --------------------------------- | ------------------------------------------- |
| Unique schema per PR (`PR_123`)   | Isolated dev env — no conflicts between PRs |
| `state:modified+` in CI           | Only test changed models — 10x faster CI    |
| `dbt build` in production         | One command: seeds + snapshots + run + test |
| Store prod `manifest.json`        | Enables slim CI comparison                  |
| Secrets in GitHub/vault           | Never hardcode credentials in YAML          |
| `dbt source freshness` before run | Fail fast if sources are stale              |
