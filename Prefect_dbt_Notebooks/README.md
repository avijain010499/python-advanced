# ❄️🌊 Prefect + dbt + Snowflake Notebooks

Comprehensive Jupyter notebooks for orchestrating dbt Snowflake pipelines using Prefect.

## 📚 Notebooks

| # | Notebook | What You'll Learn |
|---|---|---|
| 01 | `01_intro_setup.ipynb` | Setup, `dbt-snowflake` install, `profiles.yml`, Prefect Secrets |
| 02 | `02_running_dbt_commands.ipynb` | Prefect tasks wrapping dbt CLI, `--target`, `--select`, `--full-refresh` |
| 03 | `03_retries_error_handling.ipynb` | Snowflake-aware retries, graceful test failure, `on_failure` hooks |
| 04 | `04_scheduling_deployments.ipynb` | `.serve()`, `.deploy()`, cron schedules, credit-aware scheduling |
| 05 | `05_complete_production_pipeline.ipynb` | Full production-grade flow: seed → run → test → report + alerts |

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install prefect dbt-snowflake prefect-dbt

# 2. Set Snowflake credentials (run once)
python -c "
from prefect.blocks.system import Secret
Secret(value='your_account').save('snowflake-account')
Secret(value='your_user').save('snowflake-user')
Secret(value='your_password').save('snowflake-password')
Secret(value='COMPUTE_WH').save('snowflake-warehouse')
Secret(value='ANALYTICS').save('snowflake-database')
"

# 3. Update DBT_PROJECT_DIR in notebooks to your dbt project path

# 4. Run notebooks in order: 01 → 05
```

## ❄️ Snowflake profiles.yml Template

```yaml
my_etl_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account:   "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:      "{{ env_var('SNOWFLAKE_USER') }}"
      password:  "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role:      TRANSFORMER
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH') }}"
      database:  "{{ env_var('SNOWFLAKE_DATABASE', 'ANALYTICS') }}"
      schema: dbt_dev
      threads: 4
```
