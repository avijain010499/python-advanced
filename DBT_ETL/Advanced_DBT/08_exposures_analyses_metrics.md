# 🌐 Exposures, Analyses & dbt Metrics

---

## 🤔 What Are Exposures?

**Exposures** declare what consumes your dbt models — dashboards, ML models, reports. They document the downstream impact: "If `fct_sales` breaks, _Sales Dashboard_ breaks."

**Analyses** are SQL files in `analyses/` that are compiled by dbt but NOT materialized. Great for exploratory or ad-hoc queries that benefit from `ref()` and Jinja.

---

## 💻 Example 1: Defining Exposures

```python
# File: models/exposures.yml
exposures_yaml = """
version: 2

exposures:
  - name: sales_dashboard
    type: dashboard         # Options: dashboard, notebook, analysis, ml, application
    maturity: high          # high, medium, low (signals how critical this is)
    url: https://bi-tool.company.com/dashboards/sales

    description: >
      Executive Sales Dashboard showing daily/monthly revenue by region.
      Used in the Monday leadership review meeting.

    depends_on:
      - ref('fct_sales')
      - ref('dim_customer')
      - ref('dim_product')

    owner:
      name: Analytics Team
      email: analytics@company.com

  - name: churn_ml_model
    type: ml
    maturity: medium
    description: "Customer churn prediction model — trained weekly on customer features"
    depends_on:
      - ref('fct_customer_activity')
      - ref('dim_customer')
    owner:
      name: Data Science Team
      email: datascience@company.com
"""
print(exposures_yaml)
print("Exposures appear in: dbt docs serve → Lineage DAG shows who uses each model")
```

---

## 💻 Example 2: Analyses — Compiled But Not Materialized

```python
# File: analyses/monthly_revenue_check.sql
analysis_sql = """
-- This file is compiled by dbt (ref() works!) but NOT run automatically
-- Use for: ad-hoc investigations, exploratory SQL, UAT queries
-- Run manually: dbt compile → then copy SQL from target/compiled/

SELECT
    DATE_TRUNC('month', order_date) AS month,
    region,
    SUM(amount)                      AS total_revenue,
    COUNT(DISTINCT customer_id)      AS unique_customers,
    SUM(amount) / COUNT(DISTINCT customer_id) AS revenue_per_customer

FROM {{ ref('fct_sales') }}                   -- ref() works in analyses!

WHERE order_date >= DATEADD('month', -6, CURRENT_DATE())

GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
"""
print(analysis_sql)
print("\nCompile (but don't run): dbt compile --select analyses/monthly_revenue_check")
print("Compiled SQL will be in: target/compiled/my_project/analyses/")
```

---

## 💻 Example 3: dbt Semantic Layer / Metrics (dbt 1.6+)

```python
# File: models/metrics/schema.yml
metrics_yaml = """
version: 2

metrics:
  - name: total_revenue
    label: Total Revenue
    model: ref('fct_sales')
    description: "Sum of all completed order amounts"

    calculation_method: sum
    expression: amount

    timestamp: order_date
    time_grains: [day, week, month, quarter, year]

    dimensions:
      - region
      - customer_segment
      - product_category

    filters:
      - field: status
        operator: '='
        value: "'COMPLETE'"

  - name: order_count
    label: Order Count
    model: ref('fct_sales')
    calculation_method: count
    expression: order_id
    timestamp: order_date
    time_grains: [day, week, month]
    dimensions: [region]
"""
print(metrics_yaml)
print("""
Benefits of dbt Metrics:
- Single source of truth for KPI definitions
- Used by BI tools (Tableau, Looker, Mode) via the dbt Semantic Layer
- Prevents metric inconsistency across dashboards
""")
```

---

## 💻 Example 4: Documentation Best Practices

```python
doc_best_practices = {
    "Use doc blocks for long descriptions": """
        # In docs/orders.md:
        {% docs order_status %}
        The current status of the order. Possible values:
        - COMPLETE: Payment received, item shipped
        - PENDING:  Order placed, awaiting payment
        - FAILED:   Payment failed
        - CANCELLED: Customer cancelled
        {% enddocs %}

        # In schema.yml:
        columns:
          - name: status
            description: '{{ doc("order_status") }}'
    """,
    "Add meta tags for ownership": """
        models:
          - name: fct_sales
            meta:
              owner: data-engineering@company.com
              sla: "Refreshed daily by 7am UTC"
              data_classification: confidential
    """,
    "Tag models for selective runs": """
        config(tags = ['daily', 'finance', 'critical'])
        # Then run: dbt run --select tag:daily
    """,
}
for practice, example in doc_best_practices.items():
    print(f"✅ {practice}")
    print(example)
```

---

## 🏭 Summary

| Feature          | Purpose                                             |
| ---------------- | --------------------------------------------------- |
| `exposures:`     | Document dashboards/ML models that depend on dbt    |
| `analyses/`      | Write exploratory SQL that uses `ref()` and Jinja   |
| `metrics:`       | Central KPI definitions for BI tools                |
| `doc()` function | Share descriptions across multiple models           |
| `meta:`          | Custom metadata: owner, SLA, classification         |
| `tags:`          | Group models for selective `dbt run --select tag:X` |
