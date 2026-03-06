# Semantic config

`data-agent-core` loads YAML/JSON semantic metadata.

```yaml
project: retail_demo
tables:
  - name: sales_daily
    description: Daily revenue by store and category.
    preferred: true
columns:
  - table: sales_daily
    name: revenue
    description: Gross revenue in USD.
business_rules:
  - Prefer preferred views before raw tables.
examples:
  - question: What were top categories yesterday?
    sql: SELECT category, SUM(revenue) FROM sales_daily GROUP BY 1
```
