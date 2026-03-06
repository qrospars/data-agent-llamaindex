# Safety model

The SQL validation and execution path blocks risky SQL by default:

- only `SELECT` / `WITH`
- single-statement queries
- comments are stripped before validation
- no `INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/GRANT/REVOKE/CREATE/REPLACE`
- blocks known system schemas/tables (`sqlite_master`, `information_schema`, `pg_catalog`)
- forbidden-table constraints from app/semantic config
- optional allow-list table enforcement
- row limiting before execution with max-row cap

Use least-privilege database users in production and keep `max_row_limit` conservative.
