# Safety model

`data-agent-core` now enforces safety at two levels.

## SQL runtime safety
The SQL validation/execution path blocks risky SQL by default:
- only `SELECT` / `WITH`
- single-statement queries
- comments stripped before validation
- blocks mutating keywords (`INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/GRANT/REVOKE/CREATE/REPLACE`)
- blocks system schema/table access (`sqlite_master`, `information_schema`, `pg_catalog`)
- supports forbidden-table constraints
- supports allow-list table enforcement
- row limiting with max-row cap

## Analytics copilot safety
- Deterministic modules execute via the existing safe SQL runtime path.
- Semantic metric validation is fail-closed:
  - unknown metric/dimension references are rejected
  - metric expressions must be safe/read-only and column-bounded
- Synthesis layer consumes structured evidence bundles only.
- Synthesis prompt requires caveats and forbids invented causes.

## Operational guidance
- Use least-privilege DB users in production.
- Keep `max_row_limit` conservative.
- Review generated semantic artifacts under `.state/semantics/` before production use.
- Treat anomaly/findings output as decision support, not automatic root-cause proof.
