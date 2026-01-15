---
name: Parser Bug
about: Report a SQL query that doesn't parse correctly
title: ''
labels: bug
assignees: ''
---

## SQL Query

Please provide the **complete** SQL query that fails to parse:

```sql
-- Paste your SQL query here
```

## Expected Behavior

Describe what you expected to happen.

## Actual Behavior

Describe what actually happened (error message, incorrect AST, etc.).

## EXPLAIN AST Output (Optional but Helpful)

If possible, provide the ClickHouse `EXPLAIN AST` output for comparison:

```
-- Run: clickhouse client --query "EXPLAIN AST <your query>"
-- Paste the output here
```

## Additional Context

- ClickHouse version (if relevant):
- Any other context about the problem:
