# Claude Development Guide

## Next Steps

To find the next explain test to work on (fewest pending statements first), run:

```bash
go run ./cmd/next-test
```

This finds tests with `explain_todo` entries in their metadata.

## Running Tests

Always run parser tests with a 10 second timeout:

```bash
go test ./parser/... -timeout 10s
```

The tests are very fast. If a test is timing out, it indicates a bug (likely an infinite loop in the parser).

## Checking for Newly Passing Explain Tests

After implementing parser/explain changes, run:

```bash
go test ./parser/... -check-explain -v 2>&1 | grep "EXPLAIN PASSES NOW"
```

Tests that output `EXPLAIN PASSES NOW` can have their statement removed from `explain_todo` in `metadata.json`.

## Test Structure

Each test in `parser/testdata/` contains:

- `metadata.json` - `{}` for enabled tests
- `query.sql` - ClickHouse SQL to parse
- `explain.txt` - Expected EXPLAIN AST output (matches ClickHouse's format)
- `explain_N.txt` - Expected EXPLAIN AST output for Nth statement (N >= 2)

### Metadata Options

- `explain_todo: {"stmt2": true}` - Skip specific statement subtests
- `skip: true` - Skip test entirely (e.g., causes infinite loop)
- `explain: false` - Skip test (e.g., ClickHouse couldn't parse it)
- `parse_error: true` - Query is intentionally invalid SQL

## Important Rules

**NEVER modify `explain.txt` files** - These are golden files containing the expected output from ClickHouse. If tests fail due to output mismatches, fix the Go code to match the expected output, not the other way around.
