# Claude Development Guide

## Next Steps

To find the next format roundtrip test to work on, run:

```bash
go run ./cmd/next-test -format
```

This finds tests with `todo_format: true` in their metadata.

## Running Tests

Always run parser tests with a 5 second timeout:

```bash
go test ./parser/... -timeout 5s
```

The tests are very fast. If a test is timing out, it indicates a bug (likely an infinite loop in the parser).

## Checking for Newly Passing Format Tests

After implementing format changes, run:

```bash
go test ./parser/... -check-format -v 2>&1 | grep "FORMAT PASSES NOW"
```

Tests that output `FORMAT PASSES NOW` can have their `todo_format` flag removed from `metadata.json`.

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

- `todo_format: true` - Format roundtrip test is pending implementation
- `explain_todo: {"stmt2": true}` - Skip specific statement subtests
- `skip: true` - Skip test entirely (e.g., causes infinite loop)
- `explain: false` - Skip test (e.g., ClickHouse couldn't parse it)
- `parse_error: true` - Query is intentionally invalid SQL

## Important Rules

**NEVER modify `explain.txt` files** - These are golden files containing the expected output from ClickHouse. If tests fail due to output mismatches, fix the Go code to match the expected output, not the other way around.
