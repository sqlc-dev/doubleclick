# Claude Development Guide

## Next Steps

To find the next test to work on, run:

```bash
go run ./cmd/next-test
```

This tool finds all tests with `todo: true` in their metadata and returns the one with the shortest `query.sql` file.

To find the next format roundtrip test to work on, run:

```bash
go run ./cmd/next-test -format
```

This finds tests with `todo_format: true` in their metadata.

## Workflow

1. Run `go run ./cmd/next-test` to find the next test to implement
2. Check the test's `query.sql` to understand what ClickHouse SQL needs parsing
3. Check the test's `explain.txt` to understand the expected EXPLAIN output
4. Implement the necessary AST types in `ast/`
5. Add parser logic in `parser/parser.go`
6. Update the `Explain()` function if needed to match ClickHouse's output format
7. Enable the test by removing `todo: true` from its `metadata.json` (set it to `{}`)
8. Run `go test ./parser/... -timeout 5s` to verify
9. Check if other todo tests now pass (see below)

## Running Tests

Always run parser tests with a 5 second timeout:

```bash
go test ./parser/... -timeout 5s
```

The tests are very fast. If a test is timing out, it indicates a bug (likely an infinite loop in the parser).

## Checking for Newly Passing Todo Tests

After implementing parser changes, run:

```bash
go test ./parser/... -check-skipped -v 2>&1 | grep "PASSES NOW"
```

Tests that output `PASSES NOW` can have their `todo` flag removed from `metadata.json`. This helps identify when parser improvements fix multiple tests at once.

## Checking for Newly Passing Format Tests

After implementing format changes, run:

```bash
go test ./parser/... -check-format -v 2>&1 | grep "FORMAT PASSES NOW"
```

Tests that output `FORMAT PASSES NOW` can have their `todo_format` flag removed from `metadata.json`.

## Test Structure

Each test in `parser/testdata/` contains:

- `metadata.json` - `{}` for enabled tests, `{"todo": true}` for pending tests
- `query.sql` - ClickHouse SQL to parse
- `explain.txt` - Expected EXPLAIN AST output (matches ClickHouse's format)

### Metadata Options

- `todo: true` - Test is pending parser/explain implementation
- `todo_format: true` - Format roundtrip test is pending implementation
- `skip: true` - Skip test entirely (e.g., causes infinite loop)
- `explain: false` - Skip test (e.g., ClickHouse couldn't parse it)
- `parse_error: true` - Query is intentionally invalid SQL

## Important Rules

**NEVER modify `explain.txt` files** - These are golden files containing the expected output from ClickHouse. If tests fail due to output mismatches, fix the Go code to match the expected output, not the other way around.
