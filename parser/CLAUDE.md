# Parser Development Notes

## Test Data Files

The `testdata/` directory contains test cases from ClickHouse. Each test has:

- `query.sql` - The SQL query to parse
- `explain.txt` - **DO NOT MODIFY** - This is the ground truth output from ClickHouse's `EXPLAIN AST` command. Our parser must produce output that matches this exactly.
- `metadata.json` - Test metadata (todo, skip, etc.)
- `ast.json` - Optional golden file for AST regression testing

To fix a failing test, you must fix the **parser** to produce output matching `explain.txt`, never modify `explain.txt` itself.

## Running Tests

Always run parser tests with a 5 second timeout:

```bash
go test ./parser/... -timeout 5s
```

The tests are very fast. If a test is timing out, it indicates a bug (likely an infinite loop in the parser).

## Checking Skipped Tests

After fixing parser issues, check if any skipped tests now pass:

```bash
go test ./parser -check-skipped -v 2>&1 | grep "PASSES NOW"
```

Tests that output `PASSES NOW` can have their `todo` flag removed from `metadata.json`. This helps identify when parser improvements fix multiple tests at once.
