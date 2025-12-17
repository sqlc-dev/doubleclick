# Parser Development Notes

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
