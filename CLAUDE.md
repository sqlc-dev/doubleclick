# Claude Development Guide

## Next Steps

To find the next test to work on, run:

```bash
go run ./cmd/next-test
```

This tool finds all tests with `todo: true` in their metadata and returns the one with the shortest `query.sql` file.

## Workflow

1. Run `go run ./cmd/next-test` to find the next test to implement
2. Check the test's `query.sql` to understand what SQL needs parsing
3. Check the test's `ast.json` to understand the expected output format
4. Implement the necessary AST types in `ast/`
5. Add parser logic in `parser/parser.go`
6. Add JSON marshaling functions in `parser/parser.go`
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
go test ./parser/... -only-todo -v 2>&1 | grep "PASS:"
```

This shows any todo tests that now pass. Enable those tests by removing `todo: true` from their `metadata.json`.

Available test flags:

- `-only-todo` - Run only todo/invalid_syntax tests (find newly passing tests)
- `-run-todo` - Run todo/invalid_syntax tests along with normal tests

## Test Structure

Each test in `parser/testdata/` contains:

- `metadata.json` - `{}` for enabled tests, `{"todo": true}` for pending tests, or `{"invalid_syntax": true}` for tests with invalid SQL
- `query.sql` - T-SQL to parse
- `ast.json` - Expected AST output

## Important Rules

**NEVER modify `ast.json` files** - These are golden files containing the expected output. If tests fail due to JSON mismatches, fix the Go code to match the expected output, not the other way around.

## Generating ast.json with TsqlAstParser

The `TsqlAstParser/` directory contains a C# tool that generates `ast.json` files using Microsoft's official T-SQL parser (ScriptDom).

### Prerequisites

1. Install .NET 8.0 SDK:

```bash
curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --channel 8.0 --install-dir ~/.dotnet
```

2. Download the NuGet package (if `packages/` directory is empty):

```bash
mkdir -p packages
curl -L -o packages/microsoft.sqlserver.transactsql.scriptdom.170.128.0.nupkg \
  "https://api.nuget.org/v3-flatcontainer/microsoft.sqlserver.transactsql.scriptdom/170.128.0/microsoft.sqlserver.transactsql.scriptdom.170.128.0.nupkg"
```

3. Build the tool:

```bash
~/.dotnet/dotnet build TsqlAstParser -c Release
```

### Usage

Generate `ast.json` for a single test:

```bash
~/.dotnet/dotnet run --project TsqlAstParser -c Release -- parser/testdata/TestName/query.sql parser/testdata/TestName/ast.json
```

Generate `ast.json` for all tests missing it:

```bash
for dir in parser/testdata/*/; do
  if [ -f "$dir/query.sql" ] && [ ! -f "$dir/ast.json" ]; then
    ~/.dotnet/dotnet run --project TsqlAstParser -c Release -- "$dir/query.sql" "$dir/ast.json"
  fi
done
```

### Limitations

TsqlAstParser uses TSql160Parser (SQL Server 2022) and cannot parse:

- SQL Server 170+ features (VECTOR indexes, AI functions, JSON enhancements)
- Fabric DW-specific syntax (CLONE TABLE, CLUSTER BY)
- Deprecated syntax removed in newer versions
- Intentionally invalid SQL (error test cases)

Tests for unsupported syntax will not have `ast.json` files generated.
