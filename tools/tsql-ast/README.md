# T-SQL AST to JSON Parser

A .NET tool that uses Microsoft's [SqlScriptDOM](https://github.com/microsoft/SqlScriptDOM) library to parse T-SQL statements into an Abstract Syntax Tree (AST) and serialize it to JSON.

## Prerequisites

- .NET 8.0 SDK or later
- Internet access for NuGet package restoration

## Installation

```bash
cd tools/tsql-ast/TsqlAstParser
dotnet restore
dotnet build
```

## Usage

### Parse a SQL file

```bash
dotnet run -- /path/to/query.sql
```

### Parse and save to JSON file

```bash
dotnet run -- /path/to/query.sql /path/to/output/ast.json
```

### Parse from stdin

```bash
echo "SELECT 1" | dotnet run -- --stdin
```

### Parse from stdin and save to file

```bash
echo "SELECT * FROM users WHERE id = 1" | dotnet run -- --stdin ast.json
```

## Output Format

The tool outputs a JSON object with either:

1. **Successful parse** - Contains the full AST under the `ast` key
2. **Parse errors** - Contains error details under the `errors` key

### Example Output (SELECT 1)

```json
{
  "ast": {
    "_type": "TSqlScript",
    "Batches": [
      {
        "_type": "TSqlBatch",
        "Statements": [
          {
            "_type": "SelectStatement",
            "QueryExpression": {
              "_type": "QuerySpecification",
              "SelectElements": [
                {
                  "_type": "SelectScalarExpression",
                  "Expression": {
                    "_type": "IntegerLiteral",
                    "Value": "1"
                  }
                }
              ]
            }
          }
        ]
      }
    ]
  }
}
```

### Example Error Output

```json
{
  "errors": [
    {
      "line": 1,
      "column": 8,
      "message": "Incorrect syntax near 'SELEC'.",
      "number": 46010
    }
  ]
}
```

## AST Node Types

The AST uses the types from SqlScriptDOM. Each node has a `_type` field indicating its class name. Common node types include:

### Statements
- `TSqlScript` - Root node containing batches
- `TSqlBatch` - A batch of statements
- `SelectStatement` - SELECT queries
- `InsertStatement` - INSERT statements
- `UpdateStatement` - UPDATE statements
- `DeleteStatement` - DELETE statements
- `CreateTableStatement` - CREATE TABLE DDL
- `CreateProcedureStatement` - Stored procedure definitions

### Expressions
- `ColumnReferenceExpression` - Column references
- `IntegerLiteral`, `StringLiteral`, `NumericLiteral` - Literals
- `BinaryExpression` - Binary operations (AND, OR, +, -, etc.)
- `FunctionCall` - Function invocations
- `ScalarSubquery` - Subqueries in expressions

### Clauses
- `FromClause` - FROM clause with table references
- `WhereClause` - WHERE conditions
- `JoinTableReference` - JOIN operations
- `GroupByClause` - GROUP BY groupings
- `OrderByClause` - ORDER BY specifications

## SQL Server Version Support

The tool uses `TSql160Parser` which supports SQL Server 2022 (version 16) syntax. This includes all T-SQL features up to and including SQL Server 2022.

For older SQL versions, modify `Program.cs` to use:
- `TSql150Parser` - SQL Server 2019
- `TSql140Parser` - SQL Server 2017
- `TSql130Parser` - SQL Server 2016
- `TSql120Parser` - SQL Server 2014

## Integration with Test Suite

To generate `ast.json` files for the test suite:

```bash
# For each test directory
for dir in parser/testdata/*/; do
    if [ -f "$dir/query.sql" ]; then
        dotnet run -- "$dir/query.sql" "$dir/ast.json"
    fi
done
```

## References

- [SqlScriptDOM GitHub](https://github.com/microsoft/SqlScriptDOM)
- [ScriptDom NuGet Package](https://www.nuget.org/packages/Microsoft.SqlServer.TransactSql.ScriptDom)
- [TSqlParser Documentation](https://learn.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.transactsql.scriptdom.tsqlparser)
- [Azure SQL Blog: Parsing T-SQL with ScriptDom](https://devblogs.microsoft.com/azure-sql/programmatically-parsing-transact-sql-t-sql-with-the-scriptdom-parser/)
