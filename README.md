# doubleclick

A ClickHouse SQL parser written in Go. Parses ClickHouse SQL syntax into an Abstract Syntax Tree (AST) and generates EXPLAIN output matching ClickHouse's format.

## Installation

```bash
go get github.com/sqlc-dev/doubleclick
```

## Usage

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/parser"
)

func main() {
	sql := `SELECT id, name FROM users WHERE active = 1 ORDER BY created_at DESC LIMIT 10`

	stmts, err := parser.Parse(context.Background(), strings.NewReader(sql))
	if err != nil {
		panic(err)
	}

	// Serialize to JSON
	jsonBytes, _ := json.MarshalIndent(stmts[0], "", "  ")
	fmt.Println(string(jsonBytes))

	// Or print EXPLAIN AST output (matches ClickHouse format)
	fmt.Println(parser.Explain(stmts[0]))
}
```

JSON output:

```json
{
  "selects": [
    {
      "columns": [
        { "parts": ["id"] },
        { "parts": ["name"] }
      ],
      "from": {
        "tables": [
          { "table": { "table": { "table": "users" } } }
        ]
      },
      "where": {
        "left": { "parts": ["active"] },
        "op": "=",
        "right": { "type": "Integer", "value": 1 }
      },
      "order_by": [
        {
          "expression": { "parts": ["created_at"] },
          "descending": true
        }
      ],
      "limit": { "type": "Integer", "value": 10 }
    }
  ]
}
```

EXPLAIN output:

```
SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 4)
   ExpressionList (children 2)
    Identifier id
    Identifier name
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      TableIdentifier (children 1)
       Identifier users
   Function equals (children 1)
    ExpressionList (children 2)
     Identifier active
     Literal UInt64_1
   ExpressionList (children 1)
    OrderByElement (children 1)
     Identifier created_at
   Literal UInt64_10
```

## Features

- Parses SELECT, INSERT, CREATE, DROP, ALTER, and other ClickHouse statements
- Handles ClickHouse-specific syntax (Array types, PREWHERE, SAMPLE, etc.)
- Supports JOINs, subqueries, CTEs, window functions, and complex expressions
- Generates JSON-serializable AST nodes
- Produces EXPLAIN AST output matching ClickHouse's format
