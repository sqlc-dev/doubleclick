// Package format provides SQL formatting for ClickHouse AST.
package format

import (
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// Format returns the SQL string representation of the statements.
func Format(stmts []ast.Statement) string {
	var sb strings.Builder
	for i, stmt := range stmts {
		if i > 0 {
			sb.WriteString("\n")
		}
		Statement(&sb, stmt)
		sb.WriteString(";")
	}
	return sb.String()
}

// Statement formats a single statement.
func Statement(sb *strings.Builder, stmt ast.Statement) {
	if stmt == nil {
		return
	}

	switch s := stmt.(type) {
	case *ast.SelectWithUnionQuery:
		formatSelectWithUnionQuery(sb, s)
	case *ast.SelectQuery:
		formatSelectQuery(sb, s)
	default:
		// For now, only handle SELECT statements
	}
}
