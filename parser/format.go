package parser

import (
	"github.com/sqlc-dev/doubleclick/ast"
	"github.com/sqlc-dev/doubleclick/internal/format"
)

// Format returns the SQL string representation of the statements.
func Format(stmts []ast.Statement) string {
	return format.Format(stmts)
}
