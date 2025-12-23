package parser

import (
	"github.com/sqlc-dev/doubleclick/ast"
	"github.com/sqlc-dev/doubleclick/internal/explain"
)

// Explain returns the EXPLAIN AST output for a statement, matching ClickHouse's format.
func Explain(stmt ast.Statement) string {
	return explain.Explain(stmt)
}
