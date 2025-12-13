package parser

import (
	"github.com/kyleconroy/doubleclick/ast"
	"github.com/kyleconroy/doubleclick/internal/explain"
)

// Explain returns the EXPLAIN AST output for a statement, matching ClickHouse's format.
func Explain(stmt ast.Statement) string {
	return explain.Explain(stmt)
}
