package parser

import (
	"github.com/sqlc-dev/doubleclick/ast"
	"github.com/sqlc-dev/doubleclick/internal/explain"
)

// Explain returns the EXPLAIN AST output for a statement, matching ClickHouse's format.
func Explain(stmt ast.Statement) string {
	return explain.Explain(stmt)
}

// ExplainStatements returns the EXPLAIN AST output for multiple statements.
// This handles the special ClickHouse behavior where INSERT VALUES followed by SELECT
// on the same line outputs the INSERT AST and then executes the SELECT, printing its result.
func ExplainStatements(stmts []ast.Statement) string {
	return explain.ExplainStatements(stmts)
}
