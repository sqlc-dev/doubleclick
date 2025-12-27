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

// formatComments writes comments to the builder.
func formatComments(sb *strings.Builder, comments []*ast.Comment) {
	for _, c := range comments {
		sb.WriteString(c.Text)
		sb.WriteString("\n")
	}
}

// formatTrailingComments writes trailing comments (on same line) to the builder.
func formatTrailingComments(sb *strings.Builder, comments []*ast.Comment) {
	for _, c := range comments {
		sb.WriteString(" ")
		sb.WriteString(c.Text)
	}
}

// Statement formats a single statement.
func Statement(sb *strings.Builder, stmt ast.Statement) {
	if stmt == nil {
		return
	}

	// Handle statement with comments wrapper
	if swc, ok := stmt.(*ast.StatementWithComments); ok {
		formatComments(sb, swc.LeadingComments)
		Statement(sb, swc.Statement)
		formatTrailingComments(sb, swc.TrailingComments)
		return
	}

	switch s := stmt.(type) {
	case *ast.SelectWithUnionQuery:
		formatSelectWithUnionQuery(sb, s)
	case *ast.SelectQuery:
		formatSelectQuery(sb, s)
	case *ast.SelectIntersectExceptQuery:
		formatSelectIntersectExceptQuery(sb, s)
	case *ast.SetQuery:
		formatSetQuery(sb, s)
	case *ast.DropQuery:
		formatDropQuery(sb, s)
	case *ast.CreateQuery:
		formatCreateQuery(sb, s)
	case *ast.InsertQuery:
		formatInsertQuery(sb, s)
	case *ast.AlterQuery:
		formatAlterQuery(sb, s)
	case *ast.TruncateQuery:
		formatTruncateQuery(sb, s)
	case *ast.UseQuery:
		formatUseQuery(sb, s)
	case *ast.DescribeQuery:
		formatDescribeQuery(sb, s)
	case *ast.ShowQuery:
		formatShowQuery(sb, s)
	case *ast.ExplainQuery:
		formatExplainQuery(sb, s)
	case *ast.OptimizeQuery:
		formatOptimizeQuery(sb, s)
	case *ast.SystemQuery:
		formatSystemQuery(sb, s)
	case *ast.RenameQuery:
		formatRenameQuery(sb, s)
	case *ast.ExchangeQuery:
		formatExchangeQuery(sb, s)
	case *ast.ExistsQuery:
		formatExistsQueryStmt(sb, s)
	default:
		// Fallback for unhandled statements
	}
}
