// Package explain provides EXPLAIN AST output functionality for ClickHouse SQL.
package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

// Explain returns the EXPLAIN AST output for a statement, matching ClickHouse's format.
func Explain(stmt ast.Statement) string {
	var sb strings.Builder
	Node(&sb, stmt, 0)
	return sb.String()
}

// Node writes the EXPLAIN AST output for an AST node.
func Node(sb *strings.Builder, node interface{}, depth int) {
	if node == nil {
		// nil can represent an empty tuple in function arguments
		indent := strings.Repeat(" ", depth)
		fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList\n", indent)
		return
	}

	indent := strings.Repeat(" ", depth)

	switch n := node.(type) {
	// Select statements
	case *ast.SelectWithUnionQuery:
		explainSelectWithUnionQuery(sb, n, indent, depth)
	case *ast.SelectQuery:
		explainSelectQuery(sb, n, indent, depth)

	// Tables
	case *ast.TablesInSelectQuery:
		explainTablesInSelectQuery(sb, n, indent, depth)
	case *ast.TablesInSelectQueryElement:
		explainTablesInSelectQueryElement(sb, n, indent, depth)
	case *ast.TableExpression:
		explainTableExpression(sb, n, indent, depth)
	case *ast.TableIdentifier:
		explainTableIdentifier(sb, n, indent)
	case *ast.ArrayJoinClause:
		explainArrayJoinClause(sb, n, indent, depth)
	case *ast.TableJoin:
		explainTableJoin(sb, n, indent, depth)

	// Expressions
	case *ast.OrderByElement:
		explainOrderByElement(sb, n, indent, depth)
	case *ast.Identifier:
		explainIdentifier(sb, n, indent)
	case *ast.Literal:
		explainLiteral(sb, n, indent, depth)
	case *ast.BinaryExpr:
		explainBinaryExpr(sb, n, indent, depth)
	case *ast.UnaryExpr:
		explainUnaryExpr(sb, n, indent, depth)
	case *ast.Subquery:
		explainSubquery(sb, n, indent, depth)
	case *ast.AliasedExpr:
		explainAliasedExpr(sb, n, depth)
	case *ast.WithElement:
		explainWithElement(sb, n, indent, depth)
	case *ast.Asterisk:
		explainAsterisk(sb, n, indent)

	// Functions
	case *ast.FunctionCall:
		explainFunctionCall(sb, n, indent, depth)
	case *ast.Lambda:
		explainLambda(sb, n, indent, depth)
	case *ast.CastExpr:
		explainCastExpr(sb, n, indent, depth)
	case *ast.InExpr:
		explainInExpr(sb, n, indent, depth)
	case *ast.TernaryExpr:
		explainTernaryExpr(sb, n, indent, depth)
	case *ast.ArrayAccess:
		explainArrayAccess(sb, n, indent, depth)
	case *ast.TupleAccess:
		explainTupleAccess(sb, n, indent, depth)
	case *ast.LikeExpr:
		explainLikeExpr(sb, n, indent, depth)
	case *ast.BetweenExpr:
		explainBetweenExpr(sb, n, indent, depth)
	case *ast.IsNullExpr:
		explainIsNullExpr(sb, n, indent, depth)
	case *ast.CaseExpr:
		explainCaseExpr(sb, n, indent, depth)
	case *ast.IntervalExpr:
		explainIntervalExpr(sb, n, indent, depth)
	case *ast.ExistsExpr:
		explainExistsExpr(sb, n, indent, depth)
	case *ast.ExtractExpr:
		explainExtractExpr(sb, n, indent, depth)

	// DDL statements
	case *ast.InsertQuery:
		explainInsertQuery(sb, n, indent, depth)
	case *ast.CreateQuery:
		explainCreateQuery(sb, n, indent, depth)
	case *ast.DropQuery:
		explainDropQuery(sb, n, indent)
	case *ast.SetQuery:
		explainSetQuery(sb, indent)
	case *ast.SystemQuery:
		explainSystemQuery(sb, indent)
	case *ast.ExplainQuery:
		explainExplainQuery(sb, n, indent, depth)
	case *ast.ShowQuery:
		explainShowQuery(sb, n, indent)
	case *ast.UseQuery:
		explainUseQuery(sb, n, indent)
	case *ast.DescribeQuery:
		explainDescribeQuery(sb, n, indent)

	// Types
	case *ast.DataType:
		explainDataType(sb, n, indent, depth)
	case *ast.Parameter:
		explainParameter(sb, n, indent)

	default:
		// For unhandled types, just print the type name
		fmt.Fprintf(sb, "%s%T\n", indent, node)
	}
}

// TablesWithArrayJoin handles FROM and ARRAY JOIN together as TablesInSelectQuery
func TablesWithArrayJoin(sb *strings.Builder, from *ast.TablesInSelectQuery, arrayJoin *ast.ArrayJoinClause, depth int) {
	indent := strings.Repeat(" ", depth)

	tableCount := 0
	if from != nil {
		tableCount = len(from.Tables)
	}
	if arrayJoin != nil {
		tableCount++
	}

	fmt.Fprintf(sb, "%sTablesInSelectQuery (children %d)\n", indent, tableCount)

	if from != nil {
		for _, t := range from.Tables {
			Node(sb, t, depth+1)
		}
	}

	if arrayJoin != nil {
		// ARRAY JOIN is wrapped in TablesInSelectQueryElement
		fmt.Fprintf(sb, "%s TablesInSelectQueryElement (children %d)\n", indent, 1)
		Node(sb, arrayJoin, depth+2)
	}
}

// Column handles column declarations
func Column(sb *strings.Builder, col *ast.ColumnDeclaration, depth int) {
	indent := strings.Repeat(" ", depth)
	children := 0
	if col.Type != nil {
		children++
	}
	if col.Default != nil {
		children++
	}
	fmt.Fprintf(sb, "%sColumnDeclaration %s (children %d)\n", indent, col.Name, children)
	if col.Type != nil {
		Node(sb, col.Type, depth+1)
	}
	if col.Default != nil {
		Node(sb, col.Default, depth+1)
	}
}
