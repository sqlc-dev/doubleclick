package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

func explainSelectWithUnionQuery(sb *strings.Builder, n *ast.SelectWithUnionQuery, indent string, depth int) {
	children := countSelectUnionChildren(n)
	fmt.Fprintf(sb, "%sSelectWithUnionQuery (children %d)\n", indent, children)
	// Wrap selects in ExpressionList
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Selects))
	for _, sel := range n.Selects {
		Node(sb, sel, depth+2)
	}
	// FORMAT clause - check if any SelectQuery has Format set
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
			Node(sb, sq.Format, depth+1)
			break
		}
	}
}

func explainSelectQuery(sb *strings.Builder, n *ast.SelectQuery, indent string, depth int) {
	children := countSelectQueryChildren(n)
	fmt.Fprintf(sb, "%sSelectQuery (children %d)\n", indent, children)
	// Columns (ExpressionList)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Columns))
	for _, col := range n.Columns {
		Node(sb, col, depth+2)
	}
	// FROM (including ARRAY JOIN as part of TablesInSelectQuery)
	if n.From != nil || n.ArrayJoin != nil {
		TablesWithArrayJoin(sb, n.From, n.ArrayJoin, depth+1)
	}
	// PREWHERE
	if n.PreWhere != nil {
		Node(sb, n.PreWhere, depth+1)
	}
	// WHERE
	if n.Where != nil {
		Node(sb, n.Where, depth+1)
	}
	// GROUP BY
	if len(n.GroupBy) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.GroupBy))
		for _, g := range n.GroupBy {
			Node(sb, g, depth+2)
		}
	}
	// HAVING
	if n.Having != nil {
		Node(sb, n.Having, depth+1)
	}
	// ORDER BY
	if len(n.OrderBy) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.OrderBy))
		for _, o := range n.OrderBy {
			Node(sb, o, depth+2)
		}
	}
	// LIMIT
	if n.Limit != nil {
		Node(sb, n.Limit, depth+1)
	}
	// OFFSET
	if n.Offset != nil {
		Node(sb, n.Offset, depth+1)
	}
	// SETTINGS
	if len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainOrderByElement(sb *strings.Builder, n *ast.OrderByElement, indent string, depth int) {
	fmt.Fprintf(sb, "%sOrderByElement (children %d)\n", indent, 1)
	Node(sb, n.Expression, depth+1)
}

func countSelectUnionChildren(n *ast.SelectWithUnionQuery) int {
	count := 1 // ExpressionList of selects
	// Check if any SelectQuery has Format set
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
			count++
			break
		}
	}
	return count
}

func countSelectQueryChildren(n *ast.SelectQuery) int {
	count := 1 // columns ExpressionList
	// FROM and ARRAY JOIN together count as one child (TablesInSelectQuery)
	if n.From != nil || n.ArrayJoin != nil {
		count++
	}
	if n.PreWhere != nil {
		count++
	}
	if n.Where != nil {
		count++
	}
	if len(n.GroupBy) > 0 {
		count++
	}
	if n.Having != nil {
		count++
	}
	if len(n.OrderBy) > 0 {
		count++
	}
	if n.Limit != nil {
		count++
	}
	if n.Offset != nil {
		count++
	}
	if len(n.Settings) > 0 {
		count++
	}
	return count
}
