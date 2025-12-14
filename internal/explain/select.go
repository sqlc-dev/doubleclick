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
	// INTO OUTFILE clause - check if any SelectQuery has IntoOutfile set
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.IntoOutfile != nil {
			fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, sq.IntoOutfile.Filename)
			break
		}
	}
	// FORMAT clause - check if any SelectQuery has Format set
	var hasFormat bool
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
			Node(sb, sq.Format, depth+1)
			hasFormat = true
			break
		}
	}
	// When FORMAT is present, SETTINGS is output at SelectWithUnionQuery level
	if hasFormat {
		for _, sel := range n.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok && len(sq.Settings) > 0 {
				fmt.Fprintf(sb, "%s Set\n", indent)
				break
			}
		}
	}
}

func explainSelectQuery(sb *strings.Builder, n *ast.SelectQuery, indent string, depth int) {
	children := countSelectQueryChildren(n)
	fmt.Fprintf(sb, "%sSelectQuery (children %d)\n", indent, children)
	// WITH clause (ExpressionList) - output before columns
	if len(n.With) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.With))
		for _, w := range n.With {
			Node(sb, w, depth+2)
		}
	}
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
	// SETTINGS - output here if there's no FORMAT, otherwise it's at SelectWithUnionQuery level
	if len(n.Settings) > 0 && n.Format == nil {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainOrderByElement(sb *strings.Builder, n *ast.OrderByElement, indent string, depth int) {
	children := 1 // expression
	if n.WithFill {
		children++ // FillModifier
	}
	fmt.Fprintf(sb, "%sOrderByElement (children %d)\n", indent, children)
	Node(sb, n.Expression, depth+1)
	if n.WithFill {
		fillChildren := 0
		if n.FillFrom != nil {
			fillChildren++
		}
		if n.FillTo != nil {
			fillChildren++
		}
		if n.FillStep != nil {
			fillChildren++
		}
		if fillChildren > 0 {
			fmt.Fprintf(sb, "%s FillModifier (children %d)\n", indent, fillChildren)
			if n.FillFrom != nil {
				Node(sb, n.FillFrom, depth+2)
			}
			if n.FillTo != nil {
				Node(sb, n.FillTo, depth+2)
			}
			if n.FillStep != nil {
				Node(sb, n.FillStep, depth+2)
			}
		} else {
			fmt.Fprintf(sb, "%s FillModifier\n", indent)
		}
	}
}

func countSelectUnionChildren(n *ast.SelectWithUnionQuery) int {
	count := 1 // ExpressionList of selects
	// Check if any SelectQuery has IntoOutfile set
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.IntoOutfile != nil {
			count++
			break
		}
	}
	// Check if any SelectQuery has Format set
	var hasFormat bool
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
			count++
			hasFormat = true
			break
		}
	}
	// When FORMAT is present, SETTINGS is counted at this level
	if hasFormat {
		for _, sel := range n.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok && len(sq.Settings) > 0 {
				count++
				break
			}
		}
	}
	return count
}

func countSelectQueryChildren(n *ast.SelectQuery) int {
	count := 1 // columns ExpressionList
	// WITH clause
	if len(n.With) > 0 {
		count++
	}
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
	// SETTINGS is counted here only if there's no FORMAT
	// If FORMAT is present, SETTINGS is at SelectWithUnionQuery level
	if len(n.Settings) > 0 && n.Format == nil {
		count++
	}
	return count
}
