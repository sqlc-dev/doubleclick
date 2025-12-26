package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

func explainSelectIntersectExceptQuery(sb *strings.Builder, n *ast.SelectIntersectExceptQuery, indent string, depth int) {
	fmt.Fprintf(sb, "%sSelectIntersectExceptQuery (children %d)\n", indent, len(n.Selects))
	for _, sel := range n.Selects {
		Node(sb, sel, depth+1)
	}
}

func explainSelectWithUnionQuery(sb *strings.Builder, n *ast.SelectWithUnionQuery, indent string, depth int) {
	if n == nil {
		return
	}
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
	// QUALIFY
	if n.Qualify != nil {
		Node(sb, n.Qualify, depth+1)
	}
	// WINDOW clause (named window definitions)
	if len(n.Window) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Window))
		for range n.Window {
			fmt.Fprintf(sb, "%s  WindowListElement\n", indent)
		}
	}
	// ORDER BY
	if len(n.OrderBy) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.OrderBy))
		for _, o := range n.OrderBy {
			Node(sb, o, depth+2)
		}
	}
	// OFFSET (ClickHouse outputs offset before limit in EXPLAIN AST)
	if n.Offset != nil {
		Node(sb, n.Offset, depth+1)
	}
	// LIMIT
	if n.Limit != nil {
		Node(sb, n.Limit, depth+1)
	}
	// LIMIT BY - only output when there's no ORDER BY and no second LIMIT (matches ClickHouse behavior)
	if len(n.LimitBy) > 0 && len(n.OrderBy) == 0 && !n.LimitByHasLimit {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.LimitBy))
		for _, expr := range n.LimitBy {
			Node(sb, expr, depth+2)
		}
	}
	// SETTINGS - output here if there's no FORMAT, otherwise it's at SelectWithUnionQuery level
	if len(n.Settings) > 0 && n.Format == nil {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainOrderByElement(sb *strings.Builder, n *ast.OrderByElement, indent string, depth int) {
	// ClickHouse uses different formats for WITH FILL:
	// - When FROM/TO are simple literals: direct children on OrderByElement
	// - When FROM/TO are complex expressions or only STEP present: uses FillModifier wrapper
	hasFromOrTo := n.FillFrom != nil || n.FillTo != nil
	hasComplexFillExpr := hasFromOrTo && (isComplexExpr(n.FillFrom) || isComplexExpr(n.FillTo))

	// Use FillModifier when:
	// 1. Only STEP is present (no FROM/TO), or
	// 2. FROM/TO contain complex expressions (not simple literals)
	useFillModifier := n.WithFill && ((n.FillStep != nil && !hasFromOrTo) || hasComplexFillExpr)

	if useFillModifier {
		// Use FillModifier wrapper
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
		children := 2 // expression + FillModifier
		fmt.Fprintf(sb, "%sOrderByElement (children %d)\n", indent, children)
		Node(sb, n.Expression, depth+1)
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
		// Use direct children for simple literal FROM/TO cases
		children := 1 // expression
		if n.FillFrom != nil {
			children++
		}
		if n.FillTo != nil {
			children++
		}
		if n.FillStep != nil {
			children++
		}
		if n.Collate != "" {
			children++
		}
		fmt.Fprintf(sb, "%sOrderByElement (children %d)\n", indent, children)
		Node(sb, n.Expression, depth+1)
		if n.FillFrom != nil {
			Node(sb, n.FillFrom, depth+1)
		}
		if n.FillTo != nil {
			Node(sb, n.FillTo, depth+1)
		}
		if n.FillStep != nil {
			Node(sb, n.FillStep, depth+1)
		}
		if n.Collate != "" {
			// COLLATE is output as a string literal
			fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, n.Collate)
		}
	}
}

// isComplexExpr checks if an expression is complex (not a simple literal)
func isComplexExpr(expr ast.Expression) bool {
	if expr == nil {
		return false
	}
	switch expr.(type) {
	case *ast.Literal:
		return false
	default:
		return true
	}
}

// hasOnlyLiterals checks if all expressions in a slice are literals
func hasOnlyLiterals(exprs []ast.Expression) bool {
	for _, expr := range exprs {
		if _, ok := expr.(*ast.Literal); !ok {
			return false
		}
	}
	return true
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
	if n.Qualify != nil {
		count++
	}
	if len(n.Window) > 0 {
		count++
	}
	if len(n.OrderBy) > 0 {
		count++
	}
	if n.Limit != nil {
		count++
	}
	if len(n.LimitBy) > 0 && len(n.OrderBy) == 0 && !n.LimitByHasLimit {
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
