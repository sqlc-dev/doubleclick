package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

func explainSelectIntersectExceptQuery(sb *strings.Builder, n *ast.SelectIntersectExceptQuery, indent string, depth int) {
	fmt.Fprintf(sb, "%sSelectIntersectExceptQuery (children %d)\n", indent, len(n.Selects))

	// ClickHouse wraps first operand in SelectWithUnionQuery when EXCEPT is present
	hasExcept := false
	for _, op := range n.Operators {
		if op == "EXCEPT" {
			hasExcept = true
			break
		}
	}

	childIndent := strings.Repeat(" ", depth+1)
	for i, sel := range n.Selects {
		if hasExcept && i == 0 {
			// Wrap first operand in SelectWithUnionQuery -> ExpressionList format
			fmt.Fprintf(sb, "%sSelectWithUnionQuery (children 1)\n", childIndent)
			fmt.Fprintf(sb, "%s ExpressionList (children 1)\n", childIndent)
			Node(sb, sel, depth+3)
		} else {
			Node(sb, sel, depth+1)
		}
	}
}

func explainSelectWithUnionQuery(sb *strings.Builder, n *ast.SelectWithUnionQuery, indent string, depth int) {
	if n == nil {
		return
	}
	children := countSelectUnionChildren(n)
	fmt.Fprintf(sb, "%sSelectWithUnionQuery (children %d)\n", indent, children)
	// ClickHouse optimizes UNION ALL when selects have identical expressions but different aliases.
	// In that case, only the first SELECT is shown since column names come from the first SELECT anyway.
	selects := simplifyUnionSelects(n.Selects)
	// Wrap selects in ExpressionList
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(selects))
	for _, sel := range selects {
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
	// Skip this when inside CreateQuery context, as Format is output at CreateQuery level
	if !inCreateQueryContext {
		for _, sel := range n.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
				Node(sb, sq.Format, depth+1)
				break
			}
		}
	}
	// When SETTINGS comes AFTER FORMAT, it's output at SelectWithUnionQuery level
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.SettingsAfterFormat && len(sq.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
			break
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
	// GROUP BY (skip for GROUP BY ALL which doesn't output an expression list)
	if len(n.GroupBy) > 0 && !n.GroupByAll {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.GroupBy))
		for _, g := range n.GroupBy {
			if n.GroupingSets {
				// Each grouping set is wrapped in an ExpressionList
				// but we need to unwrap tuples and output elements directly
				if lit, ok := g.(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
					if elements, ok := lit.Value.([]ast.Expression); ok {
						fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(elements))
						for _, elem := range elements {
							Node(sb, elem, depth+3)
						}
					} else {
						// Fallback for unexpected tuple value type
						fmt.Fprintf(sb, "%s  ExpressionList (children 1)\n", indent)
						Node(sb, g, depth+3)
					}
				} else {
					// Single expression grouping set
					fmt.Fprintf(sb, "%s  ExpressionList (children 1)\n", indent)
					Node(sb, g, depth+3)
				}
			} else {
				Node(sb, g, depth+2)
			}
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
	// INTERPOLATE
	if len(n.Interpolate) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Interpolate))
		for _, i := range n.Interpolate {
			Node(sb, i, depth+2)
		}
	}
	// OFFSET (ClickHouse outputs offset before limit in EXPLAIN AST)
	if n.Offset != nil {
		Node(sb, n.Offset, depth+1)
	}
	// LIMIT BY handling
	if n.LimitByLimit != nil {
		// Case: LIMIT n BY x LIMIT m -> output LimitByLimit, LimitBy, Limit
		Node(sb, n.LimitByLimit, depth+1)
		if len(n.LimitBy) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.LimitBy))
			for _, expr := range n.LimitBy {
				Node(sb, expr, depth+2)
			}
		}
		if n.Limit != nil {
			Node(sb, n.Limit, depth+1)
		}
	} else if len(n.LimitBy) > 0 {
		// Case: LIMIT n BY x (no second LIMIT) -> output Limit, then LimitBy
		if n.Limit != nil {
			Node(sb, n.Limit, depth+1)
		}
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.LimitBy))
		for _, expr := range n.LimitBy {
			Node(sb, expr, depth+2)
		}
	} else if n.Limit != nil {
		// Case: plain LIMIT n (no BY)
		Node(sb, n.Limit, depth+1)
	}
	// SETTINGS is output at SelectQuery level only when NOT after FORMAT
	// When SettingsAfterFormat is true, it's output at SelectWithUnionQuery level instead
	if len(n.Settings) > 0 && !n.SettingsAfterFormat {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
	// TOP clause is output at the end
	if n.Top != nil {
		Node(sb, n.Top, depth+1)
	}
}

func explainOrderByElement(sb *strings.Builder, n *ast.OrderByElement, indent string, depth int) {
	// ClickHouse uses different formats for WITH FILL:
	// - When FROM/TO are simple literals: direct children on OrderByElement
	// - When FROM/TO are complex expressions or only STEP present: uses FillModifier wrapper
	hasFromOrTo := n.FillFrom != nil || n.FillTo != nil
	hasComplexFillExpr := hasFromOrTo && (isComplexExpr(n.FillFrom) || isComplexExpr(n.FillTo))

	// Use FillModifier when FROM/TO contain complex expressions (not simple literals)
	// When only STEP is present, output it directly as a child (no FillModifier)
	useFillModifier := n.WithFill && hasComplexFillExpr

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

// explainInterpolateElement explains an INTERPOLATE element.
// Format: InterpolateElement (column colname) (children N)
func explainInterpolateElement(sb *strings.Builder, n *ast.InterpolateElement, indent string, depth int) {
	children := 0
	if n.Value != nil {
		children = 1
	}

	if children > 0 {
		fmt.Fprintf(sb, "%sInterpolateElement (column %s) (children %d)\n", indent, n.Column, children)
		Node(sb, n.Value, depth+1)
	} else {
		fmt.Fprintf(sb, "%sInterpolateElement (column %s)\n", indent, n.Column)
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
	// Skip this when inside CreateQuery context, as Format is output at CreateQuery level
	if !inCreateQueryContext {
		for _, sel := range n.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
				count++
				break
			}
		}
	}
	// When SETTINGS comes AFTER FORMAT, it's counted at SelectWithUnionQuery level
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.SettingsAfterFormat && len(sq.Settings) > 0 {
			count++
			break
		}
	}
	return count
}

// simplifyUnionSelects returns all SELECT statements in a UNION.
// ClickHouse does not simplify UNION ALL queries in EXPLAIN AST output.
func simplifyUnionSelects(selects []ast.Statement) []ast.Statement {
	return selects
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
	if len(n.GroupBy) > 0 && !n.GroupByAll {
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
	if len(n.Interpolate) > 0 {
		count++
	}
	if n.LimitByLimit != nil {
		count++ // LIMIT n in "LIMIT n BY x LIMIT m"
	}
	if n.Limit != nil {
		count++
	}
	if len(n.LimitBy) > 0 {
		count++
	}
	if n.Offset != nil {
		count++
	}
	// SETTINGS is counted at SelectQuery level only when NOT after FORMAT
	if len(n.Settings) > 0 && !n.SettingsAfterFormat {
		count++
	}
	// TOP clause
	if n.Top != nil {
		count++
	}
	return count
}
