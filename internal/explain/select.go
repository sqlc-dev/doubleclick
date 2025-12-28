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
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
			Node(sb, sq.Format, depth+1)
			break
		}
	}
	// When SETTINGS comes AFTER FORMAT, it is ALSO output at SelectWithUnionQuery level
	// (in addition to being at SelectQuery level)
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
	// SETTINGS - output at SelectQuery level in these cases:
	// 1. SETTINGS is before FORMAT (not after)
	// 2. SETTINGS is after FORMAT AND there's a FROM clause
	// When SETTINGS is after FORMAT without FROM, it's only at SelectWithUnionQuery level
	if len(n.Settings) > 0 && (!n.SettingsAfterFormat || n.From != nil) {
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
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
			count++
			break
		}
	}
	// When SETTINGS comes AFTER FORMAT, it is ALSO counted at this level
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.SettingsAfterFormat && len(sq.Settings) > 0 {
			count++
			break
		}
	}
	return count
}

// simplifyUnionSelects implements ClickHouse's UNION ALL optimization:
// When all SELECT queries in a UNION have identical expressions (ignoring aliases)
// but different aliases, only the first SELECT is returned.
// This only applies when ALL columns in ALL SELECTs have explicit aliases.
// If aliases are the same across all SELECTs, or if any column lacks an alias, all are kept.
func simplifyUnionSelects(selects []ast.Statement) []ast.Statement {
	if len(selects) <= 1 {
		return selects
	}

	// Check if all are simple SelectQuery with only literal columns
	var queries []*ast.SelectQuery
	for _, sel := range selects {
		sq, ok := sel.(*ast.SelectQuery)
		if !ok {
			// Not a simple SelectQuery, can't simplify
			return selects
		}
		// Only handle simple SELECT with just columns, no FROM/WHERE/etc.
		if sq.From != nil || sq.Where != nil || sq.GroupBy != nil ||
			sq.Having != nil || sq.OrderBy != nil || len(sq.With) > 0 {
			return selects
		}
		queries = append(queries, sq)
	}

	// Check if all have the same number of columns
	numCols := len(queries[0].Columns)
	for _, q := range queries[1:] {
		if len(q.Columns) != numCols {
			return selects
		}
	}

	// Check if columns are all literals with aliases
	// and compare expressions (without aliases) and aliases separately
	allSameAliases := true
	allSameExprs := true
	allHaveAliases := true

	for colIdx := 0; colIdx < numCols; colIdx++ {
		firstAlias := ""
		firstExpr := ""

		for i, q := range queries {
			col := q.Columns[colIdx]
			alias := ""
			exprStr := ""
			hasAlias := false

			switch c := col.(type) {
			case *ast.AliasedExpr:
				alias = c.Alias
				hasAlias = c.Alias != ""
				// Get string representation of the expression
				if lit, ok := c.Expr.(*ast.Literal); ok {
					exprStr = fmt.Sprintf("%v", lit.Value)
				} else {
					// Non-literal expression, can't simplify
					return selects
				}
			case *ast.Literal:
				exprStr = fmt.Sprintf("%v", c.Value)
				hasAlias = false
			default:
				// Not a simple literal or aliased literal
				return selects
			}

			if !hasAlias {
				allHaveAliases = false
			}

			if i == 0 {
				firstAlias = alias
				firstExpr = exprStr
			} else {
				if alias != firstAlias {
					allSameAliases = false
				}
				if exprStr != firstExpr {
					allSameExprs = false
				}
			}
		}
	}

	// If expressions are the same, all have aliases, but aliases differ, return only first SELECT
	if allSameExprs && allHaveAliases && !allSameAliases {
		return selects[:1]
	}

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
	// SETTINGS is counted at SelectQuery level in these cases:
	// 1. SETTINGS is before FORMAT (not after)
	// 2. SETTINGS is after FORMAT AND there's a FROM clause
	// When SETTINGS is after FORMAT without FROM, it's only at SelectWithUnionQuery level
	if len(n.Settings) > 0 && (!n.SettingsAfterFormat || n.From != nil) {
		count++
	}
	return count
}
