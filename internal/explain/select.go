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
		if strings.HasPrefix(op, "EXCEPT") {
			hasExcept = true
			break
		}
	}

	// Check if first operand has a WITH clause to be inherited by subsequent operands
	var inheritedWith []ast.Expression
	if len(n.Selects) > 0 {
		inheritedWith = extractWithClause(n.Selects[0])
	}

	childIndent := strings.Repeat(" ", depth+1)
	for i, sel := range n.Selects {
		if hasExcept && i == 0 {
			// Wrap first operand in SelectWithUnionQuery -> ExpressionList format
			// But if it's already a SelectWithUnionQuery, don't double-wrap
			if _, isUnion := sel.(*ast.SelectWithUnionQuery); isUnion {
				Node(sb, sel, depth+1)
			} else {
				fmt.Fprintf(sb, "%sSelectWithUnionQuery (children 1)\n", childIndent)
				fmt.Fprintf(sb, "%s ExpressionList (children 1)\n", childIndent)
				Node(sb, sel, depth+3)
			}
		} else if i > 0 && len(inheritedWith) > 0 {
			// Subsequent operands inherit the WITH clause from the first operand
			explainSelectQueryWithInheritedWith(sb, sel, inheritedWith, depth+1)
		} else {
			Node(sb, sel, depth+1)
		}
	}
}

// extractWithClause extracts the WITH clause from a statement (if it's a SelectQuery)
func extractWithClause(stmt ast.Statement) []ast.Expression {
	switch s := stmt.(type) {
	case *ast.SelectQuery:
		return s.With
	case *ast.SelectWithUnionQuery:
		// Check the first select in the union
		if len(s.Selects) > 0 {
			return extractWithClause(s.Selects[0])
		}
	}
	return nil
}

// explainSelectQueryWithInheritedWith outputs a SELECT with an inherited WITH clause
// The inherited WITH clause is output at the END of children (after columns and tables)
func explainSelectQueryWithInheritedWith(sb *strings.Builder, stmt ast.Statement, inheritedWith []ast.Expression, depth int) {
	sq, ok := stmt.(*ast.SelectQuery)
	if !ok {
		// Not a SelectQuery, output normally
		Node(sb, stmt, depth)
		return
	}

	// If the SelectQuery already has a WITH clause, output normally
	if len(sq.With) > 0 {
		Node(sb, stmt, depth)
		return
	}

	// Output SelectQuery with inherited WITH clause at the end
	indent := strings.Repeat(" ", depth)
	children := countSelectQueryChildren(sq) + 1 // +1 for inherited WITH clause
	fmt.Fprintf(sb, "%sSelectQuery (children %d)\n", indent, children)

	// Columns (ExpressionList) - output first
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.Columns))
	for _, col := range sq.Columns {
		Node(sb, col, depth+2)
	}

	// FROM (including ARRAY JOIN as part of TablesInSelectQuery)
	if sq.From != nil || sq.ArrayJoin != nil {
		TablesWithArrayJoin(sb, sq.From, sq.ArrayJoin, depth+1)
	}
	// PREWHERE
	if sq.PreWhere != nil {
		Node(sb, sq.PreWhere, depth+1)
	}
	// WHERE
	if sq.Where != nil {
		Node(sb, sq.Where, depth+1)
	}
	// GROUP BY (skip for GROUP BY ALL which doesn't output an expression list)
	if len(sq.GroupBy) > 0 && !sq.GroupByAll {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.GroupBy))
		for _, g := range sq.GroupBy {
			Node(sb, g, depth+2)
		}
	}
	// HAVING
	if sq.Having != nil {
		Node(sb, sq.Having, depth+1)
	}
	// QUALIFY
	if sq.Qualify != nil {
		Node(sb, sq.Qualify, depth+1)
	}
	// WINDOW clause
	if len(sq.Window) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.Window))
		for range sq.Window {
			fmt.Fprintf(sb, "%s  WindowListElement\n", indent)
		}
	}
	// ORDER BY
	if len(sq.OrderBy) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.OrderBy))
		for _, o := range sq.OrderBy {
			Node(sb, o, depth+2)
		}
	}
	// SETTINGS (when INTERPOLATE is present, SETTINGS comes before INTERPOLATE)
	if len(sq.Settings) > 0 && len(sq.Interpolate) > 0 && !sq.SettingsAfterFormat {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
	// INTERPOLATE
	if len(sq.Interpolate) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.Interpolate))
		for _, i := range sq.Interpolate {
			Node(sb, i, depth+2)
		}
	}
	// LIMIT BY handling - order: LimitByOffset, LimitByLimit, LimitBy expressions, Offset, Limit
	if sq.LimitByLimit != nil {
		// Output LIMIT BY offset first (if present)
		if sq.LimitByOffset != nil {
			Node(sb, sq.LimitByOffset, depth+1)
		}
		// Output LIMIT BY count
		Node(sb, sq.LimitByLimit, depth+1)
		// Output LIMIT BY expressions
		if len(sq.LimitBy) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.LimitBy))
			for _, expr := range sq.LimitBy {
				Node(sb, expr, depth+2)
			}
		}
		// Output regular OFFSET
		if sq.Offset != nil {
			Node(sb, sq.Offset, depth+1)
		}
		// Output regular LIMIT
		if sq.Limit != nil {
			Node(sb, sq.Limit, depth+1)
		}
	} else if len(sq.LimitBy) > 0 {
		// LIMIT BY without explicit LimitByLimit
		if sq.Limit != nil {
			Node(sb, sq.Limit, depth+1)
		}
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.LimitBy))
		for _, expr := range sq.LimitBy {
			Node(sb, expr, depth+2)
		}
	} else {
		// No LIMIT BY - just regular OFFSET and LIMIT
		if sq.Offset != nil {
			Node(sb, sq.Offset, depth+1)
		}
		if sq.Limit != nil {
			Node(sb, sq.Limit, depth+1)
		}
	}
	// SETTINGS (when no INTERPOLATE - the case with INTERPOLATE is handled above)
	if len(sq.Settings) > 0 && len(sq.Interpolate) == 0 && !sq.SettingsAfterFormat {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
	// TOP clause
	if sq.Top != nil {
		Node(sb, sq.Top, depth+1)
	}

	// Inherited WITH clause (ExpressionList) - output at the END
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(inheritedWith))
	for _, w := range inheritedWith {
		Node(sb, w, depth+2)
	}
}

// ExplainSelectWithInheritedWith recursively explains a select statement with inherited WITH clause
// This is used for WITH ... INSERT ... SELECT where the WITH clause belongs to the INSERT
// but needs to be output at the end of each SelectQuery in the tree
func ExplainSelectWithInheritedWith(sb *strings.Builder, stmt ast.Statement, inheritedWith []ast.Expression, depth int) {
	switch s := stmt.(type) {
	case *ast.SelectWithUnionQuery:
		explainSelectWithUnionQueryWithInheritedWith(sb, s, inheritedWith, depth)
	case *ast.SelectIntersectExceptQuery:
		explainSelectIntersectExceptQueryWithInheritedWith(sb, s, inheritedWith, depth)
	case *ast.SelectQuery:
		explainSelectQueryWithInheritedWith(sb, s, inheritedWith, depth)
	default:
		Node(sb, stmt, depth)
	}
}

// explainSelectWithUnionQueryWithInheritedWith explains a SelectWithUnionQuery with inherited WITH
func explainSelectWithUnionQueryWithInheritedWith(sb *strings.Builder, n *ast.SelectWithUnionQuery, inheritedWith []ast.Expression, depth int) {
	if n == nil {
		return
	}
	indent := strings.Repeat(" ", depth)
	children := countSelectUnionChildren(n)
	fmt.Fprintf(sb, "%sSelectWithUnionQuery (children %d)\n", indent, children)

	selects := simplifyUnionSelects(n.Selects)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(selects))
	for _, sel := range selects {
		ExplainSelectWithInheritedWith(sb, sel, inheritedWith, depth+2)
	}

	// INTO OUTFILE clause
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.IntoOutfile != nil {
			fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, sq.IntoOutfile.Filename)
			break
		}
	}
	// SETTINGS before FORMAT
	if n.SettingsBeforeFormat && len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
	// FORMAT clause - check individual SelectQuery nodes
	for _, sel := range n.Selects {
		if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
			Node(sb, sq.Format, depth+1)
			break
		}
	}
	// SETTINGS after FORMAT
	if n.SettingsAfterFormat && len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
	} else {
		for _, sel := range n.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok && sq.SettingsAfterFormat && len(sq.Settings) > 0 {
				fmt.Fprintf(sb, "%s Set\n", indent)
				break
			}
		}
	}
}

// explainSelectIntersectExceptQueryWithInheritedWith explains a SelectIntersectExceptQuery with inherited WITH
func explainSelectIntersectExceptQueryWithInheritedWith(sb *strings.Builder, n *ast.SelectIntersectExceptQuery, inheritedWith []ast.Expression, depth int) {
	indent := strings.Repeat(" ", depth)
	fmt.Fprintf(sb, "%sSelectIntersectExceptQuery (children %d)\n", indent, len(n.Selects))

	// Check if EXCEPT is present - affects how first operand is wrapped
	hasExcept := false
	for _, op := range n.Operators {
		if strings.HasPrefix(op, "EXCEPT") {
			hasExcept = true
			break
		}
	}

	for i, sel := range n.Selects {
		if hasExcept && i == 0 {
			// Wrap first operand in SelectWithUnionQuery format
			if _, isUnion := sel.(*ast.SelectWithUnionQuery); isUnion {
				ExplainSelectWithInheritedWith(sb, sel, inheritedWith, depth+1)
			} else {
				childIndent := strings.Repeat(" ", depth+1)
				fmt.Fprintf(sb, "%sSelectWithUnionQuery (children 1)\n", childIndent)
				fmt.Fprintf(sb, "%s ExpressionList (children 1)\n", childIndent)
				ExplainSelectWithInheritedWith(sb, sel, inheritedWith, depth+3)
			}
		} else {
			ExplainSelectWithInheritedWith(sb, sel, inheritedWith, depth+1)
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
	// When SETTINGS comes BEFORE FORMAT, output Set first
	if n.SettingsBeforeFormat && len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
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
	// When SETTINGS comes AFTER FORMAT, output Set last (check SelectWithUnionQuery first, then SelectQuery)
	if n.SettingsAfterFormat && len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
	} else {
		// Legacy check for settings on SelectQuery
		for _, sel := range n.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok && sq.SettingsAfterFormat && len(sq.Settings) > 0 {
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
	// GROUP BY (skip for GROUP BY ALL which doesn't output an expression list)
	if len(n.GroupBy) > 0 && !n.GroupByAll {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.GroupBy))
		for _, g := range n.GroupBy {
			if n.GroupingSets {
				// Each grouping set is wrapped in an ExpressionList
				// but we need to unwrap tuples and output elements directly
				if lit, ok := g.(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
					if elements, ok := lit.Value.([]ast.Expression); ok {
						if len(elements) == 0 {
							// Empty grouping set () outputs ExpressionList without children count
							fmt.Fprintf(sb, "%s  ExpressionList\n", indent)
						} else {
							fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(elements))
							for _, elem := range elements {
								Node(sb, elem, depth+3)
							}
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
	// SETTINGS (when INTERPOLATE is present, SETTINGS comes before INTERPOLATE)
	if len(n.Settings) > 0 && len(n.Interpolate) > 0 && !n.SettingsAfterFormat {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
	// INTERPOLATE
	if len(n.Interpolate) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Interpolate))
		for _, i := range n.Interpolate {
			Node(sb, i, depth+2)
		}
	}
	// LIMIT BY handling - order: LimitByOffset, LimitByLimit, LimitBy expressions, Offset, Limit
	if n.LimitByLimit != nil {
		// Output LIMIT BY offset first (if present)
		if n.LimitByOffset != nil {
			Node(sb, n.LimitByOffset, depth+1)
		}
		// Output LIMIT BY count
		Node(sb, n.LimitByLimit, depth+1)
		// Output LIMIT BY expressions
		if len(n.LimitBy) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.LimitBy))
			for _, expr := range n.LimitBy {
				Node(sb, expr, depth+2)
			}
		}
		// Output regular OFFSET
		if n.Offset != nil {
			Node(sb, n.Offset, depth+1)
		}
		// Output regular LIMIT
		if n.Limit != nil {
			Node(sb, n.Limit, depth+1)
		}
	} else if len(n.LimitBy) > 0 {
		// LIMIT BY without explicit LimitByLimit
		if n.Limit != nil {
			Node(sb, n.Limit, depth+1)
		}
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.LimitBy))
		for _, expr := range n.LimitBy {
			Node(sb, expr, depth+2)
		}
	} else {
		// No LIMIT BY - just regular OFFSET and LIMIT
		if n.Offset != nil {
			Node(sb, n.Offset, depth+1)
		}
		if n.Limit != nil {
			Node(sb, n.Limit, depth+1)
		}
	}
	// SETTINGS is output at SelectQuery level only when NOT after FORMAT
	// When SettingsAfterFormat is true, it's output at SelectWithUnionQuery level instead
	// When INTERPOLATE is present, SETTINGS was already output above
	if len(n.Settings) > 0 && len(n.Interpolate) == 0 && !n.SettingsAfterFormat {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
	// TOP clause is output at the end
	if n.Top != nil {
		Node(sb, n.Top, depth+1)
	}
	// DISTINCT ON columns
	if len(n.DistinctOn) > 0 {
		fmt.Fprintf(sb, "%s Literal UInt64_1\n", indent)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.DistinctOn))
		for _, col := range n.DistinctOn {
			Node(sb, col, depth+2)
		}
	}
}

func explainOrderByElement(sb *strings.Builder, n *ast.OrderByElement, indent string, depth int) {
	// All fill-related children are direct children of OrderByElement
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
	if n.FillStaleness != nil {
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
	if n.FillStaleness != nil {
		Node(sb, n.FillStaleness, depth+1)
	}
	if n.Collate != "" {
		// COLLATE is output as a string literal
		fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, n.Collate)
	}
}

// explainInterpolateElement explains an INTERPOLATE element.
// Format: InterpolateElement (column colname) (children N)
// When there's a value expression: output the value as the child
// When there's no value: output the column identifier as the child
func explainInterpolateElement(sb *strings.Builder, n *ast.InterpolateElement, indent string, depth int) {
	fmt.Fprintf(sb, "%sInterpolateElement (column %s) (children %d)\n", indent, n.Column, 1)
	if n.Value != nil {
		// Output value expression as the child
		Node(sb, n.Value, depth+1)
	} else {
		// Output column name as Identifier when no explicit value
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Column)
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
	// Count union-level SETTINGS (either before or after FORMAT)
	if len(n.Settings) > 0 && (n.SettingsBeforeFormat || n.SettingsAfterFormat) {
		count++
	} else {
		// Legacy check for settings on SelectQuery
		for _, sel := range n.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok && sq.SettingsAfterFormat && len(sq.Settings) > 0 {
				count++
				break
			}
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
	if n.LimitByOffset != nil {
		count++ // LIMIT offset in "LIMIT offset, count BY x"
	}
	if n.LimitByLimit != nil {
		count++ // LIMIT count in "LIMIT n BY x LIMIT m"
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
	// DISTINCT ON columns (counts as 2: Literal UInt64_1 + ExpressionList)
	if len(n.DistinctOn) > 0 {
		count += 2
	}
	return count
}
