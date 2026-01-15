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
	// WINDOW clause - output before QUALIFY
	if len(sq.Window) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(sq.Window))
		for range sq.Window {
			fmt.Fprintf(sb, "%s  WindowListElement\n", indent)
		}
	}
	// QUALIFY
	if sq.Qualify != nil {
		Node(sb, sq.Qualify, depth+1)
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

	// Expand any nested SelectWithUnionQuery that would be grouped
	expandedSelects, expandedModes := expandNestedUnions(selects, n.UnionModes)

	// Check if we need to group selects due to mode changes
	groupedSelects := groupSelectsByUnionMode(expandedSelects, expandedModes)

	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(groupedSelects))
	for _, sel := range groupedSelects {
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

	// Expand any nested SelectWithUnionQuery that would be grouped
	// This flattens [S1, nested(5)] into [S1, grouped(4), S6] when grouping applies
	expandedSelects, expandedModes := expandNestedUnions(selects, n.UnionModes)

	// Check if we need to group selects due to mode changes
	// e.g., A UNION DISTINCT B UNION ALL C -> (A UNION DISTINCT B) UNION ALL C
	groupedSelects := groupSelectsByUnionMode(expandedSelects, expandedModes)

	// Wrap selects in ExpressionList
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(groupedSelects))

	// Check if first operand has a WITH clause to be inherited by subsequent operands
	var inheritedWith []ast.Expression
	if len(selects) > 0 {
		inheritedWith = extractWithClause(selects[0])
	}

	for i, sel := range groupedSelects {
		if i > 0 && len(inheritedWith) > 0 {
			// Subsequent operands inherit the WITH clause from the first operand
			explainSelectQueryWithInheritedWith(sb, sel, inheritedWith, depth+2)
		} else {
			Node(sb, sel, depth+2)
		}
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
					// Check if this tuple was from double parens ((a,b,c)) - marked as Parenthesized
					// In that case, output as Function tuple wrapped in ExpressionList(1)
					if lit.Parenthesized {
						if elements, ok := lit.Value.([]ast.Expression); ok {
							fmt.Fprintf(sb, "%s  ExpressionList (children 1)\n", indent)
							fmt.Fprintf(sb, "%s   Function tuple (children 1)\n", indent)
							if len(elements) > 0 {
								fmt.Fprintf(sb, "%s    ExpressionList (children %d)\n", indent, len(elements))
								for _, elem := range elements {
									Node(sb, elem, depth+5)
								}
							} else {
								fmt.Fprintf(sb, "%s    ExpressionList\n", indent)
							}
						}
					} else if elements, ok := lit.Value.([]ast.Expression); ok {
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
	// WINDOW clause (named window definitions) - output before QUALIFY
	if len(n.Window) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Window))
		for range n.Window {
			fmt.Fprintf(sb, "%s  WindowListElement\n", indent)
		}
	}
	// QUALIFY
	if n.Qualify != nil {
		Node(sb, n.Qualify, depth+1)
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

// expandNestedUnions expands nested SelectWithUnionQuery elements.
// - If a nested union has only ALL modes, it's completely flattened
// - If a nested union has a DISTINCT->ALL transition, it's expanded to grouped results
// For example, [S1, nested(S2,S3,S4,S5,S6)] with modes [ALL] where nested has modes [ALL,"",DISTINCT,ALL]
// becomes [S1, grouped(S2,S3,S4,S5), S6] with modes [ALL, ALL]
func expandNestedUnions(selects []ast.Statement, unionModes []string) ([]ast.Statement, []string) {
	result := make([]ast.Statement, 0, len(selects))
	resultModes := make([]string, 0, len(unionModes))

	// Helper to check if all modes are ALL
	allModesAreAll := func(modes []string) bool {
		for _, m := range modes {
			normalized := m
			if len(m) > 6 && m[:6] == "UNION " {
				normalized = m[6:]
			}
			if normalized != "ALL" && normalized != "" {
				// "" can be bare UNION which may default to DISTINCT
				// but we treat it as potentially non-ALL
				return false
			}
			// For "" (bare UNION), we check if it's truly all-ALL by also checking
			// that DISTINCT is not present
			if normalized == "" {
				return false // bare UNION may be DISTINCT based on settings
			}
		}
		return true
	}

	for i, sel := range selects {
		if nested, ok := sel.(*ast.SelectWithUnionQuery); ok {
			// Single select in parentheses - flatten it
			if len(nested.Selects) == 1 {
				result = append(result, nested.Selects[0])
				if i > 0 && i-1 < len(unionModes) {
					resultModes = append(resultModes, unionModes[i-1])
				}
				continue
			}
			// Check if all nested modes are ALL - if so, flatten completely
			if allModesAreAll(nested.UnionModes) {
				// Flatten completely: add outer mode first, then all nested selects and modes
				if i > 0 && i-1 < len(unionModes) {
					resultModes = append(resultModes, unionModes[i-1])
				}
				// Add first nested select
				if len(nested.Selects) > 0 {
					// Recursively expand in case of deeply nested unions
					expandedNested, expandedNestedModes := expandNestedUnions(nested.Selects, nested.UnionModes)
					for j, s := range expandedNested {
						result = append(result, s)
						if j < len(expandedNestedModes) {
							resultModes = append(resultModes, expandedNestedModes[j])
						}
					}
				}
			} else {
				// Check if this nested union would be grouped (DISTINCT->ALL transition)
				grouped := groupSelectsByUnionMode(nested.Selects, nested.UnionModes)
				if len(grouped) > 1 {
					// Grouping produced multiple elements - expand them
					// The outer mode (if any) applies to the first expanded element
					if i > 0 && i-1 < len(unionModes) {
						resultModes = append(resultModes, unionModes[i-1])
					}
					// Add all grouped elements and their modes
					for j, g := range grouped {
						result = append(result, g)
						if j < len(grouped)-1 {
							// Mode between grouped elements is ALL (from the transition point)
							resultModes = append(resultModes, "UNION ALL")
						}
					}
				} else {
					// No grouping, keep as-is
					result = append(result, sel)
					if i > 0 && i-1 < len(unionModes) {
						resultModes = append(resultModes, unionModes[i-1])
					}
				}
			}
		} else {
			result = append(result, sel)
			if i > 0 && i-1 < len(unionModes) {
				resultModes = append(resultModes, unionModes[i-1])
			}
		}
	}

	return result, resultModes
}

// groupSelectsByUnionMode groups selects when union modes change from DISTINCT to ALL.
// For example, A UNION DISTINCT B UNION ALL C becomes (A UNION DISTINCT B) UNION ALL C.
// This matches ClickHouse's EXPLAIN AST output which nests DISTINCT groups before ALL.
// Note: The reverse (ALL followed by DISTINCT) does NOT trigger nesting.
func groupSelectsByUnionMode(selects []ast.Statement, unionModes []string) []ast.Statement {
	if len(selects) < 3 || len(unionModes) < 2 {
		return selects
	}

	// Normalize union modes (strip "UNION " prefix if present)
	normalizeMode := func(mode string) string {
		if len(mode) > 6 && mode[:6] == "UNION " {
			return mode[6:]
		}
		return mode
	}

	// Find the last DISTINCT->ALL transition
	// A transition occurs when a non-ALL mode (DISTINCT or bare "") is followed by ALL
	modeChangeIdx := -1
	for i := 1; i < len(unionModes); i++ {
		prevMode := normalizeMode(unionModes[i-1])
		currMode := normalizeMode(unionModes[i])
		// Check for non-ALL -> ALL transition
		// Non-ALL means DISTINCT or "" (bare UNION, which defaults to DISTINCT)
		if currMode == "ALL" && prevMode != "ALL" {
			modeChangeIdx = i
			// Continue to find the LAST such transition
		}
	}

	// If no DISTINCT->ALL transition found, return as-is
	if modeChangeIdx == -1 {
		return selects
	}

	// Create a nested SelectWithUnionQuery for selects 0..modeChangeIdx (inclusive)
	// modeChangeIdx is the index of the union operator, so we include selects[0] through selects[modeChangeIdx]
	nestedSelects := selects[:modeChangeIdx+1]
	nestedModes := unionModes[:modeChangeIdx]

	nested := &ast.SelectWithUnionQuery{
		Selects:    nestedSelects,
		UnionModes: nestedModes,
	}

	// Result is [nested, selects[modeChangeIdx+1], ...]
	result := make([]ast.Statement, 0, len(selects)-modeChangeIdx)
	result = append(result, nested)
	result = append(result, selects[modeChangeIdx+1:]...)

	return result
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
