package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

func explainTablesInSelectQuery(sb *strings.Builder, n *ast.TablesInSelectQuery, indent string, depth int) {
	fmt.Fprintf(sb, "%sTablesInSelectQuery (children %d)\n", indent, len(n.Tables))
	for _, t := range n.Tables {
		Node(sb, t, depth+1)
	}
}

func explainTablesInSelectQueryElement(sb *strings.Builder, n *ast.TablesInSelectQueryElement, indent string, depth int) {
	children := 1 // table
	if n.Join != nil {
		children++
	}
	fmt.Fprintf(sb, "%sTablesInSelectQueryElement (children %d)\n", indent, children)
	if n.Table != nil {
		Node(sb, n.Table, depth+1)
	}
	if n.Join != nil {
		Node(sb, n.Join, depth+1)
	}
}

func explainTableExpression(sb *strings.Builder, n *ast.TableExpression, indent string, depth int) {
	children := 1 // table
	if n.Sample != nil {
		children++
	}
	fmt.Fprintf(sb, "%sTableExpression (children %d)\n", indent, children)
	// If there's a subquery with an alias, pass the alias to the subquery output
	if subq, ok := n.Table.(*ast.Subquery); ok {
		// Check if subquery contains an EXPLAIN query - convert to viewExplain function
		if explainQ, ok := subq.Query.(*ast.ExplainQuery); ok {
			explainViewExplain(sb, explainQ, n.Alias, indent+" ", depth+1)
		} else if n.Alias != "" {
			fmt.Fprintf(sb, "%s Subquery (alias %s) (children %d)\n", indent, n.Alias, 1)
			// Set context flag for subquery - affects how negated literals with aliases are formatted
			prevContext := inSubqueryContext
			inSubqueryContext = true
			Node(sb, subq.Query, depth+2)
			inSubqueryContext = prevContext
		} else {
			Node(sb, n.Table, depth+1)
		}
	} else if fn, ok := n.Table.(*ast.FunctionCall); ok && n.Alias != "" {
		// Table function with alias
		explainFunctionCallWithAlias(sb, fn, n.Alias, indent+" ", depth+1)
	} else if ti, ok := n.Table.(*ast.TableIdentifier); ok && n.Alias != "" {
		// Table identifier with alias
		explainTableIdentifierWithAlias(sb, ti, n.Alias, indent+" ")
	} else {
		Node(sb, n.Table, depth+1)
	}
	// Output SAMPLE clause if present
	if n.Sample != nil {
		explainSampleClause(sb, n.Sample, indent+" ", depth+1)
	}
}

func explainSampleClause(sb *strings.Builder, n *ast.SampleClause, indent string, depth int) {
	// Format the sample ratio as "SampleRatio num / den" or just the expression
	sb.WriteString(indent)
	sb.WriteString("SampleRatio ")
	formatSampleRatio(sb, n.Ratio)
	sb.WriteString("\n")
}

func formatSampleRatio(sb *strings.Builder, expr ast.Expression) {
	// Handle binary expressions like 1 / 2
	if binExpr, ok := expr.(*ast.BinaryExpr); ok && binExpr.Op == "/" {
		formatSampleRatioOperand(sb, binExpr.Left)
		sb.WriteString(" / ")
		formatSampleRatioOperand(sb, binExpr.Right)
	} else {
		formatSampleRatioOperand(sb, expr)
	}
}

func formatSampleRatioOperand(sb *strings.Builder, expr ast.Expression) {
	if lit, ok := expr.(*ast.Literal); ok {
		switch v := lit.Value.(type) {
		case int64:
			fmt.Fprintf(sb, "%d", v)
		case uint64:
			fmt.Fprintf(sb, "%d", v)
		case float64:
			// Convert decimal to fraction for EXPLAIN AST output
			// ClickHouse shows 0.1 as "1 / 10", 0.01 as "1 / 100", etc.
			if frac := floatToFraction(v); frac != "" {
				sb.WriteString(frac)
			} else {
				fmt.Fprintf(sb, "%g", v)
			}
		default:
			fmt.Fprintf(sb, "%v", v)
		}
	} else {
		fmt.Fprintf(sb, "%v", expr)
	}
}

// floatToFraction converts a float to a fraction string like "1 / 10"
// Returns empty string if the float can't be reasonably converted to a simple fraction
func floatToFraction(f float64) string {
	if f <= 0 || f >= 1 {
		return ""
	}
	// Try common denominators
	denominators := []int64{2, 3, 4, 5, 8, 10, 16, 20, 25, 32, 50, 64, 100, 128, 1000, 10000, 100000, 1000000}
	for _, denom := range denominators {
		num := f * float64(denom)
		// Check if num is close to an integer
		rounded := int64(num + 0.5)
		if rounded > 0 && abs(num-float64(rounded)) < 1e-9 {
			return fmt.Sprintf("%d / %d", rounded, denom)
		}
	}
	return ""
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// explainViewExplain handles EXPLAIN queries used as table sources, converting to viewExplain function
// ClickHouse internally transforms EXPLAIN to SELECT * FROM viewExplain(...)
func explainViewExplain(sb *strings.Builder, n *ast.ExplainQuery, alias string, indent string, depth int) {
	// When EXPLAIN is used as a table source, it becomes wrapped in SELECT * FROM viewExplain(...)
	// Structure: Subquery -> SelectWithUnionQuery -> ExpressionList -> SelectQuery -> Asterisk, TablesInSelectQuery -> viewExplain
	fmt.Fprintf(sb, "%sSubquery (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s SelectWithUnionQuery (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s   SelectQuery (children %d)\n", indent, 2)
	fmt.Fprintf(sb, "%s    ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s     Asterisk\n", indent)
	fmt.Fprintf(sb, "%s    TablesInSelectQuery (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s     TablesInSelectQueryElement (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s      TableExpression (children %d)\n", indent, 1)
	// Now output the viewExplain function
	fmt.Fprintf(sb, "%s       Function viewExplain (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s        ExpressionList (children %d)\n", indent, 3)
	// First argument: 'EXPLAIN' or 'EXPLAIN SYNTAX' etc.
	// PLAN is the default and never shown; only show non-default types like SYNTAX
	explainTypeStr := "EXPLAIN"
	if n.ExplicitType && n.ExplainType != "" && n.ExplainType != ast.ExplainAST && n.ExplainType != ast.ExplainPlan {
		explainTypeStr = "EXPLAIN " + string(n.ExplainType)
	}
	fmt.Fprintf(sb, "%s         Literal \\'%s\\'\n", indent, explainTypeStr)
	// Second argument: options string (e.g., "actions = 1")
	options := n.OptionsString
	fmt.Fprintf(sb, "%s         Literal \\'%s\\'\n", indent, options)
	// Third argument: the subquery being explained
	fmt.Fprintf(sb, "%s         Subquery (children %d)\n", indent, 1)
	Node(sb, n.Statement, depth+10)
}

func explainTableIdentifierWithAlias(sb *strings.Builder, n *ast.TableIdentifier, alias string, indent string) {
	name := n.Table
	if n.Database != "" {
		name = n.Database + "." + n.Table
	}
	fmt.Fprintf(sb, "%sTableIdentifier %s (alias %s)\n", indent, name, alias)
}

func explainTableIdentifier(sb *strings.Builder, n *ast.TableIdentifier, indent string) {
	name := n.Table
	if n.Database != "" {
		name = n.Database + "." + n.Table
	}
	fmt.Fprintf(sb, "%sTableIdentifier %s\n", indent, name)
}

func explainArrayJoinClause(sb *strings.Builder, n *ast.ArrayJoinClause, indent string, depth int) {
	fmt.Fprintf(sb, "%sArrayJoin (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList", indent)
	if len(n.Columns) > 0 {
		fmt.Fprintf(sb, " (children %d)", len(n.Columns))
	}
	fmt.Fprintln(sb)
	for _, col := range n.Columns {
		Node(sb, col, depth+2)
	}
}

func explainTableJoin(sb *strings.Builder, n *ast.TableJoin, indent string, depth int) {
	// TableJoin is part of TablesInSelectQueryElement
	// ClickHouse EXPLAIN AST doesn't show join type in the output
	children := 0
	if n.On != nil {
		children++
	}
	if len(n.Using) > 0 {
		children++
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sTableJoin (children %d)\n", indent, children)
	} else {
		fmt.Fprintf(sb, "%sTableJoin\n", indent)
	}
	if n.On != nil {
		Node(sb, n.On, depth+1)
	}
	if len(n.Using) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Using))
		for _, u := range n.Using {
			Node(sb, u, depth+2)
		}
	}
}
