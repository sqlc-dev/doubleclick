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
	} else if lit, ok := expr.(*ast.Literal); ok && lit.Type == ast.LiteralFloat {
		// Convert float to fraction if it's a simple ratio
		if v, ok := lit.Value.(float64); ok {
			num, den := floatToFraction(v)
			if den > 1 {
				fmt.Fprintf(sb, "%d / %d", num, den)
				return
			}
		}
		formatSampleRatioOperand(sb, expr)
	} else {
		formatSampleRatioOperand(sb, expr)
	}
}

// floatToFraction converts a float to a simple fraction (numerator, denominator).
// Returns (num, 1) if no simple fraction representation is found.
func floatToFraction(f float64) (int64, int64) {
	// Handle common sample ratios
	// Try denominators from 2 to 1000
	for den := int64(2); den <= 1000; den++ {
		num := int64(f * float64(den))
		// Check if this gives us back the original value (within floating point tolerance)
		if float64(num)/float64(den) == f {
			// Find GCD to simplify the fraction
			g := gcd(num, den)
			return num / g, den / g
		}
	}
	// No simple fraction found, return as integer if possible
	if f == float64(int64(f)) {
		return int64(f), 1
	}
	return 0, 1
}

// gcd calculates the greatest common divisor of two integers
func gcd(a, b int64) int64 {
	if a < 0 {
		a = -a
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func formatSampleRatioOperand(sb *strings.Builder, expr ast.Expression) {
	if lit, ok := expr.(*ast.Literal); ok {
		switch v := lit.Value.(type) {
		case int64:
			fmt.Fprintf(sb, "%d", v)
		case uint64:
			fmt.Fprintf(sb, "%d", v)
		case float64:
			fmt.Fprintf(sb, "%g", v)
		default:
			fmt.Fprintf(sb, "%v", v)
		}
	} else {
		fmt.Fprintf(sb, "%v", expr)
	}
}

// explainViewExplain handles EXPLAIN queries used as table sources, converting to viewExplain function
func explainViewExplain(sb *strings.Builder, n *ast.ExplainQuery, alias string, indent string, depth int) {
	// When EXPLAIN is used as a table source, it becomes viewExplain function
	// Arguments: 'EXPLAIN', 'options', subquery
	fmt.Fprintf(sb, "%sFunction viewExplain (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 3)
	// First argument: 'EXPLAIN' literal
	fmt.Fprintf(sb, "%s  Literal \\'EXPLAIN\\'\n", indent)
	// Second argument: options string (empty for now since we don't track detailed options)
	options := string(n.ExplainType)
	if options == "PLAN" {
		options = ""
	}
	fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, options)
	// Third argument: the subquery being explained
	fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
	Node(sb, n.Statement, depth+3)
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
