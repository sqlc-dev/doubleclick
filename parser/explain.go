package parser

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

// Explain returns the EXPLAIN AST output for a statement, matching ClickHouse's format.
func Explain(stmt ast.Statement) string {
	var sb strings.Builder
	explainNode(&sb, stmt, 0)
	return sb.String()
}

// explainNode writes the EXPLAIN AST output for an AST node.
func explainNode(sb *strings.Builder, node interface{}, depth int) {
	if node == nil {
		return
	}

	indent := strings.Repeat(" ", depth)

	switch n := node.(type) {
	case *ast.SelectWithUnionQuery:
		children := countChildren(n)
		fmt.Fprintf(sb, "%sSelectWithUnionQuery (children %d)\n", indent, children)
		// Wrap selects in ExpressionList
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Selects))
		for _, sel := range n.Selects {
			explainNode(sb, sel, depth+2)
		}

	case *ast.SelectQuery:
		children := countSelectQueryChildren(n)
		fmt.Fprintf(sb, "%sSelectQuery (children %d)\n", indent, children)
		// Columns (ExpressionList)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Columns))
		for _, col := range n.Columns {
			explainNode(sb, col, depth+2)
		}
		// FROM
		if n.From != nil {
			explainNode(sb, n.From, depth+1)
		}
		// ARRAY JOIN
		if n.ArrayJoin != nil {
			explainNode(sb, n.ArrayJoin, depth+1)
		}
		// PREWHERE
		if n.PreWhere != nil {
			explainNode(sb, n.PreWhere, depth+1)
		}
		// WHERE
		if n.Where != nil {
			explainNode(sb, n.Where, depth+1)
		}
		// GROUP BY
		if len(n.GroupBy) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.GroupBy))
			for _, g := range n.GroupBy {
				explainNode(sb, g, depth+2)
			}
		}
		// HAVING
		if n.Having != nil {
			explainNode(sb, n.Having, depth+1)
		}
		// ORDER BY
		if len(n.OrderBy) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.OrderBy))
			for _, o := range n.OrderBy {
				explainNode(sb, o, depth+2)
			}
		}
		// LIMIT
		if n.Limit != nil {
			explainNode(sb, n.Limit, depth+1)
		}
		// OFFSET
		if n.Offset != nil {
			explainNode(sb, n.Offset, depth+1)
		}

	case *ast.TablesInSelectQuery:
		fmt.Fprintf(sb, "%sTablesInSelectQuery (children %d)\n", indent, len(n.Tables))
		for _, t := range n.Tables {
			explainNode(sb, t, depth+1)
		}

	case *ast.TablesInSelectQueryElement:
		children := 1 // table
		if n.Join != nil {
			children++
		}
		fmt.Fprintf(sb, "%sTablesInSelectQueryElement (children %d)\n", indent, children)
		if n.Table != nil {
			explainNode(sb, n.Table, depth+1)
		}
		if n.Join != nil {
			explainNode(sb, n.Join, depth+1)
		}

	case *ast.TableExpression:
		children := 1 // table
		if n.Alias != "" {
			children++
		}
		fmt.Fprintf(sb, "%sTableExpression (children %d)\n", indent, children)
		explainNode(sb, n.Table, depth+1)

	case *ast.TableIdentifier:
		name := n.Table
		if n.Database != "" {
			name = n.Database + "." + n.Table
		}
		fmt.Fprintf(sb, "%sTableIdentifier %s\n", indent, name)

	case *ast.ArrayJoinClause:
		fmt.Fprintf(sb, "%sArrayJoin (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList", indent)
		if len(n.Columns) > 0 {
			fmt.Fprintf(sb, " (children %d)", len(n.Columns))
		}
		fmt.Fprintln(sb)
		for _, col := range n.Columns {
			explainNode(sb, col, depth+2)
		}

	case *ast.OrderByElement:
		fmt.Fprintf(sb, "%sOrderByElement (children %d)\n", indent, 1)
		explainNode(sb, n.Expression, depth+1)

	case *ast.Identifier:
		name := n.Name()
		if n.Alias != "" {
			fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, name, n.Alias)
		} else {
			fmt.Fprintf(sb, "%sIdentifier %s\n", indent, name)
		}

	case *ast.Literal:
		fmt.Fprintf(sb, "%sLiteral %s\n", indent, formatLiteral(n))

	case *ast.FunctionCall:
		children := 1 // arguments ExpressionList
		if len(n.Parameters) > 0 {
			children++ // parameters ExpressionList
		}
		if n.Alias != "" {
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, n.Name, n.Alias, children)
		} else {
			fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, n.Name, children)
		}
		// Arguments
		fmt.Fprintf(sb, "%s ExpressionList", indent)
		if len(n.Arguments) > 0 {
			fmt.Fprintf(sb, " (children %d)", len(n.Arguments))
		}
		fmt.Fprintln(sb)
		for _, arg := range n.Arguments {
			explainNode(sb, arg, depth+2)
		}
		// Parameters (for parametric functions)
		if len(n.Parameters) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Parameters))
			for _, p := range n.Parameters {
				explainNode(sb, p, depth+2)
			}
		}

	case *ast.BinaryExpr:
		// Convert operator to function name
		fnName := operatorToFunction(n.Op)
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		explainNode(sb, n.Left, depth+2)
		explainNode(sb, n.Right, depth+2)

	case *ast.UnaryExpr:
		fnName := unaryOperatorToFunction(n.Op)
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		explainNode(sb, n.Operand, depth+2)

	case *ast.Subquery:
		children := 1
		fmt.Fprintf(sb, "%sSubquery (children %d)\n", indent, children)
		explainNode(sb, n.Query, depth+1)

	case *ast.AliasedExpr:
		explainAliasedExpr(sb, n, depth)

	case *ast.Lambda:
		// Lambda is represented as Function lambda with tuple of params and body
		fmt.Fprintf(sb, "%sFunction lambda (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		// Parameters as tuple
		fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.Parameters))
		for _, p := range n.Parameters {
			fmt.Fprintf(sb, "%s    Identifier %s\n", indent, p)
		}
		// Body
		explainNode(sb, n.Body, depth+2)

	case *ast.SetQuery:
		fmt.Fprintf(sb, "%sSet\n", indent)

	default:
		// For unhandled types, just print the type name
		fmt.Fprintf(sb, "%s%T\n", indent, node)
	}
}

// countChildren counts the children of a SelectWithUnionQuery
func countChildren(n *ast.SelectWithUnionQuery) int {
	return 1 // ExpressionList of selects
}

// countSelectQueryChildren counts the children of a SelectQuery
func countSelectQueryChildren(n *ast.SelectQuery) int {
	count := 1 // columns ExpressionList
	if n.From != nil {
		count++
	}
	if n.ArrayJoin != nil {
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
	return count
}

// formatLiteral formats a literal value for EXPLAIN AST output
func formatLiteral(lit *ast.Literal) string {
	switch lit.Type {
	case ast.LiteralInteger:
		val := lit.Value.(int64)
		if val >= 0 {
			return fmt.Sprintf("UInt64_%d", val)
		}
		return fmt.Sprintf("Int64_%d", val)
	case ast.LiteralFloat:
		val := lit.Value.(float64)
		return fmt.Sprintf("Float64_%v", val)
	case ast.LiteralString:
		s := lit.Value.(string)
		return fmt.Sprintf("\\'%s\\'", s)
	case ast.LiteralBoolean:
		if lit.Value.(bool) {
			return "UInt8_1"
		}
		return "UInt8_0"
	case ast.LiteralNull:
		return "Null"
	case ast.LiteralArray:
		return formatArrayLiteral(lit.Value)
	case ast.LiteralTuple:
		return formatTupleLiteral(lit.Value)
	default:
		return fmt.Sprintf("%v", lit.Value)
	}
}

// formatArrayLiteral formats an array literal for EXPLAIN AST output
func formatArrayLiteral(val interface{}) string {
	exprs, ok := val.([]ast.Expression)
	if !ok {
		return "Array_[]"
	}
	var parts []string
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok {
			parts = append(parts, formatLiteral(lit))
		} else if ident, ok := e.(*ast.Identifier); ok {
			parts = append(parts, ident.Name())
		} else {
			parts = append(parts, fmt.Sprintf("%v", e))
		}
	}
	return fmt.Sprintf("Array_[%s]", strings.Join(parts, ", "))
}

// formatTupleLiteral formats a tuple literal for EXPLAIN AST output
func formatTupleLiteral(val interface{}) string {
	exprs, ok := val.([]ast.Expression)
	if !ok {
		return "Tuple_()"
	}
	var parts []string
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok {
			parts = append(parts, formatLiteral(lit))
		} else if ident, ok := e.(*ast.Identifier); ok {
			parts = append(parts, ident.Name())
		} else {
			parts = append(parts, fmt.Sprintf("%v", e))
		}
	}
	return fmt.Sprintf("Tuple_(%s)", strings.Join(parts, ", "))
}

// operatorToFunction maps binary operators to ClickHouse function names
func operatorToFunction(op string) string {
	switch op {
	case "+":
		return "plus"
	case "-":
		return "minus"
	case "*":
		return "multiply"
	case "/":
		return "divide"
	case "%":
		return "modulo"
	case "=", "==":
		return "equals"
	case "!=", "<>":
		return "notEquals"
	case "<":
		return "less"
	case ">":
		return "greater"
	case "<=":
		return "lessOrEquals"
	case ">=":
		return "greaterOrEquals"
	case "AND":
		return "and"
	case "OR":
		return "or"
	case "||":
		return "concat"
	default:
		return strings.ToLower(op)
	}
}

// unaryOperatorToFunction maps unary operators to ClickHouse function names
func unaryOperatorToFunction(op string) string {
	switch op {
	case "-":
		return "negate"
	case "NOT":
		return "not"
	default:
		return strings.ToLower(op)
	}
}

// explainAliasedExpr handles expressions with aliases
func explainAliasedExpr(sb *strings.Builder, n *ast.AliasedExpr, depth int) {
	// For aliased expressions, we need to show the underlying expression with the alias
	indent := strings.Repeat(" ", depth)

	switch e := n.Expr.(type) {
	case *ast.Literal:
		fmt.Fprintf(sb, "%sLiteral %s (alias %s)\n", indent, formatLiteral(e), n.Alias)
	default:
		// For other types, recursively explain and add alias info
		explainNode(sb, n.Expr, depth)
	}
}
