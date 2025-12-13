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
		// FROM (including ARRAY JOIN as part of TablesInSelectQuery)
		if n.From != nil || n.ArrayJoin != nil {
			explainTablesWithArrayJoin(sb, n.From, n.ArrayJoin, depth+1)
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
		// SETTINGS
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
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
		fmt.Fprintf(sb, "%sTableExpression (children %d)\n", indent, children)
		// If there's a subquery with an alias, pass the alias to the subquery output
		if subq, ok := n.Table.(*ast.Subquery); ok && n.Alias != "" {
			fmt.Fprintf(sb, "%s Subquery (alias %s) (children %d)\n", indent, n.Alias, 1)
			explainNode(sb, subq.Query, depth+2)
		} else {
			explainNode(sb, n.Table, depth+1)
		}

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
		// Check if this is a tuple with complex expressions that should be rendered as Function tuple
		if n.Type == ast.LiteralTuple {
			if exprs, ok := n.Value.([]ast.Expression); ok {
				hasComplexExpr := false
				for _, e := range exprs {
					if _, isLit := e.(*ast.Literal); !isLit {
						hasComplexExpr = true
						break
					}
				}
				if hasComplexExpr {
					// Render as Function tuple instead of Literal
					fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
					fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
					for _, e := range exprs {
						explainNode(sb, e, depth+2)
					}
					return
				}
			}
		}
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

	case *ast.CastExpr:
		// CAST is represented as Function CAST with expr and type as arguments
		fmt.Fprintf(sb, "%sFunction CAST (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		explainNode(sb, n.Expr, depth+2)
		// Type is formatted as a literal string
		typeStr := formatDataType(n.Type)
		fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, typeStr)

	case *ast.InExpr:
		// IN is represented as Function in
		fnName := "in"
		if n.Not {
			fnName = "notIn"
		}
		if n.Global {
			fnName = "global" + strings.Title(fnName)
		}
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		// Count arguments: expr + list items or subquery
		argCount := 1
		if n.Query != nil {
			argCount++
		} else {
			argCount += len(n.List)
		}
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
		explainNode(sb, n.Expr, depth+2)
		if n.Query != nil {
			// Subqueries in IN should be wrapped in Subquery node
			fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
			explainNode(sb, n.Query, depth+3)
		} else {
			for _, item := range n.List {
				explainNode(sb, item, depth+2)
			}
		}

	case *ast.TernaryExpr:
		// Ternary is represented as Function if with 3 arguments
		fmt.Fprintf(sb, "%sFunction if (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 3)
		explainNode(sb, n.Condition, depth+2)
		explainNode(sb, n.Then, depth+2)
		explainNode(sb, n.Else, depth+2)

	case *ast.ArrayAccess:
		// Array access is represented as Function arrayElement
		fmt.Fprintf(sb, "%sFunction arrayElement (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		explainNode(sb, n.Array, depth+2)
		explainNode(sb, n.Index, depth+2)

	case *ast.TupleAccess:
		// Tuple access is represented as Function tupleElement
		fmt.Fprintf(sb, "%sFunction tupleElement (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		explainNode(sb, n.Tuple, depth+2)
		explainNode(sb, n.Index, depth+2)

	case *ast.DropQuery:
		name := n.Table
		if n.View != "" {
			name = n.View
		}
		if n.DropDatabase {
			name = n.Database
		}
		fmt.Fprintf(sb, "%sDropQuery  %s (children %d)\n", indent, name, 1)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)

	case *ast.Asterisk:
		if n.Table != "" {
			fmt.Fprintf(sb, "%sQualifiedAsterisk (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		} else {
			fmt.Fprintf(sb, "%sAsterisk\n", indent)
		}

	case *ast.LikeExpr:
		// LIKE is represented as Function like
		fnName := "like"
		if n.CaseInsensitive {
			fnName = "ilike"
		}
		if n.Not {
			fnName = "not" + strings.Title(fnName)
		}
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		explainNode(sb, n.Expr, depth+2)
		explainNode(sb, n.Pattern, depth+2)

	case *ast.BetweenExpr:
		// BETWEEN is represented as Function and with two comparisons
		// But for explain, we can use a simpler form
		fnName := "between"
		if n.Not {
			fnName = "notBetween"
		}
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 3)
		explainNode(sb, n.Expr, depth+2)
		explainNode(sb, n.Low, depth+2)
		explainNode(sb, n.High, depth+2)

	case *ast.IsNullExpr:
		// IS NULL is represented as Function isNull
		fnName := "isNull"
		if n.Not {
			fnName = "isNotNull"
		}
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		explainNode(sb, n.Expr, depth+2)

	case *ast.CaseExpr:
		// CASE is represented as Function multiIf or caseWithExpression
		if n.Operand != nil {
			// CASE x WHEN ... form
			argCount := 1 + len(n.Whens)*2 // operand + (condition, result) pairs
			if n.Else != nil {
				argCount++
			}
			fmt.Fprintf(sb, "%sFunction caseWithExpression (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
			explainNode(sb, n.Operand, depth+2)
			for _, w := range n.Whens {
				explainNode(sb, w.Condition, depth+2)
				explainNode(sb, w.Result, depth+2)
			}
			if n.Else != nil {
				explainNode(sb, n.Else, depth+2)
			}
		} else {
			// CASE WHEN ... form
			argCount := len(n.Whens) * 2
			if n.Else != nil {
				argCount++
			}
			fmt.Fprintf(sb, "%sFunction multiIf (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
			for _, w := range n.Whens {
				explainNode(sb, w.Condition, depth+2)
				explainNode(sb, w.Result, depth+2)
			}
			if n.Else != nil {
				explainNode(sb, n.Else, depth+2)
			}
		}

	case *ast.IntervalExpr:
		// INTERVAL is represented as Function toInterval<Unit>
		fnName := "toInterval" + n.Unit
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		explainNode(sb, n.Value, depth+2)

	case *ast.ExistsExpr:
		// EXISTS is represented as Function exists
		fmt.Fprintf(sb, "%sFunction exists (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
		explainNode(sb, n.Query, depth+3)

	case *ast.ExtractExpr:
		// EXTRACT is represented as Function toYear, toMonth, etc.
		fnName := "to" + strings.Title(strings.ToLower(n.Field))
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		explainNode(sb, n.From, depth+2)

	case *ast.CreateQuery:
		name := n.Table
		if n.View != "" {
			name = n.View
		}
		if n.CreateDatabase {
			name = n.Database
		}
		// Count children: name + columns + engine/storage
		children := 1 // name identifier
		if len(n.Columns) > 0 {
			children++
		}
		if n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 {
			children++
		}
		if n.AsSelect != nil {
			children++
		}
		fmt.Fprintf(sb, "%sCreateQuery %s (children %d)\n", indent, name, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
		if len(n.Columns) > 0 {
			fmt.Fprintf(sb, "%s Columns definition (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Columns))
			for _, col := range n.Columns {
				explainColumn(sb, col, depth+3)
			}
		}
		if n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 {
			storageChildren := 0
			if n.Engine != nil {
				storageChildren++
			}
			if len(n.OrderBy) > 0 {
				storageChildren++
			}
			if len(n.PrimaryKey) > 0 {
				storageChildren++
			}
			fmt.Fprintf(sb, "%s Storage definition (children %d)\n", indent, storageChildren)
			if n.Engine != nil {
				fmt.Fprintf(sb, "%s  Function %s\n", indent, n.Engine.Name)
			}
			if len(n.OrderBy) > 0 {
				if len(n.OrderBy) == 1 {
					if ident, ok := n.OrderBy[0].(*ast.Identifier); ok {
						fmt.Fprintf(sb, "%s  Identifier %s\n", indent, ident.Name())
					} else {
						explainNode(sb, n.OrderBy[0], depth+2)
					}
				} else {
					fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
					fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.OrderBy))
					for _, o := range n.OrderBy {
						explainNode(sb, o, depth+4)
					}
				}
			}
		}
		if n.AsSelect != nil {
			fmt.Fprintf(sb, "%s Subquery (children %d)\n", indent, 1)
			explainNode(sb, n.AsSelect, depth+2)
		}

	case *ast.SystemQuery:
		fmt.Fprintf(sb, "%sSystem %s\n", indent, n.Command)

	case *ast.ExplainQuery:
		fmt.Fprintf(sb, "%sExplain %s (children %d)\n", indent, n.ExplainType, 1)
		explainNode(sb, n.Statement, depth+1)

	case *ast.ShowQuery:
		fmt.Fprintf(sb, "%sShow%s\n", indent, n.ShowType)

	case *ast.UseQuery:
		fmt.Fprintf(sb, "%sUse %s\n", indent, n.Database)

	case *ast.DescribeQuery:
		name := n.Table
		if n.Database != "" {
			name = n.Database + "." + n.Table
		}
		fmt.Fprintf(sb, "%sDescribe %s\n", indent, name)

	case *ast.TableJoin:
		// TableJoin is part of TablesInSelectQueryElement
		joinType := strings.ToLower(string(n.Type))
		if n.Strictness != "" {
			joinType = strings.ToLower(string(n.Strictness)) + " " + joinType
		}
		if n.Global {
			joinType = "global " + joinType
		}
		children := 0
		if n.On != nil {
			children++
		}
		if len(n.Using) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sTableJoin %s (children %d)\n", indent, joinType, children)
		if n.On != nil {
			explainNode(sb, n.On, depth+1)
		}
		if len(n.Using) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Using))
			for _, u := range n.Using {
				explainNode(sb, u, depth+2)
			}
		}

	case *ast.DataType:
		fmt.Fprintf(sb, "%sDataType %s\n", indent, formatDataType(n))

	case *ast.Parameter:
		if n.Name != "" {
			fmt.Fprintf(sb, "%sQueryParameter %s\n", indent, n.Name)
		} else {
			fmt.Fprintf(sb, "%sQueryParameter\n", indent)
		}

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

// explainTablesWithArrayJoin handles FROM and ARRAY JOIN together as TablesInSelectQuery
func explainTablesWithArrayJoin(sb *strings.Builder, from *ast.TablesInSelectQuery, arrayJoin *ast.ArrayJoinClause, depth int) {
	indent := strings.Repeat(" ", depth)

	tableCount := 0
	if from != nil {
		tableCount = len(from.Tables)
	}
	if arrayJoin != nil {
		tableCount++
	}

	fmt.Fprintf(sb, "%sTablesInSelectQuery (children %d)\n", indent, tableCount)

	if from != nil {
		for _, t := range from.Tables {
			explainNode(sb, t, depth+1)
		}
	}

	if arrayJoin != nil {
		// ARRAY JOIN is wrapped in TablesInSelectQueryElement
		fmt.Fprintf(sb, "%s TablesInSelectQueryElement (children %d)\n", indent, 1)
		explainNode(sb, arrayJoin, depth+2)
	}
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

// formatDataType formats a DataType for EXPLAIN AST output
func formatDataType(dt *ast.DataType) string {
	if dt == nil {
		return ""
	}
	if len(dt.Parameters) == 0 {
		return dt.Name
	}
	var params []string
	for _, p := range dt.Parameters {
		if lit, ok := p.(*ast.Literal); ok {
			if lit.Type == ast.LiteralString {
				// String parameters in type need extra escaping: 'val' -> \\\'val\\\'
				params = append(params, fmt.Sprintf("\\\\\\'%s\\\\\\'", lit.Value))
			} else {
				params = append(params, fmt.Sprintf("%v", lit.Value))
			}
		} else if nested, ok := p.(*ast.DataType); ok {
			params = append(params, formatDataType(nested))
		} else {
			params = append(params, fmt.Sprintf("%v", p))
		}
	}
	return fmt.Sprintf("%s(%s)", dt.Name, strings.Join(params, ", "))
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

// explainColumn handles column declarations
func explainColumn(sb *strings.Builder, col *ast.ColumnDeclaration, depth int) {
	indent := strings.Repeat(" ", depth)
	children := 0
	if col.Type != nil {
		children++
	}
	if col.Default != nil {
		children++
	}
	fmt.Fprintf(sb, "%sColumnDeclaration %s (children %d)\n", indent, col.Name, children)
	if col.Type != nil {
		fmt.Fprintf(sb, "%s DataType %s\n", indent, formatDataType(col.Type))
	}
	if col.Default != nil {
		explainNode(sb, col.Default, depth+1)
	}
}

// explainAliasedExpr handles expressions with aliases
func explainAliasedExpr(sb *strings.Builder, n *ast.AliasedExpr, depth int) {
	// For aliased expressions, we need to show the underlying expression with the alias
	indent := strings.Repeat(" ", depth)

	switch e := n.Expr.(type) {
	case *ast.Literal:
		// Check if this is a tuple with complex expressions that should be rendered as Function tuple
		if e.Type == ast.LiteralTuple {
			if exprs, ok := e.Value.([]ast.Expression); ok {
				hasComplexExpr := false
				for _, expr := range exprs {
					if _, isLit := expr.(*ast.Literal); !isLit {
						hasComplexExpr = true
						break
					}
				}
				if hasComplexExpr {
					// Render as Function tuple with alias
					fmt.Fprintf(sb, "%sFunction tuple (alias %s) (children %d)\n", indent, n.Alias, 1)
					fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
					for _, expr := range exprs {
						explainNode(sb, expr, depth+2)
					}
					return
				}
			}
		}
		fmt.Fprintf(sb, "%sLiteral %s (alias %s)\n", indent, formatLiteral(e), n.Alias)
	default:
		// For other types, recursively explain and add alias info
		explainNode(sb, n.Expr, depth)
	}
}
