package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

func explainFunctionCall(sb *strings.Builder, n *ast.FunctionCall, indent string, depth int) {
	explainFunctionCallWithAlias(sb, n, n.Alias, indent, depth)
}

func explainFunctionCallWithAlias(sb *strings.Builder, n *ast.FunctionCall, alias string, indent string, depth int) {
	children := 1 // arguments ExpressionList
	if len(n.Parameters) > 0 {
		children++ // parameters ExpressionList
	}
	if n.Over != nil {
		children++ // WindowDefinition for OVER clause
	}
	// Normalize function name
	fnName := NormalizeFunctionName(n.Name)
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, alias, children)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, children)
	}
	// Arguments (Settings are included as part of argument count)
	argCount := len(n.Arguments)
	if len(n.Settings) > 0 {
		argCount++ // Set is counted as one argument
	}
	fmt.Fprintf(sb, "%s ExpressionList", indent)
	if argCount > 0 {
		fmt.Fprintf(sb, " (children %d)", argCount)
	}
	fmt.Fprintln(sb)
	for _, arg := range n.Arguments {
		// For view() table function, unwrap Subquery wrapper
		if strings.ToLower(n.Name) == "view" {
			if sq, ok := arg.(*ast.Subquery); ok {
				Node(sb, sq.Query, depth+2)
				continue
			}
		}
		Node(sb, arg, depth+2)
	}
	// Settings appear as Set node inside ExpressionList
	if len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s  Set\n", indent)
	}
	// Parameters (for parametric functions)
	if len(n.Parameters) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Parameters))
		for _, p := range n.Parameters {
			Node(sb, p, depth+2)
		}
	}
	// Window definition (for window functions with OVER clause)
	// WindowDefinition is a sibling to ExpressionList, so use the same indent
	if n.Over != nil {
		explainWindowSpec(sb, n.Over, indent+" ", depth+1)
	}
}

func explainLambda(sb *strings.Builder, n *ast.Lambda, indent string, depth int) {
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
	Node(sb, n.Body, depth+2)
}

func explainCastExpr(sb *strings.Builder, n *ast.CastExpr, indent string, depth int) {
	explainCastExprWithAlias(sb, n, n.Alias, indent, depth)
}

func explainCastExprWithAlias(sb *strings.Builder, n *ast.CastExpr, alias string, indent string, depth int) {
	// For :: operator syntax, ClickHouse hides alias only when expression is
	// an array/tuple with complex content that gets formatted as string
	hideAlias := false
	if n.OperatorSyntax {
		if lit, ok := n.Expr.(*ast.Literal); ok {
			if lit.Type == ast.LiteralArray || lit.Type == ast.LiteralTuple {
				hideAlias = !containsOnlyPrimitives(lit)
			}
		}
	}

	// CAST is represented as Function CAST with expr and type as arguments
	if alias != "" && !hideAlias {
		fmt.Fprintf(sb, "%sFunction CAST (alias %s) (children %d)\n", indent, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction CAST (children %d)\n", indent, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	// For :: operator syntax with simple literals, format as string literal
	// For function syntax or complex expressions, use normal AST node
	if n.OperatorSyntax {
		if lit, ok := n.Expr.(*ast.Literal); ok {
			// For arrays/tuples of simple primitives, use FormatLiteral (Array_[...] format)
			// For strings and other types, use string format
			if lit.Type == ast.LiteralArray || lit.Type == ast.LiteralTuple {
				if containsOnlyPrimitives(lit) {
					fmt.Fprintf(sb, "%s  Literal %s\n", indent, FormatLiteral(lit))
				} else {
					// Complex content - format as string
					exprStr := formatExprAsString(lit)
					fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, exprStr)
				}
			} else {
				// Simple literal - format as string
				exprStr := formatExprAsString(lit)
				fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, exprStr)
			}
		} else {
			// Complex expression - use normal AST node
			Node(sb, n.Expr, depth+2)
		}
	} else {
		Node(sb, n.Expr, depth+2)
	}
	// Type is formatted as a literal string
	typeStr := FormatDataType(n.Type)
	fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, typeStr)
}

// containsOnlyPrimitives checks if a literal array/tuple contains only primitive literals
func containsOnlyPrimitives(lit *ast.Literal) bool {
	var exprs []ast.Expression
	switch lit.Type {
	case ast.LiteralArray, ast.LiteralTuple:
		var ok bool
		exprs, ok = lit.Value.([]ast.Expression)
		if !ok {
			return false
		}
	default:
		return true
	}

	for _, e := range exprs {
		innerLit, ok := e.(*ast.Literal)
		if !ok {
			return false
		}
		// Strings with special chars are not considered primitive for this purpose
		if innerLit.Type == ast.LiteralString {
			s := innerLit.Value.(string)
			// Strings that look like JSON or contain special chars should be converted to string format
			if strings.ContainsAny(s, "{}[]\"\\") {
				return false
			}
		}
		// Nested arrays/tuples need recursive check
		if innerLit.Type == ast.LiteralArray || innerLit.Type == ast.LiteralTuple {
			if !containsOnlyPrimitives(innerLit) {
				return false
			}
		}
	}
	return true
}

func explainInExpr(sb *strings.Builder, n *ast.InExpr, indent string, depth int) {
	// IN is represented as Function in
	fnName := "in"
	if n.Not {
		fnName = "notIn"
	}
	if n.Global {
		fnName = "global" + strings.Title(fnName)
	}
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)

	// Determine if the IN list should be combined into a single tuple literal
	// This happens when we have multiple literals of the same type:
	// - All numeric literals (integers/floats)
	// - All tuple literals
	canBeTupleLiteral := false
	if n.Query == nil && len(n.List) > 1 {
		allNumeric := true
		allTuples := true
		for _, item := range n.List {
			if lit, ok := item.(*ast.Literal); ok {
				if lit.Type != ast.LiteralInteger && lit.Type != ast.LiteralFloat {
					allNumeric = false
				}
				if lit.Type != ast.LiteralTuple {
					allTuples = false
				}
			} else {
				allNumeric = false
				allTuples = false
				break
			}
		}
		canBeTupleLiteral = allNumeric || allTuples
	}

	// Count arguments: expr + list items or subquery
	argCount := 1
	if n.Query != nil {
		argCount++
	} else if canBeTupleLiteral {
		// Multiple literals will be combined into a single tuple
		argCount++
	} else {
		// Check if we have a single tuple literal that should be wrapped in Function tuple
		if len(n.List) == 1 {
			if lit, ok := n.List[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
				// Single tuple literal gets wrapped in Function tuple, so count as 1
				argCount++
			} else {
				argCount += len(n.List)
			}
		} else {
			argCount += len(n.List)
		}
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
	Node(sb, n.Expr, depth+2)

	if n.Query != nil {
		// Subqueries in IN should be wrapped in Subquery node
		fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
		Node(sb, n.Query, depth+3)
	} else if canBeTupleLiteral {
		// Combine multiple literals into a single Tuple literal
		tupleLit := &ast.Literal{
			Type:  ast.LiteralTuple,
			Value: n.List,
		}
		fmt.Fprintf(sb, "%s  Literal %s\n", indent, FormatLiteral(tupleLit))
	} else if len(n.List) == 1 {
		// Single element in the list
		// If it's a tuple literal, wrap it in Function tuple
		// Otherwise, output the element directly
		if lit, ok := n.List[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
			// Wrap tuple literal in Function tuple
			fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, 1)
			Node(sb, n.List[0], depth+4)
		} else {
			// Single non-tuple element - output directly
			Node(sb, n.List[0], depth+2)
		}
	} else {
		for _, item := range n.List {
			Node(sb, item, depth+2)
		}
	}
}

func explainTernaryExpr(sb *strings.Builder, n *ast.TernaryExpr, indent string, depth int) {
	// Ternary is represented as Function if with 3 arguments
	fmt.Fprintf(sb, "%sFunction if (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 3)
	Node(sb, n.Condition, depth+2)
	Node(sb, n.Then, depth+2)
	Node(sb, n.Else, depth+2)
}

func explainArrayAccess(sb *strings.Builder, n *ast.ArrayAccess, indent string, depth int) {
	// Array access is represented as Function arrayElement
	fmt.Fprintf(sb, "%sFunction arrayElement (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, n.Array, depth+2)
	Node(sb, n.Index, depth+2)
}

func explainArrayAccessWithAlias(sb *strings.Builder, n *ast.ArrayAccess, alias string, indent string, depth int) {
	// Array access is represented as Function arrayElement
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction arrayElement (alias %s) (children %d)\n", indent, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction arrayElement (children %d)\n", indent, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, n.Array, depth+2)
	Node(sb, n.Index, depth+2)
}

func explainTupleAccess(sb *strings.Builder, n *ast.TupleAccess, indent string, depth int) {
	// Tuple access is represented as Function tupleElement
	fmt.Fprintf(sb, "%sFunction tupleElement (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, n.Tuple, depth+2)
	Node(sb, n.Index, depth+2)
}

func explainTupleAccessWithAlias(sb *strings.Builder, n *ast.TupleAccess, alias string, indent string, depth int) {
	// Tuple access is represented as Function tupleElement
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction tupleElement (alias %s) (children %d)\n", indent, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction tupleElement (children %d)\n", indent, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, n.Tuple, depth+2)
	Node(sb, n.Index, depth+2)
}

func explainLikeExpr(sb *strings.Builder, n *ast.LikeExpr, indent string, depth int) {
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
	Node(sb, n.Expr, depth+2)
	Node(sb, n.Pattern, depth+2)
}

func explainBetweenExpr(sb *strings.Builder, n *ast.BetweenExpr, indent string, depth int) {
	if n.Not {
		// NOT BETWEEN is transformed to: expr < low OR expr > high
		// Represented as: Function or with two comparisons: less and greater
		fmt.Fprintf(sb, "%sFunction or (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		// less(expr, low)
		fmt.Fprintf(sb, "%s  Function less (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, 2)
		Node(sb, n.Expr, depth+4)
		Node(sb, n.Low, depth+4)
		// greater(expr, high)
		fmt.Fprintf(sb, "%s  Function greater (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, 2)
		Node(sb, n.Expr, depth+4)
		Node(sb, n.High, depth+4)
	} else {
		// BETWEEN is represented as Function and with two comparisons
		// expr >= low AND expr <= high
		fmt.Fprintf(sb, "%sFunction and (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
		// greaterOrEquals(expr, low)
		fmt.Fprintf(sb, "%s  Function greaterOrEquals (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, 2)
		Node(sb, n.Expr, depth+4)
		Node(sb, n.Low, depth+4)
		// lessOrEquals(expr, high)
		fmt.Fprintf(sb, "%s  Function lessOrEquals (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, 2)
		Node(sb, n.Expr, depth+4)
		Node(sb, n.High, depth+4)
	}
}

func explainIsNullExpr(sb *strings.Builder, n *ast.IsNullExpr, indent string, depth int) {
	// IS NULL is represented as Function isNull
	fnName := "isNull"
	if n.Not {
		fnName = "isNotNull"
	}
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	Node(sb, n.Expr, depth+2)
}

func explainCaseExpr(sb *strings.Builder, n *ast.CaseExpr, indent string, depth int) {
	// CASE is represented as Function multiIf or caseWithExpression
	if n.Operand != nil {
		// CASE x WHEN ... form
		argCount := 1 + len(n.Whens)*2 // operand + (condition, result) pairs
		if n.Else != nil {
			argCount++
		}
		fmt.Fprintf(sb, "%sFunction caseWithExpression (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
		Node(sb, n.Operand, depth+2)
		for _, w := range n.Whens {
			Node(sb, w.Condition, depth+2)
			Node(sb, w.Result, depth+2)
		}
		if n.Else != nil {
			Node(sb, n.Else, depth+2)
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
			Node(sb, w.Condition, depth+2)
			Node(sb, w.Result, depth+2)
		}
		if n.Else != nil {
			Node(sb, n.Else, depth+2)
		}
	}
}

func explainIntervalExpr(sb *strings.Builder, n *ast.IntervalExpr, alias string, indent string, depth int) {
	// INTERVAL is represented as Function toInterval<Unit>
	// Unit needs to be title-cased (e.g., YEAR -> Year)
	unit := n.Unit
	if len(unit) > 0 {
		unit = strings.ToUpper(unit[:1]) + strings.ToLower(unit[1:])
	}
	fnName := "toInterval" + unit
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	Node(sb, n.Value, depth+2)
}

func explainExistsExpr(sb *strings.Builder, n *ast.ExistsExpr, indent string, depth int) {
	// EXISTS is represented as Function exists
	fmt.Fprintf(sb, "%sFunction exists (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
	Node(sb, n.Query, depth+3)
}

func explainExtractExpr(sb *strings.Builder, n *ast.ExtractExpr, indent string, depth int) {
	// EXTRACT is represented as Function toYear, toMonth, etc.
	fnName := "to" + strings.Title(strings.ToLower(n.Field))
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	Node(sb, n.From, depth+2)
}

func explainWindowSpec(sb *strings.Builder, n *ast.WindowSpec, indent string, depth int) {
	// Window spec is represented as WindowDefinition
	// For simple cases like OVER (), just output WindowDefinition without children
	// Note: ClickHouse's EXPLAIN AST does not output frame info (ROWS BETWEEN etc)
	children := 0
	if n.Name != "" {
		children++
	}
	if len(n.PartitionBy) > 0 {
		children++
	}
	if len(n.OrderBy) > 0 {
		children++
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sWindowDefinition (children %d)\n", indent, children)
		if n.Name != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Name)
		}
		if len(n.PartitionBy) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.PartitionBy))
			for _, e := range n.PartitionBy {
				Node(sb, e, depth+2)
			}
		}
		if len(n.OrderBy) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.OrderBy))
			for _, o := range n.OrderBy {
				explainOrderByElement(sb, o, strings.Repeat(" ", depth+2), depth+2)
			}
		}
		// Frame handling would go here if needed
	} else {
		fmt.Fprintf(sb, "%sWindowDefinition\n", indent)
	}
}
