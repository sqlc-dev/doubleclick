package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

func explainFunctionCall(sb *strings.Builder, n *ast.FunctionCall, indent string, depth int) {
	explainFunctionCallWithAlias(sb, n, n.Alias, indent, depth)
}

func explainFunctionCallWithAlias(sb *strings.Builder, n *ast.FunctionCall, alias string, indent string, depth int) {
	children := 1 // arguments ExpressionList
	if len(n.Parameters) > 0 {
		children++ // parameters ExpressionList
	}
	// Normalize function name
	fnName := NormalizeFunctionName(n.Name)
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, alias, children)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, children)
	}
	// Arguments
	fmt.Fprintf(sb, "%s ExpressionList", indent)
	if len(n.Arguments) > 0 {
		fmt.Fprintf(sb, " (children %d)", len(n.Arguments))
	}
	fmt.Fprintln(sb)
	for _, arg := range n.Arguments {
		Node(sb, arg, depth+2)
	}
	// Parameters (for parametric functions)
	if len(n.Parameters) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Parameters))
		for _, p := range n.Parameters {
			Node(sb, p, depth+2)
		}
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
	// CAST is represented as Function CAST with expr and type as arguments
	fmt.Fprintf(sb, "%sFunction CAST (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	// For :: operator syntax with simple literals, format as string literal
	// For function syntax or complex expressions, use normal AST node
	if n.OperatorSyntax {
		if lit, ok := n.Expr.(*ast.Literal); ok {
			// Format literal as string
			exprStr := formatExprAsString(lit)
			fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, exprStr)
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
	// Count arguments: expr + list items or subquery
	argCount := 1
	if n.Query != nil {
		argCount++
	} else {
		argCount += len(n.List)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
	Node(sb, n.Expr, depth+2)
	if n.Query != nil {
		// Subqueries in IN should be wrapped in Subquery node
		fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
		Node(sb, n.Query, depth+3)
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

func explainTupleAccess(sb *strings.Builder, n *ast.TupleAccess, indent string, depth int) {
	// Tuple access is represented as Function tupleElement
	fmt.Fprintf(sb, "%sFunction tupleElement (children %d)\n", indent, 1)
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
	// BETWEEN is represented as Function and with two comparisons
	// But for explain, we can use a simpler form
	fnName := "between"
	if n.Not {
		fnName = "notBetween"
	}
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 3)
	Node(sb, n.Expr, depth+2)
	Node(sb, n.Low, depth+2)
	Node(sb, n.High, depth+2)
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

func explainIntervalExpr(sb *strings.Builder, n *ast.IntervalExpr, indent string, depth int) {
	// INTERVAL is represented as Function toInterval<Unit>
	// Unit needs to be title-cased (e.g., YEAR -> Year)
	unit := n.Unit
	if len(unit) > 0 {
		unit = strings.ToUpper(unit[:1]) + strings.ToLower(unit[1:])
	}
	fnName := "toInterval" + unit
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
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
