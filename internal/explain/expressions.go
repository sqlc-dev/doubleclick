package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

func explainIdentifier(sb *strings.Builder, n *ast.Identifier, indent string) {
	name := formatIdentifierName(n)
	if n.Alias != "" {
		fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, name, n.Alias)
	} else {
		fmt.Fprintf(sb, "%sIdentifier %s\n", indent, name)
	}
}

// formatIdentifierName formats an identifier name, handling JSON path notation
func formatIdentifierName(n *ast.Identifier) string {
	if len(n.Parts) == 0 {
		return ""
	}
	if len(n.Parts) == 1 {
		return n.Parts[0]
	}
	result := n.Parts[0]
	for _, p := range n.Parts[1:] {
		// JSON path notation: ^fieldname should be formatted as ^`fieldname`
		if strings.HasPrefix(p, "^") {
			result += ".^`" + p[1:] + "`"
		} else {
			result += "." + p
		}
	}
	return result
}

func explainLiteral(sb *strings.Builder, n *ast.Literal, indent string, depth int) {
	// Check if this is a tuple - either with expressions or empty
	if n.Type == ast.LiteralTuple {
		if exprs, ok := n.Value.([]ast.Expression); ok {
			// Check if empty tuple or has complex expressions
			if len(exprs) == 0 {
				// Empty tuple renders as Function tuple with empty ExpressionList
				fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s ExpressionList\n", indent)
				return
			}
			hasComplexExpr := false
			for _, e := range exprs {
				lit, isLit := e.(*ast.Literal)
				// Non-literals or tuple/array literals count as complex
				if !isLit || (isLit && (lit.Type == ast.LiteralTuple || lit.Type == ast.LiteralArray)) {
					hasComplexExpr = true
					break
				}
			}
			if hasComplexExpr {
				// Render as Function tuple instead of Literal
				fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
				for _, e := range exprs {
					Node(sb, e, depth+2)
				}
				return
			}
		} else if n.Value == nil {
			// nil value means empty tuple
			fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s ExpressionList\n", indent)
			return
		}
	}
	// Check if this is an array with complex expressions or empty that should be rendered as Function array
	if n.Type == ast.LiteralArray {
		if exprs, ok := n.Value.([]ast.Expression); ok {
			// Empty array renders as Function array with empty ExpressionList
			if len(exprs) == 0 {
				fmt.Fprintf(sb, "%sFunction array (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s ExpressionList\n", indent)
				return
			}
			hasComplexExpr := false
			for _, e := range exprs {
				if !isSimpleLiteralOrNegation(e) {
					hasComplexExpr = true
					break
				}
			}
			if hasComplexExpr {
				// Render as Function array instead of Literal
				fmt.Fprintf(sb, "%sFunction array (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
				for _, e := range exprs {
					Node(sb, e, depth+2)
				}
				return
			}
		} else if n.Value == nil {
			// nil value means empty array
			fmt.Fprintf(sb, "%sFunction array (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s ExpressionList\n", indent)
			return
		}
	}
	fmt.Fprintf(sb, "%sLiteral %s\n", indent, FormatLiteral(n))
}

// isSimpleLiteralOrNegation checks if an expression is a simple literal
// or a unary negation of a numeric literal (for array elements)
func isSimpleLiteralOrNegation(e ast.Expression) bool {
	// Direct literal check
	if lit, ok := e.(*ast.Literal); ok {
		// Nested arrays/tuples are complex
		return lit.Type != ast.LiteralTuple && lit.Type != ast.LiteralArray
	}
	// Unary minus of a literal integer/float is also simple (negative number)
	if unary, ok := e.(*ast.UnaryExpr); ok && unary.Op == "-" {
		if lit, ok := unary.Operand.(*ast.Literal); ok {
			return lit.Type == ast.LiteralInteger || lit.Type == ast.LiteralFloat
		}
	}
	return false
}

func explainBinaryExpr(sb *strings.Builder, n *ast.BinaryExpr, indent string, depth int) {
	// Convert operator to function name
	fnName := OperatorToFunction(n.Op)

	// For || (concat) operator, flatten chained concatenations
	if n.Op == "||" {
		operands := collectConcatOperands(n)
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(operands))
		for _, op := range operands {
			Node(sb, op, depth+2)
		}
		return
	}

	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, n.Left, depth+2)
	Node(sb, n.Right, depth+2)
}

// collectConcatOperands flattens chained || (concat) operations into a list of operands
func collectConcatOperands(n *ast.BinaryExpr) []ast.Expression {
	var operands []ast.Expression

	// Recursively collect from left side if it's also a concat
	if left, ok := n.Left.(*ast.BinaryExpr); ok && left.Op == "||" {
		operands = append(operands, collectConcatOperands(left)...)
	} else {
		operands = append(operands, n.Left)
	}

	// Recursively collect from right side if it's also a concat
	if right, ok := n.Right.(*ast.BinaryExpr); ok && right.Op == "||" {
		operands = append(operands, collectConcatOperands(right)...)
	} else {
		operands = append(operands, n.Right)
	}

	return operands
}

func explainUnaryExpr(sb *strings.Builder, n *ast.UnaryExpr, indent string, depth int) {
	// Handle negate of literal numbers - output as negative literal instead of function
	if n.Op == "-" {
		if lit, ok := n.Operand.(*ast.Literal); ok {
			switch lit.Type {
			case ast.LiteralInteger:
				// Convert positive integer to negative
				switch val := lit.Value.(type) {
				case int64:
					fmt.Fprintf(sb, "%sLiteral Int64_%d\n", indent, -val)
					return
				case uint64:
					fmt.Fprintf(sb, "%sLiteral Int64_-%d\n", indent, val)
					return
				}
			case ast.LiteralFloat:
				val := lit.Value.(float64)
				s := FormatFloat(-val)
				fmt.Fprintf(sb, "%sLiteral Float64_%s\n", indent, s)
				return
			}
		}
	}

	fnName := UnaryOperatorToFunction(n.Op)
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	Node(sb, n.Operand, depth+2)
}

func explainSubquery(sb *strings.Builder, n *ast.Subquery, indent string, depth int) {
	children := 1
	if n.Alias != "" {
		fmt.Fprintf(sb, "%sSubquery (alias %s) (children %d)\n", indent, n.Alias, children)
	} else {
		fmt.Fprintf(sb, "%sSubquery (children %d)\n", indent, children)
	}
	Node(sb, n.Query, depth+1)
}

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
						Node(sb, expr, depth+2)
					}
					return
				}
			}
		}
		fmt.Fprintf(sb, "%sLiteral %s (alias %s)\n", indent, FormatLiteral(e), n.Alias)
	case *ast.BinaryExpr:
		// Binary expressions become functions with alias
		fnName := OperatorToFunction(e.Op)
		// For || (concat) operator, flatten chained concatenations
		if e.Op == "||" {
			operands := collectConcatOperands(e)
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Alias, 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(operands))
			for _, op := range operands {
				Node(sb, op, depth+2)
			}
		} else {
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Alias, 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
			Node(sb, e.Left, depth+2)
			Node(sb, e.Right, depth+2)
		}
	case *ast.UnaryExpr:
		// Unary expressions become functions with alias
		fnName := UnaryOperatorToFunction(e.Op)
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Alias, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		Node(sb, e.Operand, depth+2)
	case *ast.FunctionCall:
		// Function calls already handle aliases
		explainFunctionCallWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.Identifier:
		// Identifiers with alias
		fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, e.Name(), n.Alias)
	case *ast.IntervalExpr:
		// Interval expressions with alias
		explainIntervalExpr(sb, e, n.Alias, indent, depth)
	case *ast.TernaryExpr:
		// Ternary expressions become if functions with alias
		fmt.Fprintf(sb, "%sFunction if (alias %s) (children %d)\n", indent, n.Alias, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 3)
		Node(sb, e.Condition, depth+2)
		Node(sb, e.Then, depth+2)
		Node(sb, e.Else, depth+2)
	case *ast.CastExpr:
		// CAST expressions - show alias only for CAST(x AS Type) syntax, not CAST(x, 'Type')
		if e.UsedASSyntax {
			explainCastExprWithAlias(sb, e, n.Alias, indent, depth)
		} else {
			explainCastExpr(sb, e, indent, depth)
		}
	case *ast.ArrayAccess:
		// Array access - ClickHouse doesn't show aliases on arrayElement in EXPLAIN AST
		explainArrayAccess(sb, e, indent, depth)
	case *ast.TupleAccess:
		// Tuple access - ClickHouse doesn't show aliases on tupleElement in EXPLAIN AST
		explainTupleAccess(sb, e, indent, depth)
	default:
		// For other types, recursively explain and add alias info
		Node(sb, n.Expr, depth)
	}
}

func explainAsterisk(sb *strings.Builder, n *ast.Asterisk, indent string) {
	if n.Table != "" {
		fmt.Fprintf(sb, "%sQualifiedAsterisk (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	} else {
		fmt.Fprintf(sb, "%sAsterisk\n", indent)
	}
}

func explainWithElement(sb *strings.Builder, n *ast.WithElement, indent string, depth int) {
	// For WITH elements, we need to show the underlying expression with the name as alias
	switch e := n.Query.(type) {
	case *ast.Literal:
		fmt.Fprintf(sb, "%sLiteral %s (alias %s)\n", indent, FormatLiteral(e), n.Name)
	case *ast.Identifier:
		fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, e.Name(), n.Name)
	case *ast.FunctionCall:
		explainFunctionCallWithAlias(sb, e, n.Name, indent, depth)
	case *ast.BinaryExpr:
		// Binary expressions become functions
		fnName := OperatorToFunction(e.Op)
		// For || (concat) operator, flatten chained concatenations
		if e.Op == "||" {
			operands := collectConcatOperands(e)
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Name, 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(operands))
			for _, op := range operands {
				Node(sb, op, depth+2)
			}
		} else {
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Name, 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
			Node(sb, e.Left, depth+2)
			Node(sb, e.Right, depth+2)
		}
	case *ast.Subquery:
		fmt.Fprintf(sb, "%sSubquery (alias %s) (children %d)\n", indent, n.Name, 1)
		Node(sb, e.Query, depth+1)
	default:
		// For other types, just output the expression (alias may be lost)
		Node(sb, n.Query, depth)
	}
}
