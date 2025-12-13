package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

func explainIdentifier(sb *strings.Builder, n *ast.Identifier, indent string) {
	name := n.Name()
	if n.Alias != "" {
		fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, name, n.Alias)
	} else {
		fmt.Fprintf(sb, "%sIdentifier %s\n", indent, name)
	}
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
				if _, isLit := e.(*ast.Literal); !isLit {
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

func explainBinaryExpr(sb *strings.Builder, n *ast.BinaryExpr, indent string, depth int) {
	// Convert operator to function name
	fnName := OperatorToFunction(n.Op)
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, n.Left, depth+2)
	Node(sb, n.Right, depth+2)
}

func explainUnaryExpr(sb *strings.Builder, n *ast.UnaryExpr, indent string, depth int) {
	fnName := UnaryOperatorToFunction(n.Op)
	fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	Node(sb, n.Operand, depth+2)
}

func explainSubquery(sb *strings.Builder, n *ast.Subquery, indent string, depth int) {
	children := 1
	fmt.Fprintf(sb, "%sSubquery (children %d)\n", indent, children)
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
