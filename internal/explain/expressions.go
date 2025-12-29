package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// escapeAlias escapes backslashes in alias names for EXPLAIN output
func escapeAlias(alias string) string {
	return strings.ReplaceAll(alias, "\\", "\\\\")
}

func explainIdentifier(sb *strings.Builder, n *ast.Identifier, indent string) {
	name := formatIdentifierName(n)
	if n.Alias != "" {
		fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, name, escapeAlias(n.Alias))
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
			// Single-element tuples (from trailing comma syntax like (1,)) always render as Function tuple
			if len(exprs) == 1 {
				fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
				for _, e := range exprs {
					Node(sb, e, depth+2)
				}
				return
			}
			hasComplexExpr := false
			for _, e := range exprs {
				// Simple literals (numbers, strings, etc.) are OK
				if lit, isLit := e.(*ast.Literal); isLit {
					// Nested tuples/arrays are complex
					if lit.Type == ast.LiteralTuple || lit.Type == ast.LiteralArray {
						hasComplexExpr = true
						break
					}
					// Other literals are simple
					continue
				}
				// Unary negation of numeric literals is also simple
				if unary, isUnary := e.(*ast.UnaryExpr); isUnary && unary.Op == "-" {
					if lit, isLit := unary.Operand.(*ast.Literal); isLit {
						if lit.Type == ast.LiteralInteger || lit.Type == ast.LiteralFloat {
							continue
						}
					}
				}
				// Everything else is complex
				hasComplexExpr = true
				break
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
			// Check if we should render as Function array
			// This happens when:
			// 1. Contains non-literal, non-negation expressions OR
			// 2. Contains tuples OR
			// 3. Contains nested arrays that all have exactly 1 element (homogeneous single-element arrays) OR
			// 4. Contains nested arrays with non-literal expressions OR
			// 5. Contains nested arrays that are empty or contain tuples/non-literals
			shouldUseFunctionArray := false
			allAreSingleElementArrays := true
			hasNestedArrays := false
			nestedArraysNeedFunctionFormat := false

			for _, e := range exprs {
				if lit, ok := e.(*ast.Literal); ok {
					if lit.Type == ast.LiteralArray {
						hasNestedArrays = true
						// Check if this inner array has exactly 1 element
						if innerExprs, ok := lit.Value.([]ast.Expression); ok {
							if len(innerExprs) != 1 {
								allAreSingleElementArrays = false
							}
							// Check if inner array needs Function array format:
							// - Contains non-literal expressions OR
							// - Contains tuples OR
							// - Is empty OR
							// - Contains empty arrays
							if containsNonLiteralExpressions(innerExprs) ||
								len(innerExprs) == 0 ||
								containsTuples(innerExprs) ||
								containsEmptyArrays(innerExprs) {
								nestedArraysNeedFunctionFormat = true
							}
						} else {
							allAreSingleElementArrays = false
						}
					} else if lit.Type == ast.LiteralTuple {
						// Tuples are complex
						shouldUseFunctionArray = true
					}
				} else if !isSimpleLiteralOrNegation(e) {
					shouldUseFunctionArray = true
				}
			}

			// Use Function array when:
			// - nested arrays that are ALL single-element
			// - nested arrays that need Function format (contain non-literals, tuples, or empty arrays)
			if hasNestedArrays && (allAreSingleElementArrays || nestedArraysNeedFunctionFormat) {
				shouldUseFunctionArray = true
			}

			if shouldUseFunctionArray {
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

// containsOnlyArraysOrTuples checks if a slice of expressions contains
// only array or tuple literals (including empty arrays).
// Returns true if the slice is empty or contains only arrays/tuples.
func containsOnlyArraysOrTuples(exprs []ast.Expression) bool {
	if len(exprs) == 0 {
		return true // empty is considered "only arrays"
	}
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok {
			if lit.Type != ast.LiteralArray && lit.Type != ast.LiteralTuple {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// containsNonLiteralExpressions checks if a slice of expressions contains
// any non-literal expressions (identifiers, function calls, etc.)
func containsNonLiteralExpressions(exprs []ast.Expression) bool {
	for _, e := range exprs {
		if _, ok := e.(*ast.Literal); !ok {
			return true
		}
	}
	return false
}

// containsTuples checks if a slice of expressions contains any tuple literals
func containsTuples(exprs []ast.Expression) bool {
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
			return true
		}
	}
	return false
}

// containsEmptyArrays checks if a slice of expressions contains any empty array literals
func containsEmptyArrays(exprs []ast.Expression) bool {
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok && lit.Type == ast.LiteralArray {
			if innerExprs, ok := lit.Value.([]ast.Expression); ok && len(innerExprs) == 0 {
				return true
			}
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
		fmt.Fprintf(sb, "%sSubquery (alias %s) (children %d)\n", indent, escapeAlias(n.Alias), children)
	} else {
		fmt.Fprintf(sb, "%sSubquery (children %d)\n", indent, children)
	}
	// Set context flag before recursing into subquery content
	// This affects how negated literals with aliases are formatted
	prevContext := inSubqueryContext
	inSubqueryContext = true
	Node(sb, n.Query, depth+1)
	inSubqueryContext = prevContext
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
					fmt.Fprintf(sb, "%sFunction tuple (alias %s) (children %d)\n", indent, escapeAlias(n.Alias), 1)
					fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
					for _, expr := range exprs {
						Node(sb, expr, depth+2)
					}
					return
				}
			}
		}
		// Check if this is an array containing specific expressions that need Function array format
		if e.Type == ast.LiteralArray {
			if exprs, ok := e.Value.([]ast.Expression); ok {
				needsFunctionFormat := false
				// Empty arrays always use Function array format
				if len(exprs) == 0 {
					needsFunctionFormat = true
				}
				for _, expr := range exprs {
					// Check for tuples - use Function array
					if lit, ok := expr.(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
						needsFunctionFormat = true
						break
					}
					// Check for identifiers - use Function array
					if _, ok := expr.(*ast.Identifier); ok {
						needsFunctionFormat = true
						break
					}
				}
				if needsFunctionFormat {
					// Render as Function array with alias
					fmt.Fprintf(sb, "%sFunction array (alias %s) (children %d)\n", indent, escapeAlias(n.Alias), 1)
					if len(exprs) > 0 {
						fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
					} else {
						fmt.Fprintf(sb, "%s ExpressionList\n", indent)
					}
					for _, expr := range exprs {
						Node(sb, expr, depth+2)
					}
					return
				}
			}
		}
		fmt.Fprintf(sb, "%sLiteral %s (alias %s)\n", indent, FormatLiteral(e), escapeAlias(n.Alias))
	case *ast.BinaryExpr:
		// Binary expressions become functions with alias
		fnName := OperatorToFunction(e.Op)
		// For || (concat) operator, flatten chained concatenations
		if e.Op == "||" {
			operands := collectConcatOperands(e)
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, escapeAlias(n.Alias), 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(operands))
			for _, op := range operands {
				Node(sb, op, depth+2)
			}
		} else {
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, escapeAlias(n.Alias), 1)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
			Node(sb, e.Left, depth+2)
			Node(sb, e.Right, depth+2)
		}
	case *ast.UnaryExpr:
		// Handle negated numeric literals - output as Literal instead of Function negate
		// For integers, only do this in subquery context (ClickHouse behavior)
		// For floats (especially inf/nan), always do this
		if e.Op == "-" {
			if lit, ok := e.Operand.(*ast.Literal); ok {
				switch lit.Type {
				case ast.LiteralInteger:
					// Only convert to literal in subquery context
					if inSubqueryContext {
						switch val := lit.Value.(type) {
						case int64:
							fmt.Fprintf(sb, "%sLiteral Int64_%d (alias %s)\n", indent, -val, escapeAlias(n.Alias))
							return
						case uint64:
							fmt.Fprintf(sb, "%sLiteral Int64_-%d (alias %s)\n", indent, val, escapeAlias(n.Alias))
							return
						}
					}
				case ast.LiteralFloat:
					// Always convert negated floats to literals (especially for -inf, -nan)
					val := lit.Value.(float64)
					s := FormatFloat(-val)
					fmt.Fprintf(sb, "%sLiteral Float64_%s (alias %s)\n", indent, s, escapeAlias(n.Alias))
					return
				}
			}
		}
		// Unary expressions become functions with alias
		fnName := UnaryOperatorToFunction(e.Op)
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, escapeAlias(n.Alias), 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		Node(sb, e.Operand, depth+2)
	case *ast.FunctionCall:
		// Function calls already handle aliases
		explainFunctionCallWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.Identifier:
		// Identifiers with alias
		fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, e.Name(), escapeAlias(n.Alias))
	case *ast.IntervalExpr:
		// Interval expressions with alias
		explainIntervalExpr(sb, e, n.Alias, indent, depth)
	case *ast.TernaryExpr:
		// Ternary expressions become if functions with alias
		fmt.Fprintf(sb, "%sFunction if (alias %s) (children %d)\n", indent, escapeAlias(n.Alias), 1)
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
		// Array access - show alias only when array is not a literal
		// ClickHouse hides alias when array access is on a literal
		if _, isLit := e.Array.(*ast.Literal); isLit {
			explainArrayAccess(sb, e, indent, depth)
		} else {
			explainArrayAccessWithAlias(sb, e, n.Alias, indent, depth)
		}
	case *ast.TupleAccess:
		// Tuple access with alias
		explainTupleAccessWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.InExpr:
		// IN expressions with alias
		explainInExprWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.CaseExpr:
		// CASE expressions with alias
		explainCaseExprWithAlias(sb, e, n.Alias, indent, depth)
	default:
		// For other types, recursively explain and add alias info
		Node(sb, n.Expr, depth)
	}
}

func explainAsterisk(sb *strings.Builder, n *ast.Asterisk, indent string, depth int) {
	// Check if there are any column transformers (EXCEPT, REPLACE)
	hasTransformers := len(n.Except) > 0 || len(n.Replace) > 0

	if n.Table != "" {
		if hasTransformers {
			fmt.Fprintf(sb, "%sQualifiedAsterisk (children %d)\n", indent, 2)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
			explainColumnsTransformers(sb, n, indent+" ", depth+1)
		} else {
			fmt.Fprintf(sb, "%sQualifiedAsterisk (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		}
	} else {
		if hasTransformers {
			fmt.Fprintf(sb, "%sAsterisk (children %d)\n", indent, 1)
			explainColumnsTransformers(sb, n, indent+" ", depth+1)
		} else {
			fmt.Fprintf(sb, "%sAsterisk\n", indent)
		}
	}
}

func explainColumnsTransformers(sb *strings.Builder, n *ast.Asterisk, indent string, depth int) {
	transformerCount := 0
	if len(n.Except) > 0 {
		transformerCount++
	}
	if len(n.Replace) > 0 {
		transformerCount++
	}

	fmt.Fprintf(sb, "%sColumnsTransformerList (children %d)\n", indent, transformerCount)

	if len(n.Except) > 0 {
		fmt.Fprintf(sb, "%s ColumnsExceptTransformer (children %d)\n", indent, len(n.Except))
		for _, col := range n.Except {
			fmt.Fprintf(sb, "%s  Identifier %s\n", indent, col)
		}
	}

	if len(n.Replace) > 0 {
		fmt.Fprintf(sb, "%s ColumnsReplaceTransformer (children %d)\n", indent, len(n.Replace))
		for _, replace := range n.Replace {
			children := 0
			if replace.Expr != nil {
				children = 1
			}
			fmt.Fprintf(sb, "%s  ColumnsReplaceTransformer::Replacement (children %d)\n", indent, children)
			if replace.Expr != nil {
				// Output the expression without alias - the replacement name is implied
				Node(sb, replace.Expr, depth+3)
			}
		}
	}
}

func explainWithElement(sb *strings.Builder, n *ast.WithElement, indent string, depth int) {
	// For WITH elements, we need to show the underlying expression with the name as alias
	// When name is empty, don't show the alias part
	switch e := n.Query.(type) {
	case *ast.Literal:
		if n.Name != "" {
			fmt.Fprintf(sb, "%sLiteral %s (alias %s)\n", indent, FormatLiteral(e), n.Name)
		} else {
			fmt.Fprintf(sb, "%sLiteral %s\n", indent, FormatLiteral(e))
		}
	case *ast.Identifier:
		if n.Name != "" {
			fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, e.Name(), n.Name)
		} else {
			fmt.Fprintf(sb, "%sIdentifier %s\n", indent, e.Name())
		}
	case *ast.FunctionCall:
		explainFunctionCallWithAlias(sb, e, n.Name, indent, depth)
	case *ast.BinaryExpr:
		// Binary expressions become functions
		fnName := OperatorToFunction(e.Op)
		// For || (concat) operator, flatten chained concatenations
		if e.Op == "||" {
			operands := collectConcatOperands(e)
			if n.Name != "" {
				fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Name, 1)
			} else {
				fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
			}
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(operands))
			for _, op := range operands {
				Node(sb, op, depth+2)
			}
		} else {
			if n.Name != "" {
				fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Name, 1)
			} else {
				fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
			}
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
			Node(sb, e.Left, depth+2)
			Node(sb, e.Right, depth+2)
		}
	case *ast.Subquery:
		// Check if this is "(subquery) AS alias" syntax vs "name AS (subquery)" syntax
		if e.Alias != "" {
			// "(subquery) AS alias" syntax: output Subquery with alias directly
			fmt.Fprintf(sb, "%sSubquery (alias %s) (children 1)\n", indent, e.Alias)
			Node(sb, e.Query, depth+1)
		} else {
			// "name AS (subquery)" syntax: output WithElement wrapping the Subquery
			// The alias/name is not shown in the EXPLAIN AST output
			fmt.Fprintf(sb, "%sWithElement (children 1)\n", indent)
			fmt.Fprintf(sb, "%s Subquery (children 1)\n", indent)
			Node(sb, e.Query, depth+2)
		}
	case *ast.CastExpr:
		explainCastExprWithAlias(sb, e, n.Name, indent, depth)
	default:
		// For other types, just output the expression (alias may be lost)
		Node(sb, n.Query, depth)
	}
}
