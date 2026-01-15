package explain

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/sqlc-dev/doubleclick/ast"
)

// sanitizeUTF8 replaces invalid UTF-8 bytes with the Unicode replacement character (U+FFFD)
// and null bytes with the escape sequence \0.
// This matches ClickHouse's behavior of displaying special bytes in EXPLAIN AST output.
func sanitizeUTF8(s string) string {
	// Check if we need to process at all
	needsProcessing := !utf8.ValidString(s)
	if !needsProcessing {
		for i := 0; i < len(s); i++ {
			if s[i] == 0 {
				needsProcessing = true
				break
			}
		}
	}
	if !needsProcessing {
		return s
	}

	var result strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid byte - write replacement character
			result.WriteRune('\uFFFD')
			i++
		} else if r == 0 {
			// Null byte - write as escape sequence \0
			result.WriteString("\\0")
			i += size
		} else {
			result.WriteRune(r)
			i += size
		}
	}
	return result.String()
}

// escapeAlias escapes backslashes and single quotes in alias names for EXPLAIN output
func escapeAlias(alias string) string {
	// Escape backslashes first, then single quotes
	result := strings.ReplaceAll(alias, "\\", "\\\\")
	result = strings.ReplaceAll(result, "'", "\\'")
	return result
}

func explainIdentifier(sb *strings.Builder, n *ast.Identifier, indent string) {
	name := formatIdentifierName(n)
	if n.Alias != "" {
		fmt.Fprintf(sb, "%sIdentifier %s (alias %s)\n", indent, name, escapeAlias(n.Alias))
	} else {
		fmt.Fprintf(sb, "%sIdentifier %s\n", indent, name)
	}
}

// escapeIdentifierPart escapes backslashes and single quotes in an identifier part
// and sanitizes invalid UTF-8 bytes
func escapeIdentifierPart(s string) string {
	s = sanitizeUTF8(s)
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}

// formatIdentifierName formats an identifier name, handling JSON path notation,
// sanitizing invalid UTF-8 bytes, and escaping special characters
func formatIdentifierName(n *ast.Identifier) string {
	if len(n.Parts) == 0 {
		return ""
	}
	if len(n.Parts) == 1 {
		return escapeIdentifierPart(n.Parts[0])
	}
	result := escapeIdentifierPart(n.Parts[0])
	for _, p := range n.Parts[1:] {
		// JSON path notation: ^fieldname should be formatted as ^`fieldname`
		if strings.HasPrefix(p, "^") {
			result += ".^`" + escapeIdentifierPart(p[1:]) + "`"
		} else {
			result += "." + escapeIdentifierPart(p)
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
			// Check if any element is parenthesized (e.g., ((1), (2)) vs (1, 2))
			// Parenthesized elements mean the tuple should render as Function tuple
			hasParenthesizedElement := false
			hasComplexExpr := false
			for _, e := range exprs {
				// Check for parenthesized literals
				if lit, isLit := e.(*ast.Literal); isLit {
					if lit.Parenthesized {
						hasParenthesizedElement = true
						break
					}
					// Nested tuples that contain only primitive literals are OK
					if lit.Type == ast.LiteralTuple {
						if !containsOnlyPrimitiveLiteralsWithUnary(lit) {
							hasComplexExpr = true
							break
						}
						continue
					}
					// Arrays are always complex in tuple context
					if lit.Type == ast.LiteralArray {
						hasComplexExpr = true
						break
					}
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
			// Single-element tuples (from trailing comma syntax like (1,)) always render as Function tuple
			// Tuples with complex expressions or parenthesized elements also render as Function tuple
			if len(exprs) == 1 || hasComplexExpr || hasParenthesizedElement {
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
			// 3. Contains nested arrays with non-literal expressions OR
			// 4. Contains nested arrays that are empty or contain tuples/non-literals
			shouldUseFunctionArray := false
			hasNestedArrays := false
			nestedArraysNeedFunctionFormat := false

			for _, e := range exprs {
				if lit, ok := e.(*ast.Literal); ok {
					// Parenthesized elements require Function array format
					if lit.Parenthesized {
						shouldUseFunctionArray = true
					}
					if lit.Type == ast.LiteralArray {
						hasNestedArrays = true
						// Check if inner array needs Function array format:
						// - Contains non-literal expressions OR
						// - Contains tuples OR
						// - Is empty OR
						// - Contains empty arrays
						if innerExprs, ok := lit.Value.([]ast.Expression); ok {
							if containsNonLiteralExpressions(innerExprs) ||
								len(innerExprs) == 0 ||
								containsTuples(innerExprs) ||
								containsEmptyArrays(innerExprs) {
								nestedArraysNeedFunctionFormat = true
							}
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
			// - nested arrays that need Function format (contain non-literals, tuples, or empty arrays at any depth)
			// Note: nested arrays that are ALL single-element should still be Literal format
			if hasNestedArrays && nestedArraysNeedFunctionFormat {
				shouldUseFunctionArray = true
			}
			// Also check for empty arrays at any depth within nested arrays
			if hasNestedArrays && containsEmptyArraysRecursive(exprs) {
				shouldUseFunctionArray = true
			}
			// Also check for tuples at any depth within nested arrays
			if hasNestedArrays && containsTuplesRecursive(exprs) {
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

// isSimpleLiteralOrNestedLiteral checks if an expression is a literal (including nested tuples/arrays of literals)
// Returns false for complex expressions like subqueries, function calls, identifiers, etc.
func isSimpleLiteralOrNestedLiteral(e ast.Expression) bool {
	if lit, ok := e.(*ast.Literal); ok {
		// For nested arrays/tuples, recursively check if all elements are also literals
		if lit.Type == ast.LiteralArray || lit.Type == ast.LiteralTuple {
			if exprs, ok := lit.Value.([]ast.Expression); ok {
				for _, elem := range exprs {
					if !isSimpleLiteralOrNestedLiteral(elem) {
						return false
					}
				}
			}
		}
		return true
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
// or parenthesized literals (which need Function array format)
func containsNonLiteralExpressions(exprs []ast.Expression) bool {
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok {
			// Parenthesized literals need Function array format
			if lit.Parenthesized {
				return true
			}
			continue
		}
		// Unary minus of a literal (negative number) is also acceptable
		if unary, ok := e.(*ast.UnaryExpr); ok && unary.Op == "-" {
			if _, ok := unary.Operand.(*ast.Literal); ok {
				continue
			}
		}
		return true
	}
	return false
}

// containsNonLiteralInNested checks if an array or tuple literal contains
// non-literal elements at any nesting level (identifiers, function calls, etc.)
func containsNonLiteralInNested(lit *ast.Literal) bool {
	if lit.Type != ast.LiteralArray && lit.Type != ast.LiteralTuple {
		return false
	}
	exprs, ok := lit.Value.([]ast.Expression)
	if !ok {
		return false
	}
	for _, e := range exprs {
		// Check if this element is a non-literal (identifier, function call, etc.)
		if _, isLit := e.(*ast.Literal); !isLit {
			return true
		}
		// Recursively check nested arrays/tuples
		if innerLit, ok := e.(*ast.Literal); ok {
			if containsNonLiteralInNested(innerLit) {
				return true
			}
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

// containsEmptyArraysRecursive checks if any nested array at any depth is empty
func containsEmptyArraysRecursive(exprs []ast.Expression) bool {
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok && lit.Type == ast.LiteralArray {
			if innerExprs, ok := lit.Value.([]ast.Expression); ok {
				if len(innerExprs) == 0 {
					return true
				}
				// Recursively check nested arrays
				if containsEmptyArraysRecursive(innerExprs) {
					return true
				}
			}
		}
	}
	return false
}

// containsTuplesRecursive checks if any nested array contains tuples at any depth
func containsTuplesRecursive(exprs []ast.Expression) bool {
	for _, e := range exprs {
		if lit, ok := e.(*ast.Literal); ok {
			if lit.Type == ast.LiteralTuple {
				return true
			}
			if lit.Type == ast.LiteralArray {
				if innerExprs, ok := lit.Value.([]ast.Expression); ok {
					if containsTuplesRecursive(innerExprs) {
						return true
					}
				}
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

	// For OR and AND operators, flatten left-associative chains
	// but preserve explicit parenthesization like "(a OR b) OR c"
	if n.Op == "OR" || n.Op == "AND" {
		operands := collectLogicalOperands(n)
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

// collectLogicalOperands flattens chained OR/AND operations into a list of operands,
// but respects explicit parenthesization. For example:
// - "a OR b OR c" → [a, b, c] (flattened)
// - "(a OR b) OR c" → [(a OR b), c] (preserved due to explicit parens)
func collectLogicalOperands(n *ast.BinaryExpr) []ast.Expression {
	var operands []ast.Expression

	// Recursively collect from left side if it's the same operator AND not parenthesized
	if left, ok := n.Left.(*ast.BinaryExpr); ok && left.Op == n.Op && !left.Parenthesized {
		operands = append(operands, collectLogicalOperands(left)...)
	} else {
		operands = append(operands, n.Left)
	}

	// Also flatten right side if it's the same operator and not parenthesized
	// This handles both left-associative and right-associative parsing
	if right, ok := n.Right.(*ast.BinaryExpr); ok && right.Op == n.Op && !right.Parenthesized {
		operands = append(operands, collectLogicalOperands(right)...)
	} else {
		operands = append(operands, n.Right)
	}

	return operands
}

func explainUnaryExpr(sb *strings.Builder, n *ast.UnaryExpr, indent string, depth int) {
	// Handle negate of literal numbers - output as negative literal instead of function
	// BUT only if the literal is NOT parenthesized (e.g., -1 folds, but -(1) stays as negate function)
	if n.Op == "-" {
		if lit, ok := n.Operand.(*ast.Literal); ok && !lit.Parenthesized {
			switch lit.Type {
			case ast.LiteralInteger:
				// Convert positive integer to negative
				switch val := lit.Value.(type) {
				case int64:
					negVal := -val
					// ClickHouse normalizes -0 to UInt64_0
					if negVal == 0 {
						fmt.Fprintf(sb, "%sLiteral UInt64_0\n", indent)
					} else if negVal > 0 {
						fmt.Fprintf(sb, "%sLiteral UInt64_%d\n", indent, negVal)
					} else {
						fmt.Fprintf(sb, "%sLiteral Int64_%d\n", indent, negVal)
					}
					return
				case uint64:
					// ClickHouse normalizes -0 to UInt64_0
					if val == 0 {
						fmt.Fprintf(sb, "%sLiteral UInt64_0\n", indent)
					} else if val <= 9223372036854775808 {
						// Value fits in int64 when negated
						// Note: -9223372036854775808 is int64 min, so 9223372036854775808 is included
						fmt.Fprintf(sb, "%sLiteral Int64_-%d\n", indent, val)
					} else {
						// Value too large for int64 - output as Float64
						f := -float64(val)
						s := FormatFloat(f)
						fmt.Fprintf(sb, "%sLiteral Float64_%s\n", indent, s)
					}
					return
				}
			case ast.LiteralFloat:
				val := lit.Value.(float64)
				s := FormatFloat(-val)
				fmt.Fprintf(sb, "%sLiteral Float64_%s\n", indent, s)
				return
			case ast.LiteralString:
				// Handle BigInt - very large numbers stored as strings
				// ClickHouse converts these to Float64 in scientific notation
				if lit.IsBigInt {
					if strVal, ok := lit.Value.(string); ok {
						// Parse the string as float64 and negate it
						if f, err := strconv.ParseFloat(strVal, 64); err == nil {
							s := FormatFloat(-f)
							fmt.Fprintf(sb, "%sLiteral Float64_%s\n", indent, s)
							return
						}
					}
				}
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
				needsFunctionFormat := false
				// Empty tuples always use Function tuple format
				if len(exprs) == 0 {
					needsFunctionFormat = true
				}
				for _, expr := range exprs {
					if _, isLit := expr.(*ast.Literal); !isLit {
						needsFunctionFormat = true
						break
					}
					// Check if tuple contains array literals - these need Function tuple format
					if lit, ok := expr.(*ast.Literal); ok {
						if lit.Type == ast.LiteralArray {
							needsFunctionFormat = true
							break
						}
						// Also check if nested arrays/tuples contain non-literal elements
						if containsNonLiteralInNested(lit) {
							needsFunctionFormat = true
							break
						}
					}
				}
				if needsFunctionFormat {
					// Render as Function tuple with alias
					fmt.Fprintf(sb, "%sFunction tuple (alias %s) (children %d)\n", indent, escapeAlias(n.Alias), 1)
					// For empty ExpressionList, don't include children count
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
		// Check if this is an array containing specific expressions that need Function array format
		if e.Type == ast.LiteralArray {
			if exprs, ok := e.Value.([]ast.Expression); ok {
				needsFunctionFormat := false
				hasNestedArrays := false
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
					// Check for nested arrays
					if lit, ok := expr.(*ast.Literal); ok && lit.Type == ast.LiteralArray {
						hasNestedArrays = true
						// Check if inner array is empty or contains empty arrays
						if innerExprs, ok := lit.Value.([]ast.Expression); ok {
							if len(innerExprs) == 0 || containsEmptyArrays(innerExprs) {
								needsFunctionFormat = true
								break
							}
						}
					}
					// Check for identifiers - use Function array
					if _, ok := expr.(*ast.Identifier); ok {
						needsFunctionFormat = true
						break
					}
					// Check for function calls - use Function array
					if _, ok := expr.(*ast.FunctionCall); ok {
						needsFunctionFormat = true
						break
					}
					// Check for CAST expressions - use Function array
					if _, ok := expr.(*ast.CastExpr); ok {
						needsFunctionFormat = true
						break
					}
					// Check for binary expressions - use Function array
					if _, ok := expr.(*ast.BinaryExpr); ok {
						needsFunctionFormat = true
						break
					}
					// Check for other non-literal expressions (skip arrays/tuples which are handled separately)
					if lit, ok := expr.(*ast.Literal); !ok {
						// Not a literal - check if it's a unary negation of a number (which is OK)
						if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == "-" {
							if innerLit, ok := unary.Operand.(*ast.Literal); ok {
								if innerLit.Type == ast.LiteralInteger || innerLit.Type == ast.LiteralFloat {
									continue // Negated number is OK
								}
							}
						}
						needsFunctionFormat = true
						break
					} else if lit.Type != ast.LiteralArray && lit.Type != ast.LiteralTuple {
						// Simple literal (not array/tuple) - OK
						continue
					}
					// Arrays and tuples are handled by the earlier checks for nested arrays
				}
				// Also check for empty arrays at any depth within nested arrays
				if hasNestedArrays && containsEmptyArraysRecursive(exprs) {
					needsFunctionFormat = true
				}
				// Also check for tuples at any depth within nested arrays
				if hasNestedArrays && containsTuplesRecursive(exprs) {
					needsFunctionFormat = true
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
		} else if e.Op == "OR" || e.Op == "AND" {
			// For OR and AND operators, flatten but respect explicit parenthesization
			operands := collectLogicalOperands(e)
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
		// When an aliased expression is a negated literal, output as negative Literal
		if e.Op == "-" {
			if lit, ok := e.Operand.(*ast.Literal); ok {
				switch lit.Type {
				case ast.LiteralInteger:
					// Convert negated integer to negative literal
					switch val := lit.Value.(type) {
					case int64:
						fmt.Fprintf(sb, "%sLiteral Int64_%d (alias %s)\n", indent, -val, escapeAlias(n.Alias))
						return
					case uint64:
						if val <= 9223372036854775808 {
							// Value fits in int64 when negated
							// Note: -9223372036854775808 is int64 min, so 9223372036854775808 is included
							fmt.Fprintf(sb, "%sLiteral Int64_-%d (alias %s)\n", indent, val, escapeAlias(n.Alias))
						} else {
							// Value too large for int64 - output as Float64
							f := -float64(val)
							s := FormatFloat(f)
							fmt.Fprintf(sb, "%sLiteral Float64_%s (alias %s)\n", indent, s, escapeAlias(n.Alias))
						}
						return
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
	case *ast.Lambda:
		// Lambda expressions with alias
		explainLambdaWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.ExtractExpr:
		// EXTRACT expressions with alias
		explainExtractExprWithAlias(sb, e, n.Alias, indent, depth)
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
		// CAST expressions always show the alias from the AliasedExpr wrapper
		explainCastExprWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.ArrayAccess:
		// Array access with alias
		explainArrayAccessWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.TupleAccess:
		// Tuple access with alias
		explainTupleAccessWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.InExpr:
		// IN expressions with alias
		explainInExprWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.CaseExpr:
		// CASE expressions with alias
		explainCaseExprWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.ExistsExpr:
		// EXISTS expressions with alias
		explainExistsExprWithAlias(sb, e, n.Alias, indent, depth)
	case *ast.Parameter:
		// QueryParameter with alias
		if e.Name != "" {
			if e.Type != nil {
				fmt.Fprintf(sb, "%sQueryParameter %s:%s (alias %s)\n", indent, e.Name, FormatDataType(e.Type), escapeAlias(n.Alias))
			} else {
				fmt.Fprintf(sb, "%sQueryParameter %s (alias %s)\n", indent, e.Name, escapeAlias(n.Alias))
			}
		} else {
			fmt.Fprintf(sb, "%sQueryParameter (alias %s)\n", indent, escapeAlias(n.Alias))
		}
	default:
		// For other types, recursively explain and add alias info
		Node(sb, n.Expr, depth)
	}
}

func explainAsterisk(sb *strings.Builder, n *ast.Asterisk, indent string, depth int) {
	// Check if there are any column transformers (EXCEPT, REPLACE, APPLY)
	hasTransformers := len(n.Transformers) > 0 || len(n.Except) > 0 || len(n.Replace) > 0 || len(n.Apply) > 0

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
	// Use Transformers if available (preserves order), otherwise fall back to legacy arrays
	if len(n.Transformers) > 0 {
		fmt.Fprintf(sb, "%sColumnsTransformerList (children %d)\n", indent, len(n.Transformers))
		for _, t := range n.Transformers {
			explainSingleTransformer(sb, t, indent, depth)
		}
		return
	}

	// Legacy: use separate arrays (doesn't preserve order)
	transformerCount := 0
	if len(n.Except) > 0 {
		transformerCount++
	}
	if len(n.Replace) > 0 {
		transformerCount++
	}
	// Each APPLY adds one transformer
	transformerCount += len(n.Apply)

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

	// Each APPLY function gets its own ColumnsApplyTransformer
	for range n.Apply {
		fmt.Fprintf(sb, "%s ColumnsApplyTransformer\n", indent)
	}
}

func explainSingleTransformer(sb *strings.Builder, t *ast.ColumnTransformer, indent string, depth int) {
	switch t.Type {
	case "apply":
		fmt.Fprintf(sb, "%s ColumnsApplyTransformer\n", indent)
	case "except":
		// If it's a regex pattern, output without children
		if t.Pattern != "" {
			fmt.Fprintf(sb, "%s ColumnsExceptTransformer\n", indent)
		} else {
			fmt.Fprintf(sb, "%s ColumnsExceptTransformer (children %d)\n", indent, len(t.Except))
			for _, col := range t.Except {
				fmt.Fprintf(sb, "%s  Identifier %s\n", indent, col)
			}
		}
	case "replace":
		fmt.Fprintf(sb, "%s ColumnsReplaceTransformer (children %d)\n", indent, len(t.Replaces))
		for _, replace := range t.Replaces {
			children := 0
			if replace.Expr != nil {
				children = 1
			}
			fmt.Fprintf(sb, "%s  ColumnsReplaceTransformer::Replacement (children %d)\n", indent, children)
			if replace.Expr != nil {
				Node(sb, replace.Expr, depth+3)
			}
		}
	}
}

func explainColumnsMatcher(sb *strings.Builder, n *ast.ColumnsMatcher, indent string, depth int) {
	// Check if there are any column transformers (EXCEPT, REPLACE, APPLY)
	hasTransformers := len(n.Transformers) > 0 || len(n.Except) > 0 || len(n.Replace) > 0 || len(n.Apply) > 0

	// Determine the matcher type based on whether it's a pattern or a list
	if len(n.Columns) > 0 {
		// ColumnsListMatcher for COLUMNS(col1, col2, ...)
		typeName := "ColumnsListMatcher"
		if n.Qualifier != "" {
			typeName = "QualifiedColumnsListMatcher"
		}
		childCount := 1 // ExpressionList of columns
		if n.Qualifier != "" {
			childCount++
		}
		if hasTransformers {
			childCount++ // for ColumnsTransformerList
		}
		fmt.Fprintf(sb, "%s%s (children %d)\n", indent, typeName, childCount)
		if n.Qualifier != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Qualifier)
		}
		// Output the columns as ExpressionList
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Columns))
		for _, col := range n.Columns {
			Node(sb, col, depth+2)
		}
		if hasTransformers {
			explainColumnsMatcherTransformers(sb, n, indent+" ", depth+1)
		}
	} else {
		// ColumnsRegexpMatcher for COLUMNS('pattern')
		typeName := "ColumnsRegexpMatcher"
		if n.Qualifier != "" {
			typeName = "QualifiedColumnsRegexpMatcher"
		}
		if n.Qualifier != "" {
			childCount := 1 // Identifier
			if hasTransformers {
				childCount++
			}
			fmt.Fprintf(sb, "%s%s (children %d)\n", indent, typeName, childCount)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Qualifier)
			if hasTransformers {
				explainColumnsMatcherTransformers(sb, n, indent+" ", depth+1)
			}
		} else {
			if hasTransformers {
				fmt.Fprintf(sb, "%s%s (children %d)\n", indent, typeName, 1)
				explainColumnsMatcherTransformers(sb, n, indent+" ", depth+1)
			} else {
				fmt.Fprintf(sb, "%s%s\n", indent, typeName)
			}
		}
	}
}

func explainColumnsMatcherTransformers(sb *strings.Builder, n *ast.ColumnsMatcher, indent string, depth int) {
	// Use Transformers if available (preserves order), otherwise fall back to legacy arrays
	if len(n.Transformers) > 0 {
		fmt.Fprintf(sb, "%sColumnsTransformerList (children %d)\n", indent, len(n.Transformers))
		for _, t := range n.Transformers {
			explainSingleTransformer(sb, t, indent, depth)
		}
		return
	}

	// Legacy: use separate arrays (doesn't preserve order)
	transformerCount := 0
	if len(n.Except) > 0 {
		transformerCount++
	}
	if len(n.Replace) > 0 {
		transformerCount++
	}
	// Each APPLY adds one transformer
	transformerCount += len(n.Apply)

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

	// Each APPLY function gets its own ColumnsApplyTransformer
	for range n.Apply {
		fmt.Fprintf(sb, "%s ColumnsApplyTransformer\n", indent)
	}
}

func explainWithElement(sb *strings.Builder, n *ast.WithElement, indent string, depth int) {
	// For WITH elements, we need to show the underlying expression with the name as alias
	// When name is empty, don't show the alias part
	switch e := n.Query.(type) {
	case *ast.Literal:
		// Tuples containing complex expressions (subqueries, function calls, etc) should be rendered as Function tuple
		// But tuples of simple literals (including nested tuples of literals) stay as Literal
		if e.Type == ast.LiteralTuple {
			if exprs, ok := e.Value.([]ast.Expression); ok {
				needsFunctionFormat := false
				// Empty tuples always use Function tuple format
				if len(exprs) == 0 {
					needsFunctionFormat = true
				} else {
					for _, expr := range exprs {
						// Check if any element is a truly complex expression (not just a literal)
						if !isSimpleLiteralOrNestedLiteral(expr) {
							needsFunctionFormat = true
							break
						}
					}
				}
				if needsFunctionFormat {
					if n.Name != "" {
						fmt.Fprintf(sb, "%sFunction tuple (alias %s) (children %d)\n", indent, n.Name, 1)
					} else {
						fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
					}
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
		// Arrays containing non-literal expressions should be rendered as Function array
		if e.Type == ast.LiteralArray {
			if exprs, ok := e.Value.([]ast.Expression); ok {
				needsFunctionFormat := false
				for _, elem := range exprs {
					if !isSimpleLiteralOrNegation(elem) {
						needsFunctionFormat = true
						break
					}
				}
				if needsFunctionFormat {
					// Render as Function array with alias
					if n.Name != "" {
						fmt.Fprintf(sb, "%sFunction array (alias %s) (children %d)\n", indent, n.Name, 1)
					} else {
						fmt.Fprintf(sb, "%sFunction array (children %d)\n", indent, 1)
					}
					if len(exprs) > 0 {
						fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(exprs))
					} else {
						fmt.Fprintf(sb, "%s ExpressionList\n", indent)
					}
					for _, elem := range exprs {
						Node(sb, elem, depth+2)
					}
					return
				}
			}
		}
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
	case *ast.Lambda:
		explainLambdaWithAlias(sb, e, n.Name, indent, depth)
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
		} else if e.Op == "OR" || e.Op == "AND" {
			// For OR and AND operators, flatten but respect explicit parenthesization
			operands := collectLogicalOperands(e)
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
	case *ast.ArrayAccess:
		explainArrayAccessWithAlias(sb, e, n.Name, indent, depth)
	case *ast.BetweenExpr:
		explainBetweenExprWithAlias(sb, e, n.Name, indent, depth)
	case *ast.UnaryExpr:
		// For unary minus with numeric literal, output as negative literal with alias
		if e.Op == "-" {
			if lit, ok := e.Operand.(*ast.Literal); ok && (lit.Type == ast.LiteralInteger || lit.Type == ast.LiteralFloat) {
				// Format as negative literal
				negLit := &ast.Literal{
					Position: lit.Position,
					Type:     lit.Type,
					Value:    lit.Value,
				}
				if n.Name != "" {
					fmt.Fprintf(sb, "%sLiteral %s (alias %s)\n", indent, formatNegativeLiteral(negLit), n.Name)
				} else {
					fmt.Fprintf(sb, "%sLiteral %s\n", indent, formatNegativeLiteral(negLit))
				}
				return
			}
		}
		// For other unary expressions, output as function
		fnName := "negate"
		if e.Op == "NOT" {
			fnName = "not"
		}
		if n.Name != "" {
			fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Name, 1)
		} else {
			fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
		}
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
		Node(sb, e.Operand, depth+2)
	case *ast.TernaryExpr:
		// Ternary expressions become if functions with alias
		if n.Name != "" {
			fmt.Fprintf(sb, "%sFunction if (alias %s) (children %d)\n", indent, n.Name, 1)
		} else {
			fmt.Fprintf(sb, "%sFunction if (children %d)\n", indent, 1)
		}
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 3)
		Node(sb, e.Condition, depth+2)
		Node(sb, e.Then, depth+2)
		Node(sb, e.Else, depth+2)
	default:
		// For other types, just output the expression (alias may be lost)
		Node(sb, n.Query, depth)
	}
}
