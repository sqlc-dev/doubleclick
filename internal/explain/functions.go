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
	// Only count WindowDefinition as a child for inline window specs that have content
	// Empty OVER () doesn't produce a WindowDefinition in ClickHouse EXPLAIN AST
	// Named refs like "OVER w" are shown in the SELECT's WINDOW clause instead
	hasNonEmptyWindowSpec := n.Over != nil && n.Over.Name == "" && windowSpecHasContent(n.Over)
	if hasNonEmptyWindowSpec {
		children++ // WindowDefinition for OVER clause
	}
	// Normalize function name
	fnName := NormalizeFunctionName(n.Name)
	// Append "Distinct" if the function has DISTINCT modifier
	if n.Distinct {
		fnName = fnName + "Distinct"
	}
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
		// Also reset the subquery context since view() SELECT is not in a Subquery node
		if strings.ToLower(n.Name) == "view" {
			if sq, ok := arg.(*ast.Subquery); ok {
				prevContext := inSubqueryContext
				inSubqueryContext = false
				Node(sb, sq.Query, depth+2)
				inSubqueryContext = prevContext
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
	// Window definition (for window functions with inline OVER clause)
	// WindowDefinition is a sibling to ExpressionList, so use the same indent
	// Only output for non-empty inline specs, not named references like "OVER w"
	if hasNonEmptyWindowSpec {
		explainWindowSpec(sb, n.Over, indent+" ", depth+1)
	}
}

// windowSpecHasContent returns true if the window spec has any content.
// ClickHouse EXPLAIN AST never includes WindowDefinition nodes for window
// functions, even when OVER clause has PARTITION BY, ORDER BY, or frame specs.
func windowSpecHasContent(w *ast.WindowSpec) bool {
	return false
}

func explainLambda(sb *strings.Builder, n *ast.Lambda, indent string, depth int) {
	explainLambdaWithAlias(sb, n, "", indent, depth)
}

func explainLambdaWithAlias(sb *strings.Builder, n *ast.Lambda, alias string, indent string, depth int) {
	// Lambda is represented as Function lambda with tuple of params and body
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction lambda (alias %s) (children %d)\n", indent, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction lambda (children %d)\n", indent, 1)
	}
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
	useArrayFormat := false
	if n.OperatorSyntax {
		if lit, ok := n.Expr.(*ast.Literal); ok {
			if lit.Type == ast.LiteralArray || lit.Type == ast.LiteralTuple {
				// Determine format based on both content and target type
				useArrayFormat = shouldUseArrayFormat(lit, n.Type)
				hideAlias = !useArrayFormat
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
				if useArrayFormat {
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
		} else if negatedLit := extractNegatedLiteral(n.Expr); negatedLit != "" {
			// Handle negated literal like -0::Int16 -> CAST('-0', 'Int16')
			fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, negatedLit)
		} else {
			// Complex expression - use normal AST node
			Node(sb, n.Expr, depth+2)
		}
	} else {
		Node(sb, n.Expr, depth+2)
	}
	// Type is formatted as a literal string, or as a node if it's a dynamic type expression
	if n.TypeExpr != nil {
		Node(sb, n.TypeExpr, depth+2)
	} else {
		typeStr := FormatDataType(n.Type)
		// Only escape if the DataType doesn't have parameters - this means the entire
		// type was parsed from a string literal and may contain unescaped quotes.
		// If it has parameters, FormatDataType already handles escaping.
		if n.Type == nil || len(n.Type.Parameters) == 0 {
			typeStr = escapeStringLiteral(typeStr)
		}
		fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, typeStr)
	}
}

// shouldUseArrayFormat determines whether to use Array_[...] format or string format
// for array/tuple literals in :: cast expressions.
// This depends on both the literal content and the target type.
func shouldUseArrayFormat(lit *ast.Literal, targetType *ast.DataType) bool {
	// First check if the literal contains only primitive literals (not expressions)
	if !containsOnlyLiterals(lit) {
		return false
	}

	// For arrays of strings, check the target type to determine format
	if lit.Type == ast.LiteralArray && hasStringElements(lit) {
		// Only use Array_ format when casting to Array(String) specifically
		// For other types like Array(JSON), Array(LowCardinality(String)), etc., use string format
		if targetType != nil && strings.ToLower(targetType.Name) == "array" && len(targetType.Parameters) > 0 {
			if innerType, ok := targetType.Parameters[0].(*ast.DataType); ok {
				// Only use Array_ format if inner type is exactly "String" with no parameters
				if strings.ToLower(innerType.Name) == "string" && len(innerType.Parameters) == 0 {
					return true
				}
			}
		}
		// For any other type (JSON, LowCardinality, etc.), use string format
		return false
	}

	// For non-string primitives, always use Array_ format
	return true
}

// containsOnlyLiterals checks if a literal array/tuple contains only literal values (no expressions)
func containsOnlyLiterals(lit *ast.Literal) bool {
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
		// Nested arrays/tuples need recursive check
		if innerLit.Type == ast.LiteralArray || innerLit.Type == ast.LiteralTuple {
			if !containsOnlyLiterals(innerLit) {
				return false
			}
		}
	}
	return true
}

// hasStringElements checks if an array literal contains any string elements
func hasStringElements(lit *ast.Literal) bool {
	if lit.Type != ast.LiteralArray {
		return false
	}
	exprs, ok := lit.Value.([]ast.Expression)
	if !ok {
		return false
	}
	for _, e := range exprs {
		if innerLit, ok := e.(*ast.Literal); ok {
			if innerLit.Type == ast.LiteralString {
				return true
			}
		}
	}
	return false
}

// containsOnlyPrimitives checks if a literal array/tuple contains only primitive literals
// Deprecated: Use shouldUseArrayFormat instead for :: cast expressions
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
		// Nested arrays/tuples need recursive check
		if innerLit.Type == ast.LiteralArray || innerLit.Type == ast.LiteralTuple {
			if !containsOnlyPrimitives(innerLit) {
				return false
			}
		}
	}
	return true
}

// isNumericExpr checks if an expression is a numeric value (literal or unary minus of numeric)
func isNumericExpr(expr ast.Expression) bool {
	if lit, ok := expr.(*ast.Literal); ok {
		return lit.Type == ast.LiteralInteger || lit.Type == ast.LiteralFloat
	}
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == "-" {
		if lit, ok := unary.Operand.(*ast.Literal); ok {
			return lit.Type == ast.LiteralInteger || lit.Type == ast.LiteralFloat
		}
	}
	return false
}

// containsOnlyPrimitiveLiterals checks if a tuple literal contains only primitive literals (recursively)
func containsOnlyPrimitiveLiterals(lit *ast.Literal) bool {
	if lit.Type != ast.LiteralTuple {
		// Non-tuple literals are primitive
		return true
	}
	exprs, ok := lit.Value.([]ast.Expression)
	if !ok {
		return false
	}
	for _, e := range exprs {
		innerLit, ok := e.(*ast.Literal)
		if !ok {
			// Non-literal expression in tuple
			return false
		}
		// Recursively check nested tuples
		if innerLit.Type == ast.LiteralTuple {
			if !containsOnlyPrimitiveLiterals(innerLit) {
				return false
			}
		}
	}
	return true
}

// containsOnlyPrimitiveLiteralsWithUnary is like containsOnlyPrimitiveLiterals but also handles
// unary negation of numeric literals (e.g., -0., -123)
func containsOnlyPrimitiveLiteralsWithUnary(lit *ast.Literal) bool {
	if lit.Type != ast.LiteralTuple {
		// Non-tuple literals are primitive
		return true
	}
	exprs, ok := lit.Value.([]ast.Expression)
	if !ok {
		return false
	}
	for _, e := range exprs {
		// Direct literal
		if innerLit, ok := e.(*ast.Literal); ok {
			// Recursively check nested tuples
			if innerLit.Type == ast.LiteralTuple {
				if !containsOnlyPrimitiveLiteralsWithUnary(innerLit) {
					return false
				}
			}
			// Arrays inside tuples make it complex
			if innerLit.Type == ast.LiteralArray {
				return false
			}
			continue
		}
		// Unary negation of numeric literal is also primitive
		if unary, ok := e.(*ast.UnaryExpr); ok && unary.Op == "-" {
			if innerLit, ok := unary.Operand.(*ast.Literal); ok {
				if innerLit.Type == ast.LiteralInteger || innerLit.Type == ast.LiteralFloat {
					continue
				}
			}
		}
		// Non-literal expression in tuple
		return false
	}
	return true
}

// exprToLiteral converts a numeric expression to a literal (handles unary minus)
func exprToLiteral(expr ast.Expression) *ast.Literal {
	if lit, ok := expr.(*ast.Literal); ok {
		return lit
	}
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == "-" {
		if lit, ok := unary.Operand.(*ast.Literal); ok {
			// Create a new literal with negated value
			switch val := lit.Value.(type) {
			case int64:
				return &ast.Literal{Type: ast.LiteralInteger, Value: -val}
			case uint64:
				// Convert to int64 and negate
				return &ast.Literal{Type: ast.LiteralInteger, Value: -int64(val)}
			case float64:
				return &ast.Literal{Type: ast.LiteralFloat, Value: -val}
			}
		}
	}
	return nil
}

// extractNegatedLiteral checks if expr is a negated literal (like -0, -12)
// and returns its string representation (like "-0", "-12") for :: cast expressions.
// Returns empty string if not a negated literal.
func extractNegatedLiteral(expr ast.Expression) string {
	unary, ok := expr.(*ast.UnaryExpr)
	if !ok || unary.Op != "-" {
		return ""
	}
	lit, ok := unary.Operand.(*ast.Literal)
	if !ok {
		return ""
	}
	switch lit.Type {
	case ast.LiteralInteger:
		return "-" + formatExprAsString(lit)
	case ast.LiteralFloat:
		return "-" + formatExprAsString(lit)
	}
	return ""
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
	// This happens when we have multiple literals of compatible types:
	// - All numeric literals/expressions (integers/floats, including unary minus)
	// - All string literals (only for small lists, max 10 items)
	// - All tuple literals that contain only primitive literals (recursively)
	canBeTupleLiteral := false
	// Only combine strings into tuple for small lists (up to 10 items)
	// Large string lists are kept as separate children in ClickHouse EXPLAIN AST
	const maxStringTupleSize = 10
	if n.Query == nil && len(n.List) > 1 {
		allNumeric := true
		allStrings := true
		allTuples := true
		allTuplesArePrimitive := true
		for _, item := range n.List {
			if lit, ok := item.(*ast.Literal); ok {
				if lit.Type != ast.LiteralInteger && lit.Type != ast.LiteralFloat {
					allNumeric = false
				}
				if lit.Type != ast.LiteralString {
					allStrings = false
				}
				if lit.Type != ast.LiteralTuple {
					allTuples = false
				} else {
					// Check if this tuple contains only primitive literals
					if !containsOnlyPrimitiveLiterals(lit) {
						allTuplesArePrimitive = false
					}
				}
			} else if isNumericExpr(item) {
				// Unary minus of numeric is still numeric
				allStrings = false
				allTuples = false
			} else {
				allNumeric = false
				allStrings = false
				allTuples = false
				break
			}
		}
		// For strings, only combine if list is small enough
		// For tuples, only combine if all contain primitive literals
		canBeTupleLiteral = allNumeric || (allStrings && len(n.List) <= maxStringTupleSize) || (allTuples && allTuplesArePrimitive)
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
			// Check if all items are string literals (large list case - no wrapper)
			allStringLiterals := true
			for _, item := range n.List {
				if lit, ok := item.(*ast.Literal); !ok || lit.Type != ast.LiteralString {
					allStringLiterals = false
					break
				}
			}
			if allStringLiterals {
				// Large string list - separate children
				argCount += len(n.List)
			} else {
				// Non-string items get wrapped in a single Function tuple
				argCount++
			}
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
		// Check if all items are tuple literals (some may have expressions)
		allTuples := true
		for _, item := range n.List {
			if lit, ok := item.(*ast.Literal); !ok || lit.Type != ast.LiteralTuple {
				allTuples = false
				break
			}
		}
		if allTuples {
			// Wrap all tuples in Function tuple
			fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.List))
			for _, item := range n.List {
				explainTupleInInList(sb, item.(*ast.Literal), indent+"   ", depth+4)
			}
		} else {
			// Check if all items are string literals (large list case)
			allStringLiterals := true
			for _, item := range n.List {
				if lit, ok := item.(*ast.Literal); !ok || lit.Type != ast.LiteralString {
					allStringLiterals = false
					break
				}
			}
			if allStringLiterals {
				// Large string list - output as separate children (no tuple wrapper)
				for _, item := range n.List {
					Node(sb, item, depth+2)
				}
			} else {
				// Wrap non-literal/non-tuple list items in Function tuple
				fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.List))
				for _, item := range n.List {
					Node(sb, item, depth+4)
				}
			}
		}
	}
}

// explainTupleInInList renders a tuple in an IN list - either as Literal or Function tuple
func explainTupleInInList(sb *strings.Builder, lit *ast.Literal, indent string, depth int) {
	if containsOnlyPrimitiveLiterals(lit) {
		// All primitives - render as Literal Tuple_
		fmt.Fprintf(sb, "%s Literal %s\n", indent, FormatLiteral(lit))
	} else {
		// Contains expressions - render as Function tuple
		exprs, ok := lit.Value.([]ast.Expression)
		if !ok {
			fmt.Fprintf(sb, "%s Literal %s\n", indent, FormatLiteral(lit))
			return
		}
		fmt.Fprintf(sb, "%s Function tuple (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(exprs))
		for _, e := range exprs {
			Node(sb, e, depth+2)
		}
	}
}

func explainInExprWithAlias(sb *strings.Builder, n *ast.InExpr, alias string, indent string, depth int) {
	// IN is represented as Function in with alias
	fnName := "in"
	if n.Not {
		fnName = "notIn"
	}
	if n.Global {
		fnName = "global" + strings.Title(fnName)
	}
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	}

	// Determine if the IN list should be combined into a single tuple literal
	// Only combine strings into tuple for small lists (up to 10 items)
	const maxStringTupleSizeWithAlias = 10
	canBeTupleLiteral := false
	if n.Query == nil && len(n.List) > 1 {
		allNumeric := true
		allStrings := true
		allTuples := true
		allTuplesArePrimitive := true
		for _, item := range n.List {
			if lit, ok := item.(*ast.Literal); ok {
				if lit.Type != ast.LiteralInteger && lit.Type != ast.LiteralFloat {
					allNumeric = false
				}
				if lit.Type != ast.LiteralString {
					allStrings = false
				}
				if lit.Type != ast.LiteralTuple {
					allTuples = false
				} else {
					if !containsOnlyPrimitiveLiterals(lit) {
						allTuplesArePrimitive = false
					}
				}
			} else if isNumericExpr(item) {
				allStrings = false
				allTuples = false
			} else {
				allNumeric = false
				allStrings = false
				allTuples = false
				break
			}
		}
		canBeTupleLiteral = allNumeric || (allStrings && len(n.List) <= maxStringTupleSizeWithAlias) || (allTuples && allTuplesArePrimitive)
	}

	// Count arguments
	argCount := 1
	if n.Query != nil {
		argCount++
	} else if canBeTupleLiteral {
		argCount++
	} else {
		if len(n.List) == 1 {
			if lit, ok := n.List[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
				argCount++
			} else {
				argCount += len(n.List)
			}
		} else {
			// Check if all items are string literals (large list case - no wrapper)
			allStringLiterals := true
			for _, item := range n.List {
				if lit, ok := item.(*ast.Literal); !ok || lit.Type != ast.LiteralString {
					allStringLiterals = false
					break
				}
			}
			if allStringLiterals {
				// Large string list - separate children
				argCount += len(n.List)
			} else {
				// Non-string items get wrapped in a single Function tuple
				argCount++
			}
		}
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
	Node(sb, n.Expr, depth+2)

	if n.Query != nil {
		fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
		Node(sb, n.Query, depth+3)
	} else if canBeTupleLiteral {
		tupleLit := &ast.Literal{
			Type:  ast.LiteralTuple,
			Value: n.List,
		}
		fmt.Fprintf(sb, "%s  Literal %s\n", indent, FormatLiteral(tupleLit))
	} else if len(n.List) == 1 {
		if lit, ok := n.List[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
			fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, 1)
			Node(sb, n.List[0], depth+4)
		} else {
			Node(sb, n.List[0], depth+2)
		}
	} else {
		// Check if all items are tuple literals (some may have expressions)
		allTuples := true
		for _, item := range n.List {
			if lit, ok := item.(*ast.Literal); !ok || lit.Type != ast.LiteralTuple {
				allTuples = false
				break
			}
		}
		if allTuples {
			// Wrap all tuples in Function tuple
			fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.List))
			for _, item := range n.List {
				explainTupleInInList(sb, item.(*ast.Literal), indent+"   ", depth+4)
			}
		} else {
			// Check if all items are string literals (large list case)
			allStringLiterals := true
			for _, item := range n.List {
				if lit, ok := item.(*ast.Literal); !ok || lit.Type != ast.LiteralString {
					allStringLiterals = false
					break
				}
			}
			if allStringLiterals {
				// Large string list - output as separate children (no tuple wrapper)
				for _, item := range n.List {
					Node(sb, item, depth+2)
				}
			} else {
				// Wrap non-literal/non-tuple list items in Function tuple
				fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.List))
				for _, item := range n.List {
					Node(sb, item, depth+4)
				}
			}
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
	if n.Alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, n.Alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	}
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
	// Only output alias if it's unquoted (ClickHouse doesn't show quoted aliases)
	alias := ""
	if n.Alias != "" && !n.QuotedAlias {
		alias = n.Alias
	}
	explainCaseExprWithAlias(sb, n, alias, indent, depth)
}

func explainCaseExprWithAlias(sb *strings.Builder, n *ast.CaseExpr, alias string, indent string, depth int) {
	// CASE is represented as Function multiIf or caseWithExpression
	if n.Operand != nil {
		// CASE x WHEN ... form
		argCount := 1 + len(n.Whens)*2 // operand + (condition, result) pairs
		if n.Else != nil {
			argCount++
		}
		if alias != "" {
			fmt.Fprintf(sb, "%sFunction caseWithExpression (alias %s) (children %d)\n", indent, alias, 1)
		} else {
			fmt.Fprintf(sb, "%sFunction caseWithExpression (children %d)\n", indent, 1)
		}
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
		// CASE without ELSE implicitly has NULL as the else value
		argCount := len(n.Whens)*2 + 1 // Always add 1 for ELSE (explicit or implicit NULL)
		if alias != "" {
			fmt.Fprintf(sb, "%sFunction multiIf (alias %s) (children %d)\n", indent, alias, 1)
		} else {
			fmt.Fprintf(sb, "%sFunction multiIf (children %d)\n", indent, 1)
		}
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)
		for _, w := range n.Whens {
			Node(sb, w.Condition, depth+2)
			Node(sb, w.Result, depth+2)
		}
		if n.Else != nil {
			Node(sb, n.Else, depth+2)
		} else {
			// Implicit NULL when no ELSE clause
			fmt.Fprintf(sb, "%s  Literal NULL\n", indent)
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
	explainExtractExprWithAlias(sb, n, n.Alias, indent, depth)
}

func explainExtractExprWithAlias(sb *strings.Builder, n *ast.ExtractExpr, alias string, indent string, depth int) {
	// EXTRACT is represented as Function toYear, toMonth, etc.
	// ClickHouse uses specific function names for date/time extraction
	fnName := extractFieldToFunction(n.Field)
	// Use alias from parameter, or fall back to expression's alias
	effectiveAlias := alias
	if effectiveAlias == "" {
		effectiveAlias = n.Alias
	}
	if effectiveAlias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, effectiveAlias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	Node(sb, n.From, depth+2)
}

// extractFieldToFunction maps EXTRACT field names to ClickHouse function names
func extractFieldToFunction(field string) string {
	switch strings.ToUpper(field) {
	case "DAY":
		return "toDayOfMonth"
	case "MONTH":
		return "toMonth"
	case "YEAR", "YYYY":
		return "toYear"
	case "SECOND":
		return "toSecond"
	case "MINUTE":
		return "toMinute"
	case "HOUR":
		return "toHour"
	case "QUARTER":
		return "toQuarter"
	case "WEEK":
		return "toWeek"
	default:
		// Fallback to generic "to" + TitleCase(field)
		return "to" + strings.Title(strings.ToLower(field))
	}
}

func explainWindowSpec(sb *strings.Builder, n *ast.WindowSpec, indent string, depth int) {
	// Window spec is represented as WindowDefinition
	// For simple cases like OVER (), just output WindowDefinition without children
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
	// Count frame offset as child if present
	if n.Frame != nil && n.Frame.StartBound != nil && n.Frame.StartBound.Offset != nil {
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
		// Frame start offset
		if n.Frame != nil && n.Frame.StartBound != nil && n.Frame.StartBound.Offset != nil {
			Node(sb, n.Frame.StartBound.Offset, depth+1)
		}
	} else {
		fmt.Fprintf(sb, "%sWindowDefinition\n", indent)
	}
}
