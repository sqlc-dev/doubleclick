package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// normalizeIntervalUnit converts interval units to title-cased singular form
// e.g., "years" -> "Year", "MONTH" -> "Month", "days" -> "Day"
func normalizeIntervalUnit(unit string) string {
	if len(unit) == 0 {
		return ""
	}
	u := strings.ToLower(unit)
	// Remove trailing 's' for plural forms
	if strings.HasSuffix(u, "s") && len(u) > 1 {
		u = u[:len(u)-1]
	}
	// Title-case
	return strings.ToUpper(u[:1]) + u[1:]
}

func explainFunctionCall(sb *strings.Builder, n *ast.FunctionCall, indent string, depth int) {
	explainFunctionCallWithAlias(sb, n, n.Alias, indent, depth)
}

func explainFunctionCallWithAlias(sb *strings.Builder, n *ast.FunctionCall, alias string, indent string, depth int) {
	// Handle special function transformations that ClickHouse does internally
	if handled := handleSpecialFunction(sb, n, alias, indent, depth); handled {
		return
	}

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

// handleSpecialFunction handles special function transformations that ClickHouse does internally.
// Returns true if the function was handled, false otherwise.
func handleSpecialFunction(sb *strings.Builder, n *ast.FunctionCall, alias string, indent string, depth int) bool {
	fnName := strings.ToUpper(n.Name)

	// Handle quantified comparison operators (ANY/ALL with comparison operators)
	if handled := handleQuantifiedComparison(sb, n, alias, indent, depth); handled {
		return true
	}

	// POSITION('ll' IN 'Hello') -> position('Hello', 'll')
	if fnName == "POSITION" && len(n.Arguments) == 1 {
		if inExpr, ok := n.Arguments[0].(*ast.InExpr); ok {
			// Transform: POSITION(needle IN haystack) -> position(haystack, needle)
			explainPositionWithIn(sb, inExpr.Expr, inExpr.List[0], alias, indent, depth)
			return true
		}
	}

	// DATE_ADD/DATEADD/TIMESTAMP_ADD/TIMESTAMPADD
	if fnName == "DATE_ADD" || fnName == "DATEADD" || fnName == "TIMESTAMP_ADD" || fnName == "TIMESTAMPADD" {
		return handleDateAddSub(sb, n, alias, indent, depth, "plus")
	}

	// DATE_SUB/DATESUB/TIMESTAMP_SUB/TIMESTAMPSUB
	if fnName == "DATE_SUB" || fnName == "DATESUB" || fnName == "TIMESTAMP_SUB" || fnName == "TIMESTAMPSUB" {
		return handleDateAddSub(sb, n, alias, indent, depth, "minus")
	}

	// DATE_DIFF/DATEDIFF
	if fnName == "DATE_DIFF" || fnName == "DATEDIFF" {
		return handleDateDiff(sb, n, alias, indent, depth)
	}

	// TRIM functions with empty string as trim characters - simplify to just the string
	// Only for SQL standard syntax: trim(LEADING '' FROM 'foo') -> just 'foo'
	// Direct function calls like trimLeft('foo', '') are NOT simplified
	if n.SQLStandard && (fnName == "TRIM" || fnName == "LTRIM" || fnName == "RTRIM" ||
		fnName == "TRIMLEFT" || fnName == "TRIMRIGHT" || fnName == "TRIMBOTH") {
		if len(n.Arguments) == 2 {
			if lit, ok := n.Arguments[1].(*ast.Literal); ok {
				if lit.Type == ast.LiteralString && lit.Value == "" {
					// Trim with empty string is a no-op, just output the original string
					Node(sb, n.Arguments[0], depth)
					return true
				}
			}
		}
	}

	return false
}

// handleQuantifiedComparison handles ANY/ALL with comparison operators
// Returns true if the function was handled, false otherwise.
func handleQuantifiedComparison(sb *strings.Builder, n *ast.FunctionCall, alias string, indent string, depth int) bool {
	fnName := strings.ToLower(n.Name)

	// Check if this is a quantified comparison function
	var modifier, op string
	if strings.HasPrefix(fnName, "any") {
		modifier = "any"
		op = fnName[3:]
	} else if strings.HasPrefix(fnName, "all") {
		modifier = "all"
		op = fnName[3:]
	} else {
		return false
	}

	// Must have exactly 2 arguments: left expr and subquery
	if len(n.Arguments) != 2 {
		return false
	}

	subquery, ok := n.Arguments[1].(*ast.Subquery)
	if !ok {
		return false
	}

	// Handle based on the operator and modifier
	switch op {
	case "equals":
		if modifier == "any" {
			// x == ANY (subquery) -> in(x, subquery)
			return false // Let NormalizeFunctionName handle this
		}
		// x == ALL (subquery) -> complex with singleValueOrNull
		outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "in", "singleValueOrNull", alias, indent, depth)
		return true

	case "notequals":
		if modifier == "all" {
			// x != ALL (subquery) -> notIn(x, subquery)
			return false // Let NormalizeFunctionName handle this
		}
		// x != ANY (subquery) -> complex notIn with singleValueOrNull
		outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "notIn", "singleValueOrNull", alias, indent, depth)
		return true

	case "less":
		if modifier == "any" {
			// x < ANY (subquery) -> x < max(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "less", "max", alias, indent, depth)
		} else {
			// x < ALL (subquery) -> x < min(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "less", "min", alias, indent, depth)
		}
		return true

	case "lessorequals":
		if modifier == "any" {
			// x <= ANY (subquery) -> x <= max(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "lessOrEquals", "max", alias, indent, depth)
		} else {
			// x <= ALL (subquery) -> x <= min(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "lessOrEquals", "min", alias, indent, depth)
		}
		return true

	case "greater":
		if modifier == "any" {
			// x > ANY (subquery) -> x > min(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "greater", "min", alias, indent, depth)
		} else {
			// x > ALL (subquery) -> x > max(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "greater", "max", alias, indent, depth)
		}
		return true

	case "greaterorequals":
		if modifier == "any" {
			// x >= ANY (subquery) -> x >= min(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "greaterOrEquals", "min", alias, indent, depth)
		} else {
			// x >= ALL (subquery) -> x >= max(subquery)
			outputQuantifiedWithAggregate(sb, n.Arguments[0], subquery, "greaterOrEquals", "max", alias, indent, depth)
		}
		return true
	}

	return false
}

// outputQuantifiedWithAggregate outputs the ClickHouse AST format for quantified comparisons
// with an aggregate function wrapped around the subquery
func outputQuantifiedWithAggregate(sb *strings.Builder, left ast.Expression, subquery *ast.Subquery, compFunc, aggFunc string, alias string, indent string, depth int) {
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, compFunc, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, compFunc, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, left, depth+2)

	// Output the subquery wrapped with aggregate function
	// Structure: Subquery -> SelectWithUnionQuery -> ExpressionList -> SelectQuery with 4 children
	fmt.Fprintf(sb, "%s  Subquery (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s   SelectWithUnionQuery (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s    ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s     SelectQuery (children %d)\n", indent, 4)

	// First ExpressionList with aggregate function
	fmt.Fprintf(sb, "%s      ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s       Function %s (children %d)\n", indent, aggFunc, 1)
	fmt.Fprintf(sb, "%s        ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s         Asterisk\n", indent)

	// First TablesInSelectQuery - wrap the original subquery
	fmt.Fprintf(sb, "%s      TablesInSelectQuery (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s       TablesInSelectQueryElement (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s        TableExpression (children %d)\n", indent, 1)
	Node(sb, subquery, depth+9)

	// Second ExpressionList with aggregate function (repeated)
	fmt.Fprintf(sb, "%s      ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s       Function %s (children %d)\n", indent, aggFunc, 1)
	fmt.Fprintf(sb, "%s        ExpressionList (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s         Asterisk\n", indent)

	// Second TablesInSelectQuery (repeated)
	fmt.Fprintf(sb, "%s      TablesInSelectQuery (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s       TablesInSelectQueryElement (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s        TableExpression (children %d)\n", indent, 1)
	Node(sb, subquery, depth+9)
}

// explainPositionWithIn outputs POSITION(needle IN haystack) as position(haystack, needle)
func explainPositionWithIn(sb *strings.Builder, needle, haystack ast.Expression, alias string, indent string, depth int) {
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction position (alias %s) (children %d)\n", indent, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction position (children %d)\n", indent, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	// Arguments are swapped: haystack first, then needle
	Node(sb, haystack, depth+2)
	Node(sb, needle, depth+2)
}

// handleDateAddSub handles DATE_ADD/DATE_SUB and variants
// opFunc is "plus" for ADD or "minus" for SUB
func handleDateAddSub(sb *strings.Builder, n *ast.FunctionCall, alias string, indent string, depth int, opFunc string) bool {
	if len(n.Arguments) == 3 {
		// DATE_ADD(unit, n, date) -> plus/minus(date, toIntervalUnit(n))
		unitArg := n.Arguments[0]
		valueArg := n.Arguments[1]
		dateArg := n.Arguments[2]

		// Extract unit from identifier
		unitName := ""
		if ident, ok := unitArg.(*ast.Identifier); ok {
			unitName = ident.Name()
		}

		if unitName != "" {
			explainDateAddSubResult(sb, opFunc, dateArg, valueArg, unitName, alias, indent, depth)
			return true
		}
	} else if len(n.Arguments) == 2 {
		// DATE_ADD(interval, date) -> plus(interval, date)
		// DATE_SUB(date, interval) -> minus(date, interval)
		intervalArg := n.Arguments[0]
		dateArg := n.Arguments[1]

		// Check which argument is the interval
		if _, ok := intervalArg.(*ast.IntervalExpr); ok {
			// Interval first: plus(interval, date)
			explainDateAddSubWithInterval(sb, opFunc, intervalArg, dateArg, alias, indent, depth)
			return true
		}
		// Check if first arg is already a toInterval function (from parser)
		if fc, ok := intervalArg.(*ast.FunctionCall); ok && strings.HasPrefix(strings.ToLower(fc.Name), "tointerval") {
			// Interval first: plus(interval, date)
			explainDateAddSubWithInterval(sb, opFunc, intervalArg, dateArg, alias, indent, depth)
			return true
		}

		// DATE_SUB(date, interval) -> minus(date, interval)
		if _, ok := dateArg.(*ast.IntervalExpr); ok {
			explainDateAddSubWithInterval(sb, opFunc, intervalArg, dateArg, alias, indent, depth)
			return true
		}
		if fc, ok := dateArg.(*ast.FunctionCall); ok && strings.HasPrefix(strings.ToLower(fc.Name), "tointerval") {
			explainDateAddSubWithInterval(sb, opFunc, intervalArg, dateArg, alias, indent, depth)
			return true
		}
	}

	return false
}

// explainDateAddSubResult outputs the transformed DATE_ADD/SUB with unit syntax
func explainDateAddSubResult(sb *strings.Builder, opFunc string, dateArg, valueArg ast.Expression, unit string, alias string, indent string, depth int) {
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, opFunc, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, opFunc, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)

	// First arg: date
	Node(sb, dateArg, depth+2)

	// Second arg: toIntervalUnit(value)
	unitNorm := normalizeIntervalUnit(unit)
	fmt.Fprintf(sb, "%s  Function toInterval%s (children %d)\n", indent, unitNorm, 1)
	fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, 1)
	Node(sb, valueArg, depth+4)
}

// explainDateAddSubWithInterval outputs the transformed DATE_ADD/SUB with INTERVAL syntax
func explainDateAddSubWithInterval(sb *strings.Builder, opFunc string, arg1, arg2 ast.Expression, alias string, indent string, depth int) {
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, opFunc, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, opFunc, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 2)
	Node(sb, arg1, depth+2)
	Node(sb, arg2, depth+2)
}

// handleDateDiff handles DATE_DIFF/DATEDIFF
// DATE_DIFF(unit, date1, date2[, timezone]) -> dateDiff('unit', date1, date2[, timezone])
func handleDateDiff(sb *strings.Builder, n *ast.FunctionCall, alias string, indent string, depth int) bool {
	if len(n.Arguments) < 3 || len(n.Arguments) > 4 {
		return false
	}

	unitArg := n.Arguments[0]
	date1Arg := n.Arguments[1]
	date2Arg := n.Arguments[2]

	// Extract unit from identifier
	unitName := ""
	if ident, ok := unitArg.(*ast.Identifier); ok {
		unitName = ident.Name()
	}

	if unitName == "" {
		return false
	}

	argCount := 3
	if len(n.Arguments) == 4 {
		argCount = 4
	}

	if alias != "" {
		fmt.Fprintf(sb, "%sFunction dateDiff (alias %s) (children %d)\n", indent, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction dateDiff (children %d)\n", indent, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, argCount)

	// First arg: unit as lowercase string literal
	fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, strings.ToLower(unitName))

	// Second and third args: dates
	Node(sb, date1Arg, depth+2)
	Node(sb, date2Arg, depth+2)

	// Fourth arg: optional timezone
	if len(n.Arguments) == 4 {
		Node(sb, n.Arguments[3], depth+2)
	}

	return true
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
	// When there are no parameters, ClickHouse omits the (children N) part
	if len(n.Parameters) > 0 {
		fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.Parameters))
		for _, p := range n.Parameters {
			fmt.Fprintf(sb, "%s    Identifier %s\n", indent, p)
		}
	} else {
		fmt.Fprintf(sb, "%s   ExpressionList\n", indent)
	}
	// Body
	Node(sb, n.Body, depth+2)
}

func explainCastExpr(sb *strings.Builder, n *ast.CastExpr, indent string, depth int) {
	explainCastExprWithAlias(sb, n, n.Alias, indent, depth)
}

func explainCastExprWithAlias(sb *strings.Builder, n *ast.CastExpr, alias string, indent string, depth int) {
	// For :: operator syntax with arrays/tuples, determine formatting based on content
	useArrayFormat := false
	if n.OperatorSyntax {
		if lit, ok := n.Expr.(*ast.Literal); ok {
			if lit.Type == ast.LiteralArray || lit.Type == ast.LiteralTuple {
				// Determine format based on both content and target type
				useArrayFormat = shouldUseArrayFormat(lit, n.Type)
			}
		}
	}
	// Alias is always shown for :: cast syntax with arrays/tuples
	hideAlias := false

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
			} else if lit.Type == ast.LiteralNull {
				// NULL stays as Literal NULL, not formatted as a string
				fmt.Fprintf(sb, "%s  Literal NULL\n", indent)
			} else {
				// Simple literal - format as string (escape special chars for string literals)
				exprStr := formatExprAsString(lit)
				if lit.Type == ast.LiteralString {
					exprStr = escapeStringLiteral(exprStr)
				}
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
// ClickHouse uses different formats depending on element types:
// - Boolean arrays: Array_[Bool_0, Bool_1] format
// - Numeric arrays: '[1, 2, 3]' string format
func shouldUseArrayFormat(lit *ast.Literal, targetType *ast.DataType) bool {
	// First check if the literal contains only primitive literals (not expressions)
	if !containsOnlyLiterals(lit) {
		return false
	}

	// Check if array contains boolean elements - these use Array_ format
	if containsBooleanElements(lit) {
		return true
	}

	// Check if array contains NULL elements - these use Array_ format
	if containsNullElements(lit) {
		return true
	}

	// For arrays of strings, always use string format in :: casts
	// This applies to all target types including Array(String)
	if lit.Type == ast.LiteralArray && hasStringElements(lit) {
		return false
	}

	// For numeric primitives, use string format in :: casts
	return false
}

// containsNullElements checks if a literal array/tuple contains NULL elements
func containsNullElements(lit *ast.Literal) bool {
	var exprs []ast.Expression
	switch lit.Type {
	case ast.LiteralArray, ast.LiteralTuple:
		var ok bool
		exprs, ok = lit.Value.([]ast.Expression)
		if !ok {
			return false
		}
	default:
		return false
	}

	for _, e := range exprs {
		innerLit, ok := e.(*ast.Literal)
		if !ok {
			continue
		}
		if innerLit.Type == ast.LiteralNull {
			return true
		}
		// Check nested arrays/tuples
		if innerLit.Type == ast.LiteralArray || innerLit.Type == ast.LiteralTuple {
			if containsNullElements(innerLit) {
				return true
			}
		}
	}
	return false
}

// containsBooleanElements checks if a literal array/tuple contains boolean elements
func containsBooleanElements(lit *ast.Literal) bool {
	var exprs []ast.Expression
	switch lit.Type {
	case ast.LiteralArray, ast.LiteralTuple:
		var ok bool
		exprs, ok = lit.Value.([]ast.Expression)
		if !ok {
			return false
		}
	default:
		return false
	}

	for _, e := range exprs {
		innerLit, ok := e.(*ast.Literal)
		if !ok {
			continue
		}
		if innerLit.Type == ast.LiteralBoolean {
			return true
		}
		// Check nested arrays/tuples
		if innerLit.Type == ast.LiteralArray || innerLit.Type == ast.LiteralTuple {
			if containsBooleanElements(innerLit) {
				return true
			}
		}
	}
	return false
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
	// - All numeric literals/expressions (integers/floats, including unary minus) + NULLs
	// - All string literals + NULLs
	// - All boolean literals + NULLs
	// - All tuple literals that contain only primitive literals (recursively)
	canBeTupleLiteral := false
	if n.Query == nil && len(n.List) > 1 {
		allNumericOrNull := true
		allStringsOrNull := true
		allBooleansOrNull := true
		allTuples := true
		allTuplesArePrimitive := true
		hasNonNull := false // Need at least one non-null value
		for _, item := range n.List {
			if lit, ok := item.(*ast.Literal); ok {
				if lit.Type == ast.LiteralNull {
					// NULL is compatible with all literal type lists
					continue
				}
				hasNonNull = true
				if lit.Type != ast.LiteralInteger && lit.Type != ast.LiteralFloat {
					allNumericOrNull = false
				}
				if lit.Type != ast.LiteralString {
					allStringsOrNull = false
				}
				if lit.Type != ast.LiteralBoolean {
					allBooleansOrNull = false
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
				hasNonNull = true
				allStringsOrNull = false
				allBooleansOrNull = false
				allTuples = false
			} else {
				allNumericOrNull = false
				allStringsOrNull = false
				allBooleansOrNull = false
				allTuples = false
				break
			}
		}
		// For tuples, only combine if all contain primitive literals
		canBeTupleLiteral = hasNonNull && (allNumericOrNull || allStringsOrNull || allBooleansOrNull || (allTuples && allTuplesArePrimitive))
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
			// Non-string items get wrapped in a single Function tuple
			argCount++
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
			// Wrap non-literal/non-tuple list items in Function tuple
			fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.List))
			for _, item := range n.List {
				Node(sb, item, depth+4)
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
		allNumericOrNull := true
		allStringsOrNull := true
		allBooleansOrNull := true
		allTuples := true
		allTuplesArePrimitive := true
		hasNonNull := false // Need at least one non-null value
		for _, item := range n.List {
			if lit, ok := item.(*ast.Literal); ok {
				if lit.Type == ast.LiteralNull {
					// NULL is compatible with all literal type lists
					continue
				}
				hasNonNull = true
				if lit.Type != ast.LiteralInteger && lit.Type != ast.LiteralFloat {
					allNumericOrNull = false
				}
				if lit.Type != ast.LiteralString {
					allStringsOrNull = false
				}
				if lit.Type != ast.LiteralBoolean {
					allBooleansOrNull = false
				}
				if lit.Type != ast.LiteralTuple {
					allTuples = false
				} else {
					if !containsOnlyPrimitiveLiterals(lit) {
						allTuplesArePrimitive = false
					}
				}
			} else if isNumericExpr(item) {
				hasNonNull = true
				allStringsOrNull = false
				allBooleansOrNull = false
				allTuples = false
			} else {
				allNumericOrNull = false
				allStringsOrNull = false
				allBooleansOrNull = false
				allTuples = false
				break
			}
		}
		canBeTupleLiteral = hasNonNull && (allNumericOrNull || (allStringsOrNull && len(n.List) <= maxStringTupleSizeWithAlias) || allBooleansOrNull || (allTuples && allTuplesArePrimitive))
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
			// Wrap non-literal/non-tuple list items in Function tuple
			fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.List))
			for _, item := range n.List {
				Node(sb, item, depth+4)
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
	// Unit needs to be title-cased and singular (e.g., YEAR -> Year, YEARS -> Year)
	unit := n.Unit
	value := n.Value

	// Handle string literals like INTERVAL '2 years' - extract value and unit
	if unit == "" {
		if lit, ok := n.Value.(*ast.Literal); ok && lit.Type == ast.LiteralString {
			if strVal, ok := lit.Value.(string); ok {
				val, u := parseIntervalString(strVal)
				if u != "" {
					unit = u
					// Create a numeric literal for the value
					value = &ast.Literal{
						Type:  ast.LiteralInteger,
						Value: val,
					}
				}
			}
		}
	}

	unitNorm := normalizeIntervalUnit(unit)
	fnName := "toInterval" + unitNorm
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction %s (children %d)\n", indent, fnName, 1)
	}
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, 1)
	Node(sb, value, depth+2)
}

// parseIntervalString parses a string like "2 years" into value and unit
func parseIntervalString(s string) (value string, unit string) {
	// Trim surrounding quotes if present
	s = strings.Trim(s, "'\"")
	s = strings.TrimSpace(s)

	// Find the split between number and unit
	parts := strings.Fields(s)
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	return s, ""
}

func explainExistsExpr(sb *strings.Builder, n *ast.ExistsExpr, indent string, depth int) {
	explainExistsExprWithAlias(sb, n, "", indent, depth)
}

func explainExistsExprWithAlias(sb *strings.Builder, n *ast.ExistsExpr, alias string, indent string, depth int) {
	// EXISTS is represented as Function exists
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction exists (alias %s) (children %d)\n", indent, alias, 1)
	} else {
		fmt.Fprintf(sb, "%sFunction exists (children %d)\n", indent, 1)
	}
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
	// Only use the external alias parameter (from explicit AS on EXTRACT itself)
	// NOT the alias from the From expression - that stays on the inner expression
	if alias != "" {
		fmt.Fprintf(sb, "%sFunction %s (alias %s) (children %d)\n", indent, fnName, alias, 1)
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
