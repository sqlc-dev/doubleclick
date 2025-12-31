package explain

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// FormatFloat formats a float value for EXPLAIN AST output
func FormatFloat(val float64) string {
	// Handle special float values - ClickHouse uses lowercase
	if math.IsInf(val, 1) {
		return "inf"
	}
	if math.IsInf(val, -1) {
		return "-inf"
	}
	if math.IsNaN(val) {
		return "nan"
	}
	// Use scientific notation for very small numbers (< 1e-6) or very large numbers (>= 1e21)
	// This matches ClickHouse's behavior
	absVal := math.Abs(val)
	if (absVal > 0 && absVal < 1e-6) || absVal >= 1e21 {
		s := strconv.FormatFloat(val, 'e', -1, 64)
		// Remove leading zeros from exponent (e-07 -> e-7, e+07 -> e+7)
		s = strings.Replace(s, "e-0", "e-", 1)
		s = strings.Replace(s, "e+0", "e+", 1)
		// Remove the + from positive exponents (e+21 -> e21)
		s = strings.Replace(s, "e+", "e", 1)
		return s
	}
	// Use decimal notation for normal-sized numbers
	return strconv.FormatFloat(val, 'f', -1, 64)
}

// escapeStringLiteral escapes special characters in a string for EXPLAIN AST output
// Uses double-escaping as ClickHouse EXPLAIN AST displays strings
// Iterates over bytes to preserve raw bytes (including invalid UTF-8)
func escapeStringLiteral(s string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch b {
		case '\\':
			sb.WriteString("\\\\\\\\") // backslash becomes four backslashes (\\\\)
		case '\'':
			sb.WriteString("\\\\\\'") // single quote becomes \\\' (three backslashes + quote)
		case '\n':
			sb.WriteString("\\\\n") // newline becomes \\n
		case '\t':
			sb.WriteString("\\\\t") // tab becomes \\t
		case '\r':
			sb.WriteString("\\\\r") // carriage return becomes \\r
		case '\x00':
			sb.WriteString("\\\\0") // null becomes \\0
		case '\b':
			sb.WriteString("\\\\b") // backspace becomes \\b
		case '\f':
			sb.WriteString("\\\\f") // form feed becomes \\f
		default:
			sb.WriteByte(b)
		}
	}
	return sb.String()
}

// FormatLiteral formats a literal value for EXPLAIN AST output
func FormatLiteral(lit *ast.Literal) string {
	switch lit.Type {
	case ast.LiteralInteger:
		// Handle both int64 and uint64 values
		switch val := lit.Value.(type) {
		case int64:
			if val >= 0 {
				return fmt.Sprintf("UInt64_%d", val)
			}
			return fmt.Sprintf("Int64_%d", val)
		case uint64:
			return fmt.Sprintf("UInt64_%d", val)
		default:
			return fmt.Sprintf("UInt64_%v", lit.Value)
		}
	case ast.LiteralFloat:
		val := lit.Value.(float64)
		return fmt.Sprintf("Float64_%s", FormatFloat(val))
	case ast.LiteralString:
		s := lit.Value.(string)
		// Escape special characters for display
		s = escapeStringLiteral(s)
		return fmt.Sprintf("\\'%s\\'", s)
	case ast.LiteralBoolean:
		if lit.Value.(bool) {
			return "Bool_1"
		}
		return "Bool_0"
	case ast.LiteralNull:
		return "NULL"
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
			parts = append(parts, FormatLiteral(lit))
		} else if unary, ok := e.(*ast.UnaryExpr); ok && unary.Op == "-" {
			// Handle negation of numeric literals
			if lit, ok := unary.Operand.(*ast.Literal); ok {
				if lit.Type == ast.LiteralInteger {
					switch val := lit.Value.(type) {
					case int64:
						parts = append(parts, fmt.Sprintf("Int64_%d", -val))
					case uint64:
						parts = append(parts, fmt.Sprintf("Int64_-%d", val))
					default:
						parts = append(parts, fmt.Sprintf("Int64_-%v", lit.Value))
					}
				} else if lit.Type == ast.LiteralFloat {
					val := lit.Value.(float64)
					parts = append(parts, fmt.Sprintf("Float64_%s", FormatFloat(-val)))
				} else {
					parts = append(parts, formatExprAsString(e))
				}
			} else {
				parts = append(parts, formatExprAsString(e))
			}
		} else if ident, ok := e.(*ast.Identifier); ok {
			parts = append(parts, ident.Name())
		} else {
			parts = append(parts, formatExprAsString(e))
		}
	}
	return fmt.Sprintf("Array_[%s]", strings.Join(parts, ", "))
}

// formatNumericExpr formats a numeric expression (literal or unary minus of literal)
func formatNumericExpr(e ast.Expression) (string, bool) {
	if lit, ok := e.(*ast.Literal); ok {
		if lit.Type == ast.LiteralInteger || lit.Type == ast.LiteralFloat {
			return FormatLiteral(lit), true
		}
	}
	if unary, ok := e.(*ast.UnaryExpr); ok && unary.Op == "-" {
		if lit, ok := unary.Operand.(*ast.Literal); ok {
			switch val := lit.Value.(type) {
			case int64:
				return fmt.Sprintf("Int64_%d", -val), true
			case uint64:
				return fmt.Sprintf("Int64_%d", -int64(val)), true
			case float64:
				return fmt.Sprintf("Float64_%s", FormatFloat(-val)), true
			}
		}
	}
	return "", false
}

// formatTupleLiteral formats a tuple literal for EXPLAIN AST output
func formatTupleLiteral(val interface{}) string {
	exprs, ok := val.([]ast.Expression)
	if !ok {
		return "Tuple_()"
	}
	var parts []string
	for _, e := range exprs {
		if formatted, ok := formatNumericExpr(e); ok {
			parts = append(parts, formatted)
		} else if lit, ok := e.(*ast.Literal); ok {
			parts = append(parts, FormatLiteral(lit))
		} else if ident, ok := e.(*ast.Identifier); ok {
			parts = append(parts, ident.Name())
		} else {
			parts = append(parts, formatExprAsString(e))
		}
	}
	return fmt.Sprintf("Tuple_(%s)", strings.Join(parts, ", "))
}

// formatInListAsTuple formats an IN expression's value list as a tuple literal
func formatInListAsTuple(list []ast.Expression) string {
	var parts []string
	for _, e := range list {
		if formatted, ok := formatNumericExpr(e); ok {
			parts = append(parts, formatted)
		} else if lit, ok := e.(*ast.Literal); ok {
			parts = append(parts, FormatLiteral(lit))
		} else if ident, ok := e.(*ast.Identifier); ok {
			parts = append(parts, ident.Name())
		} else {
			parts = append(parts, formatExprAsString(e))
		}
	}
	return fmt.Sprintf("Tuple_(%s)", strings.Join(parts, ", "))
}

// FormatDataType formats a DataType for EXPLAIN AST output
func FormatDataType(dt *ast.DataType) string {
	if dt == nil {
		return ""
	}
	if len(dt.Parameters) == 0 {
		return dt.Name
	}
	var params []string
	for _, p := range dt.Parameters {
		// Unwrap ObjectTypeArgument if present (used for JSON/OBJECT types)
		if ota, ok := p.(*ast.ObjectTypeArgument); ok {
			p = ota.Expr
		}
		if lit, ok := p.(*ast.Literal); ok {
			if lit.Type == ast.LiteralString {
				// String parameters in type need extra escaping: 'val' -> \\\'val\\\'
				params = append(params, fmt.Sprintf("\\\\\\'%s\\\\\\'", lit.Value))
			} else {
				params = append(params, fmt.Sprintf("%v", lit.Value))
			}
		} else if nested, ok := p.(*ast.DataType); ok {
			params = append(params, FormatDataType(nested))
		} else if ntp, ok := p.(*ast.NameTypePair); ok {
			// Named tuple field: "name Type"
			params = append(params, ntp.Name+" "+FormatDataType(ntp.Type))
		} else if binExpr, ok := p.(*ast.BinaryExpr); ok {
			// Binary expression (e.g., 'hello' = 1 for Enum types)
			params = append(params, formatBinaryExprForType(binExpr))
		} else {
			params = append(params, fmt.Sprintf("%v", p))
		}
	}
	return fmt.Sprintf("%s(%s)", dt.Name, strings.Join(params, ", "))
}

// formatBinaryExprForType formats a binary expression for use in type parameters
func formatBinaryExprForType(expr *ast.BinaryExpr) string {
	var left, right string

	// Format left side
	if lit, ok := expr.Left.(*ast.Literal); ok {
		if lit.Type == ast.LiteralString {
			left = fmt.Sprintf("\\\\\\'%s\\\\\\'", lit.Value)
		} else {
			left = fmt.Sprintf("%v", lit.Value)
		}
	} else if ident, ok := expr.Left.(*ast.Identifier); ok {
		left = ident.Name()
	} else {
		left = fmt.Sprintf("%v", expr.Left)
	}

	// Format right side
	if lit, ok := expr.Right.(*ast.Literal); ok {
		right = fmt.Sprintf("%v", lit.Value)
	} else if ident, ok := expr.Right.(*ast.Identifier); ok {
		right = ident.Name()
	} else {
		right = fmt.Sprintf("%v", expr.Right)
	}

	return left + " " + expr.Op + " " + right
}

// NormalizeFunctionName normalizes function names to match ClickHouse's EXPLAIN AST output
func NormalizeFunctionName(name string) string {
	// ClickHouse normalizes certain function names in EXPLAIN AST
	normalized := map[string]string{
		"trim":       "trimBoth",
		"ltrim":      "trimLeft",
		"rtrim":      "trimRight",
		"lcase":      "lower",
		"ucase":      "upper",
		"mid":        "substring",
		"ceiling":    "ceil",
		"ln":         "log",
		"log10":      "log10",
		"log2":       "log2",
		"rand":       "rand",
		"ifnull":     "ifNull",
		"nullif":     "nullIf",
		"coalesce":   "coalesce",
		"greatest":   "greatest",
		"least":      "least",
		"concat_ws":  "concat",
	}
	if n, ok := normalized[strings.ToLower(name)]; ok {
		return n
	}
	return name
}

// OperatorToFunction maps binary operators to ClickHouse function names
func OperatorToFunction(op string) string {
	switch op {
	case "+":
		return "plus"
	case "-":
		return "minus"
	case "*":
		return "multiply"
	case "/":
		return "divide"
	case "DIV":
		return "intDiv"
	case "%", "MOD":
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

// UnaryOperatorToFunction maps unary operators to ClickHouse function names
func UnaryOperatorToFunction(op string) string {
	switch op {
	case "-":
		return "negate"
	case "NOT":
		return "not"
	default:
		return strings.ToLower(op)
	}
}

// formatExprAsString formats an expression as a string literal for :: cast syntax
func formatExprAsString(expr ast.Expression) string {
	switch e := expr.(type) {
	case *ast.Literal:
		// Handle explicitly negative literals (like -0 in -0::Int16)
		prefix := ""
		if e.Negative {
			prefix = "-"
		}
		switch e.Type {
		case ast.LiteralInteger:
			// For explicitly negative literals, show the absolute value with prefix
			if e.Negative {
				switch v := e.Value.(type) {
				case int64:
					if v <= 0 {
						return fmt.Sprintf("-%d", -v)
					}
				case uint64:
					return fmt.Sprintf("-%d", v)
				}
			}
			return fmt.Sprintf("%d", e.Value)
		case ast.LiteralFloat:
			// Use Source field if available to preserve original representation (e.g., "0.0")
			if e.Source != "" {
				return e.Source
			}
			if e.Negative {
				switch v := e.Value.(type) {
				case float64:
					if v <= 0 {
						return fmt.Sprintf("%s%v", prefix, -v)
					}
				}
			}
			return fmt.Sprintf("%v", e.Value)
		case ast.LiteralString:
			return e.Value.(string)
		case ast.LiteralBoolean:
			if e.Value.(bool) {
				return "true"
			}
			return "false"
		case ast.LiteralNull:
			return "NULL"
		case ast.LiteralArray:
			return formatArrayAsString(e.Value)
		case ast.LiteralTuple:
			return formatTupleAsString(e.Value)
		default:
			return fmt.Sprintf("%v", e.Value)
		}
	case *ast.Identifier:
		return e.Name()
	case *ast.FunctionCall:
		// Format function call as name(args)
		var args []string
		for _, arg := range e.Arguments {
			args = append(args, formatExprAsString(arg))
		}
		return e.Name + "(" + strings.Join(args, ", ") + ")"
	case *ast.BinaryExpr:
		// Format binary expression as left op right
		left := formatExprAsString(e.Left)
		right := formatExprAsString(e.Right)
		return left + " " + e.Op + " " + right
	case *ast.UnaryExpr:
		// Format unary expression (prefix operators)
		operand := formatExprAsString(e.Operand)
		return e.Op + operand
	case *ast.InExpr:
		// Format IN expression as expr IN (...)
		exprStr := formatExprAsString(e.Expr)
		var listStr string
		if e.Query != nil {
			listStr = "(SELECT ...)" // Simplified for nested queries
		} else if len(e.List) > 0 {
			var parts []string
			for _, item := range e.List {
				parts = append(parts, formatExprAsString(item))
			}
			listStr = "(" + strings.Join(parts, ", ") + ")"
		}
		keyword := "IN"
		if e.Not {
			keyword = "NOT IN"
		}
		if e.Global {
			keyword = "GLOBAL " + keyword
		}
		return exprStr + " " + keyword + " " + listStr
	default:
		return fmt.Sprintf("%v", expr)
	}
}

// formatArrayAsString formats an array literal as a string for :: cast syntax
func formatArrayAsString(val interface{}) string {
	exprs, ok := val.([]ast.Expression)
	if !ok {
		return "[]"
	}
	var parts []string
	for _, e := range exprs {
		parts = append(parts, formatElementAsString(e))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// formatTupleAsString formats a tuple literal as a string for :: cast syntax
func formatTupleAsString(val interface{}) string {
	exprs, ok := val.([]ast.Expression)
	if !ok {
		return "()"
	}
	var parts []string
	for _, e := range exprs {
		parts = append(parts, formatElementAsString(e))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// formatElementAsString formats a single element for array/tuple string representation
func formatElementAsString(expr ast.Expression) string {
	switch e := expr.(type) {
	case *ast.Literal:
		switch e.Type {
		case ast.LiteralInteger:
			return fmt.Sprintf("%d", e.Value)
		case ast.LiteralFloat:
			return fmt.Sprintf("%v", e.Value)
		case ast.LiteralString:
			// Quote strings with single quotes, triple-escape for nested context
			// Expected output format is \\\' (three backslashes + quote)
			s := e.Value.(string)
			// Triple-escape single quotes for nested string literal context
			s = strings.ReplaceAll(s, "'", "\\\\\\'")
			return "\\\\\\'" + s + "\\\\\\'"
		case ast.LiteralBoolean:
			if e.Value.(bool) {
				return "true"
			}
			return "false"
		case ast.LiteralNull:
			return "NULL"
		case ast.LiteralArray:
			return formatArrayAsString(e.Value)
		case ast.LiteralTuple:
			return formatTupleAsString(e.Value)
		default:
			return fmt.Sprintf("%v", e.Value)
		}
	case *ast.Identifier:
		return e.Name()
	case *ast.FunctionCall:
		// Format function call as name(args)
		var args []string
		for _, arg := range e.Arguments {
			args = append(args, formatElementAsString(arg))
		}
		return e.Name + "(" + strings.Join(args, ", ") + ")"
	case *ast.BinaryExpr:
		// Format binary expression as left op right
		left := formatElementAsString(e.Left)
		right := formatElementAsString(e.Right)
		return left + " " + e.Op + " " + right
	case *ast.UnaryExpr:
		// Format unary expression (prefix operators)
		operand := formatElementAsString(e.Operand)
		return e.Op + operand
	default:
		return formatExprAsString(expr)
	}
}
