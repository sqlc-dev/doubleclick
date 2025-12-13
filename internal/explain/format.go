package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

// FormatLiteral formats a literal value for EXPLAIN AST output
func FormatLiteral(lit *ast.Literal) string {
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
			parts = append(parts, FormatLiteral(lit))
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
			parts = append(parts, FormatLiteral(lit))
		} else if ident, ok := e.(*ast.Identifier); ok {
			parts = append(parts, ident.Name())
		} else {
			parts = append(parts, fmt.Sprintf("%v", e))
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
		if lit, ok := p.(*ast.Literal); ok {
			if lit.Type == ast.LiteralString {
				// String parameters in type need extra escaping: 'val' -> \\\'val\\\'
				params = append(params, fmt.Sprintf("\\\\\\'%s\\\\\\'", lit.Value))
			} else {
				params = append(params, fmt.Sprintf("%v", lit.Value))
			}
		} else if nested, ok := p.(*ast.DataType); ok {
			params = append(params, FormatDataType(nested))
		} else {
			params = append(params, fmt.Sprintf("%v", p))
		}
	}
	return fmt.Sprintf("%s(%s)", dt.Name, strings.Join(params, ", "))
}

// NormalizeFunctionName normalizes function names to match ClickHouse's EXPLAIN AST output
func NormalizeFunctionName(name string) string {
	// ClickHouse normalizes certain function names in EXPLAIN AST
	normalized := map[string]string{
		"ltrim":       "trimLeft",
		"rtrim":       "trimRight",
		"lcase":       "lower",
		"ucase":       "upper",
		"mid":         "substring",
		"substr":      "substring",
		"pow":         "power",
		"ceil":        "ceiling",
		"ln":          "log",
		"log10":       "log10",
		"log2":        "log2",
		"rand":        "rand",
		"ifnull":      "ifNull",
		"nullif":      "nullIf",
		"coalesce":    "coalesce",
		"greatest":    "greatest",
		"least":       "least",
		"concat_ws":   "concat",
		"length":      "length",
		"char_length": "length",
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
