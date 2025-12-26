package format

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// Expression formats an expression.
func Expression(sb *strings.Builder, expr ast.Expression) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *ast.Literal:
		formatLiteral(sb, e)
	case *ast.Identifier:
		formatIdentifier(sb, e)
	case *ast.TableIdentifier:
		formatTableIdentifier(sb, e)
	case *ast.FunctionCall:
		formatFunctionCall(sb, e)
	case *ast.BinaryExpr:
		formatBinaryExpr(sb, e)
	case *ast.UnaryExpr:
		formatUnaryExpr(sb, e)
	case *ast.Asterisk:
		formatAsterisk(sb, e)
	case *ast.AliasedExpr:
		formatAliasedExpr(sb, e)
	default:
		// Fallback for unhandled expressions
		sb.WriteString(fmt.Sprintf("%v", expr))
	}
}

// formatLiteral formats a literal value.
func formatLiteral(sb *strings.Builder, lit *ast.Literal) {
	switch lit.Type {
	case ast.LiteralString:
		sb.WriteString("'")
		// Escape single quotes in the string
		s := lit.Value.(string)
		s = strings.ReplaceAll(s, "'", "''")
		sb.WriteString(s)
		sb.WriteString("'")
	case ast.LiteralInteger:
		switch v := lit.Value.(type) {
		case int64:
			sb.WriteString(fmt.Sprintf("%d", v))
		case uint64:
			sb.WriteString(fmt.Sprintf("%d", v))
		default:
			sb.WriteString(fmt.Sprintf("%v", lit.Value))
		}
	case ast.LiteralFloat:
		sb.WriteString(fmt.Sprintf("%v", lit.Value))
	case ast.LiteralBoolean:
		if lit.Value.(bool) {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case ast.LiteralNull:
		sb.WriteString("NULL")
	case ast.LiteralArray:
		formatArrayLiteral(sb, lit.Value)
	case ast.LiteralTuple:
		formatTupleLiteral(sb, lit.Value)
	default:
		sb.WriteString(fmt.Sprintf("%v", lit.Value))
	}
}

// formatArrayLiteral formats an array literal.
func formatArrayLiteral(sb *strings.Builder, val interface{}) {
	sb.WriteString("[")
	exprs, ok := val.([]ast.Expression)
	if ok {
		for i, e := range exprs {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, e)
		}
	}
	sb.WriteString("]")
}

// formatTupleLiteral formats a tuple literal.
func formatTupleLiteral(sb *strings.Builder, val interface{}) {
	sb.WriteString("(")
	exprs, ok := val.([]ast.Expression)
	if ok {
		for i, e := range exprs {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, e)
		}
	}
	sb.WriteString(")")
}

// formatIdentifier formats an identifier.
func formatIdentifier(sb *strings.Builder, id *ast.Identifier) {
	sb.WriteString(id.Name())
}

// formatTableIdentifier formats a table identifier.
func formatTableIdentifier(sb *strings.Builder, t *ast.TableIdentifier) {
	if t.Database != "" {
		sb.WriteString(t.Database)
		sb.WriteString(".")
	}
	sb.WriteString(t.Table)
}

// formatFunctionCall formats a function call.
func formatFunctionCall(sb *strings.Builder, fn *ast.FunctionCall) {
	sb.WriteString(fn.Name)
	sb.WriteString("(")
	if fn.Distinct {
		sb.WriteString("DISTINCT ")
	}
	for i, arg := range fn.Arguments {
		if i > 0 {
			sb.WriteString(", ")
		}
		Expression(sb, arg)
	}
	sb.WriteString(")")
}

// formatBinaryExpr formats a binary expression.
func formatBinaryExpr(sb *strings.Builder, expr *ast.BinaryExpr) {
	Expression(sb, expr.Left)
	sb.WriteString(" ")
	sb.WriteString(expr.Op)
	sb.WriteString(" ")
	Expression(sb, expr.Right)
}

// formatUnaryExpr formats a unary expression.
func formatUnaryExpr(sb *strings.Builder, expr *ast.UnaryExpr) {
	sb.WriteString(expr.Op)
	Expression(sb, expr.Operand)
}

// formatAsterisk formats an asterisk.
func formatAsterisk(sb *strings.Builder, a *ast.Asterisk) {
	if a.Table != "" {
		sb.WriteString(a.Table)
		sb.WriteString(".")
	}
	sb.WriteString("*")
}

// formatAliasedExpr formats an aliased expression.
func formatAliasedExpr(sb *strings.Builder, a *ast.AliasedExpr) {
	Expression(sb, a.Expr)
	sb.WriteString(" AS ")
	sb.WriteString(a.Alias)
}
