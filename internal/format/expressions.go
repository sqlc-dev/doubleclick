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
	case *ast.Subquery:
		formatSubquery(sb, e)
	case *ast.LikeExpr:
		formatLikeExpr(sb, e)
	case *ast.CaseExpr:
		formatCaseExpr(sb, e)
	case *ast.CastExpr:
		formatCastExpr(sb, e)
	case *ast.InExpr:
		formatInExpr(sb, e)
	case *ast.BetweenExpr:
		formatBetweenExpr(sb, e)
	case *ast.IsNullExpr:
		formatIsNullExpr(sb, e)
	case *ast.TernaryExpr:
		formatTernaryExpr(sb, e)
	case *ast.ArrayAccess:
		formatArrayAccess(sb, e)
	case *ast.TupleAccess:
		formatTupleAccess(sb, e)
	case *ast.IntervalExpr:
		formatIntervalExpr(sb, e)
	case *ast.ExtractExpr:
		formatExtractExpr(sb, e)
	case *ast.Lambda:
		formatLambda(sb, e)
	case *ast.ColumnsMatcher:
		formatColumnsMatcher(sb, e)
	case *ast.WithElement:
		formatWithElement(sb, e)
	case *ast.ExistsExpr:
		formatExistsExpr(sb, e)
	case *ast.DataType:
		formatDataType(sb, e)
	case *ast.NameTypePair:
		formatNameTypePair(sb, e)
	case *ast.Parameter:
		formatParameter(sb, e)
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
	// Handle parametric functions like quantile(0.9)(x)
	if len(fn.Parameters) > 0 {
		sb.WriteString("(")
		for i, p := range fn.Parameters {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, p)
		}
		sb.WriteString(")")
	}
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
	// Handle SETTINGS for table functions
	if len(fn.Settings) > 0 {
		sb.WriteString(" SETTINGS ")
		for i, s := range fn.Settings {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(s.Name)
			sb.WriteString(" = ")
			Expression(sb, s.Value)
		}
	}
	// Handle window functions (OVER clause)
	if fn.Over != nil {
		sb.WriteString(" OVER ")
		formatWindowSpec(sb, fn.Over)
	}
	// Handle alias
	if fn.Alias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(fn.Alias)
	}
}

// formatWindowSpec formats a window specification.
func formatWindowSpec(sb *strings.Builder, w *ast.WindowSpec) {
	if w.Name != "" {
		sb.WriteString(w.Name)
		return
	}
	sb.WriteString("(")
	if len(w.PartitionBy) > 0 {
		sb.WriteString("PARTITION BY ")
		for i, expr := range w.PartitionBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, expr)
		}
	}
	if len(w.OrderBy) > 0 {
		if len(w.PartitionBy) > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString("ORDER BY ")
		for i, elem := range w.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			formatOrderByElement(sb, elem)
		}
	}
	if w.Frame != nil {
		sb.WriteString(" ")
		formatWindowFrame(sb, w.Frame)
	}
	sb.WriteString(")")
}

// formatWindowFrame formats a window frame.
func formatWindowFrame(sb *strings.Builder, f *ast.WindowFrame) {
	sb.WriteString(string(f.Type))
	sb.WriteString(" ")
	if f.EndBound != nil {
		sb.WriteString("BETWEEN ")
		formatFrameBound(sb, f.StartBound)
		sb.WriteString(" AND ")
		formatFrameBound(sb, f.EndBound)
	} else {
		formatFrameBound(sb, f.StartBound)
	}
}

// formatFrameBound formats a frame bound.
func formatFrameBound(sb *strings.Builder, b *ast.FrameBound) {
	switch b.Type {
	case ast.BoundCurrentRow:
		sb.WriteString("CURRENT ROW")
	case ast.BoundUnboundedPre:
		sb.WriteString("UNBOUNDED PRECEDING")
	case ast.BoundUnboundedFol:
		sb.WriteString("UNBOUNDED FOLLOWING")
	case ast.BoundPreceding:
		Expression(sb, b.Offset)
		sb.WriteString(" PRECEDING")
	case ast.BoundFollowing:
		Expression(sb, b.Offset)
		sb.WriteString(" FOLLOWING")
	}
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
	if len(a.Except) > 0 {
		sb.WriteString(" EXCEPT (")
		for i, col := range a.Except {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	if len(a.Replace) > 0 {
		sb.WriteString(" REPLACE (")
		for i, r := range a.Replace {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, r.Expr)
			sb.WriteString(" AS ")
			sb.WriteString(r.Name)
		}
		sb.WriteString(")")
	}
}

// formatAliasedExpr formats an aliased expression.
func formatAliasedExpr(sb *strings.Builder, a *ast.AliasedExpr) {
	Expression(sb, a.Expr)
	sb.WriteString(" AS ")
	sb.WriteString(a.Alias)
}

// formatSubquery formats a subquery expression.
func formatSubquery(sb *strings.Builder, s *ast.Subquery) {
	sb.WriteString("(")
	Statement(sb, s.Query)
	sb.WriteString(")")
}

// formatLikeExpr formats a LIKE expression.
func formatLikeExpr(sb *strings.Builder, l *ast.LikeExpr) {
	Expression(sb, l.Expr)
	if l.Not {
		sb.WriteString(" NOT")
	}
	if l.CaseInsensitive {
		sb.WriteString(" ILIKE ")
	} else {
		sb.WriteString(" LIKE ")
	}
	Expression(sb, l.Pattern)
}

// formatCaseExpr formats a CASE expression.
func formatCaseExpr(sb *strings.Builder, c *ast.CaseExpr) {
	sb.WriteString("CASE")
	if c.Operand != nil {
		sb.WriteString(" ")
		Expression(sb, c.Operand)
	}
	for _, when := range c.Whens {
		sb.WriteString(" WHEN ")
		Expression(sb, when.Condition)
		sb.WriteString(" THEN ")
		Expression(sb, when.Result)
	}
	if c.Else != nil {
		sb.WriteString(" ELSE ")
		Expression(sb, c.Else)
	}
	sb.WriteString(" END")
}

// formatCastExpr formats a CAST expression.
func formatCastExpr(sb *strings.Builder, c *ast.CastExpr) {
	if c.OperatorSyntax {
		Expression(sb, c.Expr)
		sb.WriteString("::")
		formatDataType(sb, c.Type)
	} else {
		sb.WriteString("CAST(")
		Expression(sb, c.Expr)
		if c.UsedASSyntax {
			sb.WriteString(" AS ")
			formatDataType(sb, c.Type)
		} else if c.TypeExpr != nil {
			sb.WriteString(", ")
			Expression(sb, c.TypeExpr)
		} else if c.Type != nil {
			sb.WriteString(", '")
			sb.WriteString(c.Type.Name)
			sb.WriteString("'")
		}
		sb.WriteString(")")
	}
}

// formatInExpr formats an IN expression.
func formatInExpr(sb *strings.Builder, i *ast.InExpr) {
	Expression(sb, i.Expr)
	if i.Global {
		sb.WriteString(" GLOBAL")
	}
	if i.Not {
		sb.WriteString(" NOT IN ")
	} else {
		sb.WriteString(" IN ")
	}
	if i.Query != nil {
		sb.WriteString("(")
		Statement(sb, i.Query)
		sb.WriteString(")")
	} else {
		sb.WriteString("(")
		for j, e := range i.List {
			if j > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, e)
		}
		sb.WriteString(")")
	}
}

// formatBetweenExpr formats a BETWEEN expression.
func formatBetweenExpr(sb *strings.Builder, b *ast.BetweenExpr) {
	Expression(sb, b.Expr)
	if b.Not {
		sb.WriteString(" NOT BETWEEN ")
	} else {
		sb.WriteString(" BETWEEN ")
	}
	Expression(sb, b.Low)
	sb.WriteString(" AND ")
	Expression(sb, b.High)
}

// formatIsNullExpr formats an IS NULL expression.
func formatIsNullExpr(sb *strings.Builder, i *ast.IsNullExpr) {
	Expression(sb, i.Expr)
	if i.Not {
		sb.WriteString(" IS NOT NULL")
	} else {
		sb.WriteString(" IS NULL")
	}
}

// formatTernaryExpr formats a ternary expression.
func formatTernaryExpr(sb *strings.Builder, t *ast.TernaryExpr) {
	Expression(sb, t.Condition)
	sb.WriteString(" ? ")
	Expression(sb, t.Then)
	sb.WriteString(" : ")
	Expression(sb, t.Else)
}

// formatArrayAccess formats an array access expression.
func formatArrayAccess(sb *strings.Builder, a *ast.ArrayAccess) {
	Expression(sb, a.Array)
	sb.WriteString("[")
	Expression(sb, a.Index)
	sb.WriteString("]")
}

// formatTupleAccess formats a tuple access expression.
func formatTupleAccess(sb *strings.Builder, t *ast.TupleAccess) {
	Expression(sb, t.Tuple)
	sb.WriteString(".")
	Expression(sb, t.Index)
}

// formatIntervalExpr formats an INTERVAL expression.
func formatIntervalExpr(sb *strings.Builder, i *ast.IntervalExpr) {
	sb.WriteString("INTERVAL ")
	Expression(sb, i.Value)
	sb.WriteString(" ")
	sb.WriteString(i.Unit)
}

// formatExtractExpr formats an EXTRACT expression.
func formatExtractExpr(sb *strings.Builder, e *ast.ExtractExpr) {
	sb.WriteString("EXTRACT(")
	sb.WriteString(e.Field)
	sb.WriteString(" FROM ")
	Expression(sb, e.From)
	sb.WriteString(")")
}

// formatLambda formats a lambda expression.
func formatLambda(sb *strings.Builder, l *ast.Lambda) {
	if len(l.Parameters) == 1 {
		sb.WriteString(l.Parameters[0])
	} else {
		sb.WriteString("(")
		for i, p := range l.Parameters {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(p)
		}
		sb.WriteString(")")
	}
	sb.WriteString(" -> ")
	Expression(sb, l.Body)
}

// formatColumnsMatcher formats a COLUMNS expression.
func formatColumnsMatcher(sb *strings.Builder, c *ast.ColumnsMatcher) {
	sb.WriteString("COLUMNS('")
	sb.WriteString(c.Pattern)
	sb.WriteString("')")
}

// formatWithElement formats a WITH element.
func formatWithElement(sb *strings.Builder, w *ast.WithElement) {
	Expression(sb, w.Query)
	sb.WriteString(" AS ")
	sb.WriteString(w.Name)
}

// formatExistsExpr formats an EXISTS expression.
func formatExistsExpr(sb *strings.Builder, e *ast.ExistsExpr) {
	sb.WriteString("EXISTS (")
	Statement(sb, e.Query)
	sb.WriteString(")")
}

// formatNameTypePair formats a name-type pair.
func formatNameTypePair(sb *strings.Builder, n *ast.NameTypePair) {
	sb.WriteString(n.Name)
	sb.WriteString(" ")
	formatDataType(sb, n.Type)
}

// formatParameter formats a parameter.
func formatParameter(sb *strings.Builder, p *ast.Parameter) {
	sb.WriteString("{")
	sb.WriteString(p.Name)
	if p.Type != nil {
		sb.WriteString(":")
		formatDataType(sb, p.Type)
	}
	sb.WriteString("}")
}
