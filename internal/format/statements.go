package format

import (
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// formatSelectWithUnionQuery formats a SELECT with UNION query.
func formatSelectWithUnionQuery(sb *strings.Builder, q *ast.SelectWithUnionQuery) {
	if q == nil {
		return
	}
	for i, sel := range q.Selects {
		if i > 0 {
			sb.WriteString(" UNION ")
			if len(q.UnionModes) > i-1 && q.UnionModes[i-1] == "ALL" {
				sb.WriteString("ALL ")
			} else if len(q.UnionModes) > i-1 && q.UnionModes[i-1] == "DISTINCT" {
				sb.WriteString("DISTINCT ")
			}
		}
		Statement(sb, sel)
	}
}

// formatSelectQuery formats a SELECT query.
func formatSelectQuery(sb *strings.Builder, q *ast.SelectQuery) {
	if q == nil {
		return
	}
	sb.WriteString("SELECT ")

	if q.Distinct {
		sb.WriteString("DISTINCT ")
	}

	// Format columns
	for i, col := range q.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		Expression(sb, col)
	}

	// Format FROM clause
	if q.From != nil {
		sb.WriteString(" FROM ")
		formatTablesInSelectQuery(sb, q.From)
	}

	// Format WHERE clause
	if q.Where != nil {
		sb.WriteString(" WHERE ")
		Expression(sb, q.Where)
	}

	// Format GROUP BY clause
	if len(q.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, expr := range q.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, expr)
		}
	}

	// Format HAVING clause
	if q.Having != nil {
		sb.WriteString(" HAVING ")
		Expression(sb, q.Having)
	}

	// Format ORDER BY clause
	if len(q.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, elem := range q.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			formatOrderByElement(sb, elem)
		}
	}

	// Format LIMIT clause
	if q.Limit != nil {
		sb.WriteString(" LIMIT ")
		Expression(sb, q.Limit)
	}
}

// formatTablesInSelectQuery formats the FROM clause tables.
func formatTablesInSelectQuery(sb *strings.Builder, t *ast.TablesInSelectQuery) {
	for i, elem := range t.Tables {
		if i > 0 {
			// TODO: Handle JOINs properly
			sb.WriteString(", ")
		}
		formatTablesInSelectQueryElement(sb, elem)
	}
}

// formatTablesInSelectQueryElement formats a single table element.
func formatTablesInSelectQueryElement(sb *strings.Builder, t *ast.TablesInSelectQueryElement) {
	if t.Table != nil {
		formatTableExpression(sb, t.Table)
	}
}

// formatTableExpression formats a table expression.
func formatTableExpression(sb *strings.Builder, t *ast.TableExpression) {
	Expression(sb, t.Table)
	if t.Alias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(t.Alias)
	}
}

// formatOrderByElement formats an ORDER BY element.
func formatOrderByElement(sb *strings.Builder, o *ast.OrderByElement) {
	Expression(sb, o.Expression)
	if o.Descending {
		sb.WriteString(" DESC")
	}
}
