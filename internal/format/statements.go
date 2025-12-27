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

	// Format WITH clause
	if len(q.With) > 0 {
		sb.WriteString("WITH ")
		for i, w := range q.With {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, w)
		}
		sb.WriteString(" ")
	}

	sb.WriteString("SELECT ")

	if q.Distinct {
		sb.WriteString("DISTINCT ")
	}

	// Format TOP clause
	if q.Top != nil {
		sb.WriteString("TOP ")
		Expression(sb, q.Top)
		sb.WriteString(" ")
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

	// Format ARRAY JOIN clause
	if q.ArrayJoin != nil {
		formatArrayJoinClause(sb, q.ArrayJoin)
	}

	// Format PREWHERE clause
	if q.PreWhere != nil {
		sb.WriteString(" PREWHERE ")
		Expression(sb, q.PreWhere)
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
		if q.WithRollup {
			sb.WriteString(" WITH ROLLUP")
		}
		if q.WithCube {
			sb.WriteString(" WITH CUBE")
		}
		if q.WithTotals {
			sb.WriteString(" WITH TOTALS")
		}
	}

	// Format HAVING clause
	if q.Having != nil {
		sb.WriteString(" HAVING ")
		Expression(sb, q.Having)
	}

	// Format QUALIFY clause
	if q.Qualify != nil {
		sb.WriteString(" QUALIFY ")
		Expression(sb, q.Qualify)
	}

	// Format WINDOW clause
	if len(q.Window) > 0 {
		sb.WriteString(" WINDOW ")
		for i, w := range q.Window {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(w.Name)
			sb.WriteString(" AS ")
			formatWindowSpec(sb, w.Spec)
		}
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

	// Format LIMIT clause with OFFSET
	if q.Limit != nil {
		sb.WriteString(" LIMIT ")
		Expression(sb, q.Limit)
	}
	if q.Offset != nil {
		sb.WriteString(" OFFSET ")
		Expression(sb, q.Offset)
	}

	// Format LIMIT BY clause
	if len(q.LimitBy) > 0 {
		sb.WriteString(" BY ")
		for i, expr := range q.LimitBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, expr)
		}
	}

	// Format SETTINGS clause
	if len(q.Settings) > 0 {
		sb.WriteString(" SETTINGS ")
		for i, s := range q.Settings {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(s.Name)
			sb.WriteString(" = ")
			Expression(sb, s.Value)
		}
	}

	// Format INTO OUTFILE clause
	if q.IntoOutfile != nil {
		sb.WriteString(" INTO OUTFILE '")
		sb.WriteString(q.IntoOutfile.Filename)
		sb.WriteString("'")
		if q.IntoOutfile.Truncate {
			sb.WriteString(" TRUNCATE")
		}
	}

	// Format FORMAT clause
	if q.Format != nil {
		sb.WriteString(" FORMAT ")
		sb.WriteString(q.Format.Name())
	}
}

// formatArrayJoinClause formats an ARRAY JOIN clause.
func formatArrayJoinClause(sb *strings.Builder, a *ast.ArrayJoinClause) {
	if a.Left {
		sb.WriteString(" LEFT")
	}
	sb.WriteString(" ARRAY JOIN ")
	for i, col := range a.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		Expression(sb, col)
	}
}

// formatTablesInSelectQuery formats the FROM clause tables.
func formatTablesInSelectQuery(sb *strings.Builder, t *ast.TablesInSelectQuery) {
	for i, elem := range t.Tables {
		if i > 0 {
			if elem.Join == nil {
				sb.WriteString(", ")
			} else if isImplicitCrossJoin(elem.Join) {
				// Cross join without ON/USING is represented as comma-separated
				sb.WriteString(", ")
				if elem.Table != nil {
					formatTableExpression(sb, elem.Table)
				}
				continue
			}
		}
		formatTablesInSelectQueryElement(sb, elem)
	}
}

// isImplicitCrossJoin checks if a join is an implicit cross join (comma-separated)
func isImplicitCrossJoin(j *ast.TableJoin) bool {
	// Empty type or CROSS with no ON/USING is implicit cross join
	return (j.Type == "" || j.Type == ast.JoinCross) && j.On == nil && len(j.Using) == 0 && !j.Global && j.Strictness == ""
}

// formatTablesInSelectQueryElement formats a single table element.
func formatTablesInSelectQueryElement(sb *strings.Builder, t *ast.TablesInSelectQueryElement) {
	if t.Join != nil {
		formatTableJoinPrefix(sb, t.Join)
	}
	if t.Table != nil {
		formatTableExpression(sb, t.Table)
	}
	if t.Join != nil {
		formatTableJoinSuffix(sb, t.Join)
	}
}

// formatTableJoinPrefix formats the JOIN keyword and modifiers.
func formatTableJoinPrefix(sb *strings.Builder, j *ast.TableJoin) {
	sb.WriteString(" ")
	if j.Global {
		sb.WriteString("GLOBAL ")
	}
	if j.Strictness != "" {
		sb.WriteString(string(j.Strictness))
		sb.WriteString(" ")
	}
	if j.Type != "" {
		sb.WriteString(string(j.Type))
		sb.WriteString(" ")
	}
	sb.WriteString("JOIN ")
}

// formatTableJoinSuffix formats the ON or USING clause.
func formatTableJoinSuffix(sb *strings.Builder, j *ast.TableJoin) {
	if j.On != nil {
		sb.WriteString(" ON ")
		Expression(sb, j.On)
	} else if len(j.Using) > 0 {
		sb.WriteString(" USING ")
		if len(j.Using) == 1 {
			Expression(sb, j.Using[0])
		} else {
			sb.WriteString("(")
			for i, u := range j.Using {
				if i > 0 {
					sb.WriteString(", ")
				}
				Expression(sb, u)
			}
			sb.WriteString(")")
		}
	}
}

// formatTableExpression formats a table expression.
func formatTableExpression(sb *strings.Builder, t *ast.TableExpression) {
	Expression(sb, t.Table)
	if t.Final {
		sb.WriteString(" FINAL")
	}
	if t.Sample != nil {
		sb.WriteString(" SAMPLE ")
		Expression(sb, t.Sample.Ratio)
		if t.Sample.Offset != nil {
			sb.WriteString(" OFFSET ")
			Expression(sb, t.Sample.Offset)
		}
	}
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
	if o.NullsFirst != nil {
		if *o.NullsFirst {
			sb.WriteString(" NULLS FIRST")
		} else {
			sb.WriteString(" NULLS LAST")
		}
	}
	if o.Collate != "" {
		sb.WriteString(" COLLATE ")
		sb.WriteString(o.Collate)
	}
	if o.WithFill {
		sb.WriteString(" WITH FILL")
		if o.FillFrom != nil {
			sb.WriteString(" FROM ")
			Expression(sb, o.FillFrom)
		}
		if o.FillTo != nil {
			sb.WriteString(" TO ")
			Expression(sb, o.FillTo)
		}
		if o.FillStep != nil {
			sb.WriteString(" STEP ")
			Expression(sb, o.FillStep)
		}
	}
}

// formatSelectIntersectExceptQuery formats a SELECT INTERSECT/EXCEPT query.
func formatSelectIntersectExceptQuery(sb *strings.Builder, q *ast.SelectIntersectExceptQuery) {
	for i, sel := range q.Selects {
		if i > 0 {
			sb.WriteString(" ")
		}
		Statement(sb, sel)
	}
}

// formatSetQuery formats a SET statement.
func formatSetQuery(sb *strings.Builder, q *ast.SetQuery) {
	if q == nil {
		return
	}
	sb.WriteString("SET ")
	for i, s := range q.Settings {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(s.Name)
		sb.WriteString(" = ")
		Expression(sb, s.Value)
	}
}

// formatDropQuery formats a DROP statement.
func formatDropQuery(sb *strings.Builder, q *ast.DropQuery) {
	if q == nil {
		return
	}
	sb.WriteString("DROP ")
	if q.DropDatabase {
		sb.WriteString("DATABASE ")
	} else if q.View != "" {
		sb.WriteString("VIEW ")
	} else if q.Function != "" {
		sb.WriteString("FUNCTION ")
	} else if q.User != "" {
		sb.WriteString("USER ")
	} else {
		if q.Temporary {
			sb.WriteString("TEMPORARY ")
		}
		sb.WriteString("TABLE ")
	}
	if q.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	if q.DropDatabase {
		sb.WriteString(q.Database)
	} else if q.View != "" {
		if q.Database != "" {
			sb.WriteString(q.Database)
			sb.WriteString(".")
		}
		sb.WriteString(q.View)
	} else if q.Function != "" {
		sb.WriteString(q.Function)
	} else if q.User != "" {
		sb.WriteString(q.User)
	} else if len(q.Tables) > 0 {
		for i, t := range q.Tables {
			if i > 0 {
				sb.WriteString(", ")
			}
			if t.Database != "" {
				sb.WriteString(t.Database)
				sb.WriteString(".")
			}
			sb.WriteString(t.Table)
		}
	} else {
		if q.Database != "" {
			sb.WriteString(q.Database)
			sb.WriteString(".")
		}
		sb.WriteString(q.Table)
	}
	if q.OnCluster != "" {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(q.OnCluster)
	}
	if q.Sync {
		sb.WriteString(" SYNC")
	}
}

// formatCreateQuery formats a CREATE statement.
func formatCreateQuery(sb *strings.Builder, q *ast.CreateQuery) {
	if q == nil {
		return
	}
	sb.WriteString("CREATE ")
	if q.OrReplace {
		sb.WriteString("OR REPLACE ")
	}
	if q.CreateDatabase {
		sb.WriteString("DATABASE ")
		if q.IfNotExists {
			sb.WriteString("IF NOT EXISTS ")
		}
		sb.WriteString(q.Database)
	} else if q.CreateFunction {
		sb.WriteString("FUNCTION ")
		if q.IfNotExists {
			sb.WriteString("IF NOT EXISTS ")
		}
		sb.WriteString(q.FunctionName)
		sb.WriteString(" AS ")
		Expression(sb, q.FunctionBody)
	} else if q.View != "" {
		if q.Materialized {
			sb.WriteString("MATERIALIZED VIEW ")
		} else {
			sb.WriteString("VIEW ")
		}
		if q.IfNotExists {
			sb.WriteString("IF NOT EXISTS ")
		}
		if q.Database != "" {
			sb.WriteString(q.Database)
			sb.WriteString(".")
		}
		sb.WriteString(q.View)
		if q.To != "" {
			sb.WriteString(" TO ")
			sb.WriteString(q.To)
		}
		if q.AsSelect != nil {
			sb.WriteString(" AS ")
			Statement(sb, q.AsSelect)
		}
	} else {
		if q.Temporary {
			sb.WriteString("TEMPORARY ")
		}
		sb.WriteString("TABLE ")
		if q.IfNotExists {
			sb.WriteString("IF NOT EXISTS ")
		}
		if q.Database != "" {
			sb.WriteString(q.Database)
			sb.WriteString(".")
		}
		sb.WriteString(q.Table)
		if q.OnCluster != "" {
			sb.WriteString(" ON CLUSTER ")
			sb.WriteString(q.OnCluster)
		}
		if len(q.Columns) > 0 {
			sb.WriteString(" (")
			for i, col := range q.Columns {
				if i > 0 {
					sb.WriteString(", ")
				}
				formatColumnDeclaration(sb, col)
			}
			sb.WriteString(")")
		}
		if q.Engine != nil {
			sb.WriteString(" ENGINE = ")
			sb.WriteString(q.Engine.Name)
			if q.Engine.HasParentheses || len(q.Engine.Parameters) > 0 {
				sb.WriteString("(")
				for i, p := range q.Engine.Parameters {
					if i > 0 {
						sb.WriteString(", ")
					}
					Expression(sb, p)
				}
				sb.WriteString(")")
			}
		}
		if len(q.OrderBy) > 0 {
			sb.WriteString(" ORDER BY ")
			if len(q.OrderBy) == 1 {
				Expression(sb, q.OrderBy[0])
			} else {
				sb.WriteString("(")
				for i, e := range q.OrderBy {
					if i > 0 {
						sb.WriteString(", ")
					}
					Expression(sb, e)
				}
				sb.WriteString(")")
			}
		}
		if q.PartitionBy != nil {
			sb.WriteString(" PARTITION BY ")
			Expression(sb, q.PartitionBy)
		}
		if len(q.PrimaryKey) > 0 {
			sb.WriteString(" PRIMARY KEY ")
			if len(q.PrimaryKey) == 1 {
				Expression(sb, q.PrimaryKey[0])
			} else {
				sb.WriteString("(")
				for i, e := range q.PrimaryKey {
					if i > 0 {
						sb.WriteString(", ")
					}
					Expression(sb, e)
				}
				sb.WriteString(")")
			}
		}
		if q.SampleBy != nil {
			sb.WriteString(" SAMPLE BY ")
			Expression(sb, q.SampleBy)
		}
		// Format SETTINGS clause (before AS SELECT)
		if len(q.Settings) > 0 {
			sb.WriteString(" SETTINGS ")
			for i, s := range q.Settings {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(s.Name)
				sb.WriteString(" = ")
				Expression(sb, s.Value)
			}
		}
		if q.AsSelect != nil {
			sb.WriteString(" AS ")
			Statement(sb, q.AsSelect)
		}
	}
}

// formatColumnDeclaration formats a column declaration.
func formatColumnDeclaration(sb *strings.Builder, c *ast.ColumnDeclaration) {
	sb.WriteString(c.Name)
	sb.WriteString(" ")
	formatDataType(sb, c.Type)
	if c.Nullable != nil {
		if *c.Nullable {
			sb.WriteString(" NULL")
		} else {
			sb.WriteString(" NOT NULL")
		}
	}
	if c.Default != nil {
		if c.DefaultKind != "" {
			sb.WriteString(" ")
			sb.WriteString(c.DefaultKind)
			sb.WriteString(" ")
		} else {
			sb.WriteString(" DEFAULT ")
		}
		Expression(sb, c.Default)
	}
	if c.Codec != nil {
		sb.WriteString(" CODEC(")
		for i, codec := range c.Codec.Codecs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(codec.Name)
			if len(codec.Arguments) > 0 {
				sb.WriteString("(")
				for j, arg := range codec.Arguments {
					if j > 0 {
						sb.WriteString(", ")
					}
					Expression(sb, arg)
				}
				sb.WriteString(")")
			}
		}
		sb.WriteString(")")
	}
	if c.Comment != "" {
		sb.WriteString(" COMMENT '")
		sb.WriteString(c.Comment)
		sb.WriteString("'")
	}
}

// formatDataType formats a data type.
func formatDataType(sb *strings.Builder, d *ast.DataType) {
	if d == nil {
		return
	}
	sb.WriteString(d.Name)
	if len(d.Parameters) > 0 || d.HasParentheses {
		sb.WriteString("(")
		for i, p := range d.Parameters {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, p)
		}
		sb.WriteString(")")
	}
}

// formatInsertQuery formats an INSERT statement.
func formatInsertQuery(sb *strings.Builder, q *ast.InsertQuery) {
	if q == nil {
		return
	}
	sb.WriteString("INSERT INTO ")
	if q.Function != nil {
		sb.WriteString("FUNCTION ")
		sb.WriteString(q.Function.Name)
		sb.WriteString("(")
		for i, arg := range q.Function.Arguments {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, arg)
		}
		sb.WriteString(")")
	} else {
		if q.Database != "" {
			sb.WriteString(q.Database)
			sb.WriteString(".")
		}
		sb.WriteString(q.Table)
	}
	if len(q.Columns) > 0 {
		sb.WriteString(" (")
		for i, col := range q.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col.Name())
		}
		sb.WriteString(")")
	}
	if q.Select != nil {
		sb.WriteString(" ")
		Statement(sb, q.Select)
	}
	if q.Format != nil {
		sb.WriteString(" FORMAT ")
		sb.WriteString(q.Format.Name())
	}
}

// formatAlterQuery formats an ALTER statement.
func formatAlterQuery(sb *strings.Builder, q *ast.AlterQuery) {
	if q == nil {
		return
	}
	sb.WriteString("ALTER TABLE ")
	if q.Database != "" {
		sb.WriteString(q.Database)
		sb.WriteString(".")
	}
	sb.WriteString(q.Table)
	if q.OnCluster != "" {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(q.OnCluster)
	}
	for i, cmd := range q.Commands {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(" ")
		formatAlterCommand(sb, cmd)
	}
}

// formatAlterCommand formats an ALTER command.
func formatAlterCommand(sb *strings.Builder, c *ast.AlterCommand) {
	switch c.Type {
	case ast.AlterAddColumn:
		sb.WriteString("ADD COLUMN ")
		if c.IfNotExists {
			sb.WriteString("IF NOT EXISTS ")
		}
		formatColumnDeclaration(sb, c.Column)
		if c.AfterColumn != "" {
			sb.WriteString(" AFTER ")
			sb.WriteString(c.AfterColumn)
		}
	case ast.AlterDropColumn:
		sb.WriteString("DROP COLUMN ")
		if c.IfExists {
			sb.WriteString("IF EXISTS ")
		}
		sb.WriteString(c.ColumnName)
	case ast.AlterModifyColumn:
		sb.WriteString("MODIFY COLUMN ")
		if c.IfExists {
			sb.WriteString("IF EXISTS ")
		}
		formatColumnDeclaration(sb, c.Column)
	case ast.AlterRenameColumn:
		sb.WriteString("RENAME COLUMN ")
		if c.IfExists {
			sb.WriteString("IF EXISTS ")
		}
		sb.WriteString(c.ColumnName)
		sb.WriteString(" TO ")
		sb.WriteString(c.NewName)
	case ast.AlterDeleteWhere:
		sb.WriteString("DELETE WHERE ")
		Expression(sb, c.Where)
	case ast.AlterUpdate:
		sb.WriteString("UPDATE ")
		for i, a := range c.Assignments {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(a.Column)
			sb.WriteString(" = ")
			Expression(sb, a.Value)
		}
		if c.Where != nil {
			sb.WriteString(" WHERE ")
			Expression(sb, c.Where)
		}
	case ast.AlterDropPartition:
		sb.WriteString("DROP PARTITION ")
		Expression(sb, c.Partition)
	case ast.AlterDetachPartition:
		sb.WriteString("DETACH PARTITION ")
		Expression(sb, c.Partition)
	case ast.AlterAttachPartition:
		sb.WriteString("ATTACH PARTITION ")
		Expression(sb, c.Partition)
		if c.FromTable != "" {
			sb.WriteString(" FROM ")
			sb.WriteString(c.FromTable)
		}
	}
}

// formatTruncateQuery formats a TRUNCATE statement.
func formatTruncateQuery(sb *strings.Builder, q *ast.TruncateQuery) {
	if q == nil {
		return
	}
	sb.WriteString("TRUNCATE ")
	if q.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	sb.WriteString("TABLE ")
	if q.Database != "" {
		sb.WriteString(q.Database)
		sb.WriteString(".")
	}
	sb.WriteString(q.Table)
	if q.OnCluster != "" {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(q.OnCluster)
	}
}

// formatUseQuery formats a USE statement.
func formatUseQuery(sb *strings.Builder, q *ast.UseQuery) {
	if q == nil {
		return
	}
	sb.WriteString("USE ")
	sb.WriteString(q.Database)
}

// formatDescribeQuery formats a DESCRIBE statement.
func formatDescribeQuery(sb *strings.Builder, q *ast.DescribeQuery) {
	if q == nil {
		return
	}
	sb.WriteString("DESCRIBE ")
	if q.TableFunction != nil {
		sb.WriteString(q.TableFunction.Name)
		sb.WriteString("(")
		for i, arg := range q.TableFunction.Arguments {
			if i > 0 {
				sb.WriteString(", ")
			}
			Expression(sb, arg)
		}
		sb.WriteString(")")
	} else {
		if q.Database != "" {
			sb.WriteString(q.Database)
			sb.WriteString(".")
		}
		sb.WriteString(q.Table)
	}
	if q.Format != "" {
		sb.WriteString(" FORMAT ")
		sb.WriteString(q.Format)
	}
}

// formatShowQuery formats a SHOW statement.
func formatShowQuery(sb *strings.Builder, q *ast.ShowQuery) {
	if q == nil {
		return
	}
	sb.WriteString("SHOW ")
	switch q.ShowType {
	case ast.ShowTables:
		sb.WriteString("TABLES")
	case ast.ShowDatabases:
		sb.WriteString("DATABASES")
	case ast.ShowProcesses:
		sb.WriteString("PROCESSLIST")
	case ast.ShowCreate:
		sb.WriteString("CREATE TABLE ")
		if q.Database != "" {
			sb.WriteString(q.Database)
			sb.WriteString(".")
		}
		sb.WriteString(q.From)
	case ast.ShowCreateDB:
		sb.WriteString("CREATE DATABASE ")
		sb.WriteString(q.Database)
	case ast.ShowColumns:
		sb.WriteString("COLUMNS FROM ")
		sb.WriteString(q.From)
	case ast.ShowDictionaries:
		sb.WriteString("DICTIONARIES")
	case ast.ShowFunctions:
		sb.WriteString("FUNCTIONS")
	case ast.ShowSettings:
		sb.WriteString("SETTINGS")
	}
	if q.From != "" && q.ShowType != ast.ShowCreate && q.ShowType != ast.ShowColumns {
		sb.WriteString(" FROM ")
		sb.WriteString(q.From)
	}
	if q.Like != "" {
		sb.WriteString(" LIKE '")
		sb.WriteString(q.Like)
		sb.WriteString("'")
	}
	if q.Where != nil {
		sb.WriteString(" WHERE ")
		Expression(sb, q.Where)
	}
	if q.Limit != nil {
		sb.WriteString(" LIMIT ")
		Expression(sb, q.Limit)
	}
}

// formatExplainQuery formats an EXPLAIN statement.
func formatExplainQuery(sb *strings.Builder, q *ast.ExplainQuery) {
	if q == nil {
		return
	}
	sb.WriteString("EXPLAIN ")
	sb.WriteString(string(q.ExplainType))
	sb.WriteString(" ")
	Statement(sb, q.Statement)
}

// formatOptimizeQuery formats an OPTIMIZE statement.
func formatOptimizeQuery(sb *strings.Builder, q *ast.OptimizeQuery) {
	if q == nil {
		return
	}
	sb.WriteString("OPTIMIZE TABLE ")
	if q.Database != "" {
		sb.WriteString(q.Database)
		sb.WriteString(".")
	}
	sb.WriteString(q.Table)
	if q.OnCluster != "" {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(q.OnCluster)
	}
	if q.Partition != nil {
		sb.WriteString(" PARTITION ")
		Expression(sb, q.Partition)
	}
	if q.Final {
		sb.WriteString(" FINAL")
	}
	if q.Dedupe {
		sb.WriteString(" DEDUPLICATE")
	}
}

// formatSystemQuery formats a SYSTEM statement.
func formatSystemQuery(sb *strings.Builder, q *ast.SystemQuery) {
	if q == nil {
		return
	}
	sb.WriteString("SYSTEM ")
	sb.WriteString(q.Command)
	if q.Database != "" {
		sb.WriteString(" ")
		sb.WriteString(q.Database)
		if q.Table != "" {
			sb.WriteString(".")
			sb.WriteString(q.Table)
		}
	} else if q.Table != "" {
		sb.WriteString(" ")
		sb.WriteString(q.Table)
	}
}

// formatRenameQuery formats a RENAME statement.
func formatRenameQuery(sb *strings.Builder, q *ast.RenameQuery) {
	if q == nil {
		return
	}
	sb.WriteString("RENAME TABLE ")
	for i, p := range q.Pairs {
		if i > 0 {
			sb.WriteString(", ")
		}
		if p.FromDatabase != "" {
			sb.WriteString(p.FromDatabase)
			sb.WriteString(".")
		}
		sb.WriteString(p.FromTable)
		sb.WriteString(" TO ")
		if p.ToDatabase != "" {
			sb.WriteString(p.ToDatabase)
			sb.WriteString(".")
		}
		sb.WriteString(p.ToTable)
	}
	if q.OnCluster != "" {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(q.OnCluster)
	}
}

// formatExchangeQuery formats an EXCHANGE statement.
func formatExchangeQuery(sb *strings.Builder, q *ast.ExchangeQuery) {
	if q == nil {
		return
	}
	sb.WriteString("EXCHANGE TABLES ")
	sb.WriteString(q.Table1)
	sb.WriteString(" AND ")
	sb.WriteString(q.Table2)
	if q.OnCluster != "" {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(q.OnCluster)
	}
}

// formatExistsQueryStmt formats an EXISTS statement.
func formatExistsQueryStmt(sb *strings.Builder, q *ast.ExistsQuery) {
	if q == nil {
		return
	}
	sb.WriteString("EXISTS ")
	if q.Database != "" {
		sb.WriteString(q.Database)
		sb.WriteString(".")
	}
	sb.WriteString(q.Table)
}
