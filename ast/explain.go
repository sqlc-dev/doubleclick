package ast

import (
	"fmt"
	"strings"
)

// Explain returns a string representation of the AST in the same format
// as ClickHouse's EXPLAIN AST output.
func Explain(stmt Statement) string {
	var b strings.Builder
	explainNode(&b, stmt, 0)
	return b.String()
}

// explainNode recursively writes the AST node to the builder.
func explainNode(b *strings.Builder, node interface{}, depth int) {
	indent := strings.Repeat(" ", depth)

	switch n := node.(type) {
	case *SelectWithUnionQuery:
		children := len(n.Selects)
		fmt.Fprintf(b, "%sSelectWithUnionQuery (children 1)\n", indent)
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, children)
		for _, sel := range n.Selects {
			explainNode(b, sel, depth+2)
		}

	case *SelectQuery:
		children := countSelectQueryChildren(n)
		fmt.Fprintf(b, "%sSelectQuery (children %d)\n", indent, children)
		// WITH clause (comes first)
		if len(n.With) > 0 {
			fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(n.With))
			for _, w := range n.With {
				explainNode(b, w, depth+2)
			}
		}
		// Columns
		if len(n.Columns) > 0 {
			fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(n.Columns))
			for _, col := range n.Columns {
				explainNode(b, col, depth+2)
			}
		}
		// From (with ArrayJoin integrated)
		if n.From != nil || n.ArrayJoin != nil {
			explainTablesWithArrayJoin(b, n.From, n.ArrayJoin, depth+1)
		}
		// Where
		if n.Where != nil {
			explainNode(b, n.Where, depth+1)
		}
		// GroupBy
		if len(n.GroupBy) > 0 {
			fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(n.GroupBy))
			for _, expr := range n.GroupBy {
				explainNode(b, expr, depth+2)
			}
		}
		// Having
		if n.Having != nil {
			explainNode(b, n.Having, depth+1)
		}
		// OrderBy
		if len(n.OrderBy) > 0 {
			fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(n.OrderBy))
			for _, elem := range n.OrderBy {
				explainOrderByElement(b, elem, depth+2)
			}
		}
		// Offset (comes before Limit in ClickHouse output)
		if n.Offset != nil {
			explainNode(b, n.Offset, depth+1)
		}
		// Limit
		if n.Limit != nil {
			explainNode(b, n.Limit, depth+1)
		}

	case *TablesInSelectQuery:
		fmt.Fprintf(b, "%sTablesInSelectQuery (children %d)\n", indent, len(n.Tables))
		for _, table := range n.Tables {
			explainNode(b, table, depth+1)
		}

	case *TablesInSelectQueryElement:
		children := 0
		if n.Table != nil {
			children++
		}
		if n.Join != nil {
			children++
		}
		fmt.Fprintf(b, "%sTablesInSelectQueryElement (children %d)\n", indent, children)
		if n.Table != nil {
			explainNode(b, n.Table, depth+1)
		}
		if n.Join != nil {
			explainTableJoin(b, n.Join, depth+1)
		}

	case *TableExpression:
		children := 1
		fmt.Fprintf(b, "%sTableExpression (children %d)\n", indent, children)
		// Pass alias to the inner Table
		explainTableWithAlias(b, n.Table, n.Alias, depth+1)

	case *TableIdentifier:
		name := n.Table
		if n.Database != "" {
			name = n.Database + "." + name
		}
		if n.Alias != "" {
			fmt.Fprintf(b, "%sTableIdentifier %s (alias %s)\n", indent, name, n.Alias)
		} else {
			fmt.Fprintf(b, "%sTableIdentifier %s\n", indent, name)
		}

	case *Identifier:
		name := n.Name()
		if n.Alias != "" {
			fmt.Fprintf(b, "%sIdentifier %s (alias %s)\n", indent, name, n.Alias)
		} else {
			fmt.Fprintf(b, "%sIdentifier %s\n", indent, name)
		}

	case *Literal:
		explainLiteral(b, n, "", depth)

	case *FunctionCall:
		explainFunctionCall(b, n, depth)

	case *BinaryExpr:
		funcName := binaryOpToFunction(n.Op)
		args := []Expression{n.Left, n.Right}
		fmt.Fprintf(b, "%sFunction %s (children 1)\n", indent, funcName)
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(args))
		for _, arg := range args {
			explainNode(b, arg, depth+2)
		}

	case *UnaryExpr:
		funcName := unaryOpToFunction(n.Op)
		fmt.Fprintf(b, "%sFunction %s (children 1)\n", indent, funcName)
		fmt.Fprintf(b, "%s ExpressionList (children 1)\n", indent)
		explainNode(b, n.Operand, depth+2)

	case *Asterisk:
		if len(n.Except) > 0 || len(n.Replace) > 0 {
			children := 0
			if len(n.Except) > 0 || len(n.Replace) > 0 {
				children = 1
			}
			if n.Table != "" {
				fmt.Fprintf(b, "%sQualifiedAsterisk (children %d)\n", indent, children+1)
				fmt.Fprintf(b, "%s Identifier %s\n", indent, n.Table)
			} else {
				fmt.Fprintf(b, "%sAsterisk (children %d)\n", indent, children)
			}
			if len(n.Except) > 0 {
				fmt.Fprintf(b, "%s ColumnsTransformerList (children 1)\n", indent)
				fmt.Fprintf(b, "%s  ColumnsExceptTransformer (children %d)\n", indent, len(n.Except))
				for _, col := range n.Except {
					fmt.Fprintf(b, "%s   Identifier %s\n", indent, col)
				}
			}
			if len(n.Replace) > 0 {
				fmt.Fprintf(b, "%s ColumnsTransformerList (children 1)\n", indent)
				fmt.Fprintf(b, "%s  ColumnsReplaceTransformer (children %d)\n", indent, len(n.Replace))
				for _, r := range n.Replace {
					explainNode(b, r.Expr, depth+3)
				}
			}
		} else if n.Table != "" {
			fmt.Fprintf(b, "%sQualifiedAsterisk (children 1)\n", indent)
			fmt.Fprintf(b, "%s Identifier %s\n", indent, n.Table)
		} else {
			fmt.Fprintf(b, "%sAsterisk\n", indent)
		}

	case *ColumnsMatcher:
		fmt.Fprintf(b, "%sColumnsMatcher %s\n", indent, n.Pattern)

	case *Subquery:
		if n.Alias != "" {
			fmt.Fprintf(b, "%sSubquery (alias %s) (children 1)\n", indent, n.Alias)
		} else {
			fmt.Fprintf(b, "%sSubquery (children 1)\n", indent)
		}
		explainNode(b, n.Query, depth+1)

	case *CaseExpr:
		explainCaseExpr(b, n, depth)

	case *CastExpr:
		explainCastExpr(b, n, depth)

	case *Lambda:
		explainLambda(b, n, depth)

	case *TernaryExpr:
		// Ternary is represented as if(cond, then, else)
		fmt.Fprintf(b, "%sFunction if (children 1)\n", indent)
		fmt.Fprintf(b, "%s ExpressionList (children 3)\n", indent)
		explainNode(b, n.Condition, depth+2)
		explainNode(b, n.Then, depth+2)
		explainNode(b, n.Else, depth+2)

	case *InExpr:
		funcName := "in"
		if n.Not {
			funcName = "notIn"
		}
		if n.Global {
			funcName = "global" + strings.Title(funcName)
		}
		fmt.Fprintf(b, "%sFunction %s (children 1)\n", indent, funcName)
		fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
		explainNode(b, n.Expr, depth+2)
		if n.Query != nil {
			explainNode(b, n.Query, depth+2)
		} else {
			// List is shown as a Tuple literal
			explainInListAsTuple(b, n.List, depth+2)
		}

	case *BetweenExpr:
		// BETWEEN is expanded to and(greaterOrEquals(expr, low), lessOrEquals(expr, high))
		// NOT BETWEEN is expanded to or(less(expr, low), greater(expr, high))
		if n.Not {
			fmt.Fprintf(b, "%sFunction or (children 1)\n", indent)
			fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
			fmt.Fprintf(b, "%s  Function less (children 1)\n", indent)
			fmt.Fprintf(b, "%s   ExpressionList (children 2)\n", indent)
			explainNode(b, n.Expr, depth+4)
			explainNode(b, n.Low, depth+4)
			fmt.Fprintf(b, "%s  Function greater (children 1)\n", indent)
			fmt.Fprintf(b, "%s   ExpressionList (children 2)\n", indent)
			explainNode(b, n.Expr, depth+4)
			explainNode(b, n.High, depth+4)
		} else {
			fmt.Fprintf(b, "%sFunction and (children 1)\n", indent)
			fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
			fmt.Fprintf(b, "%s  Function greaterOrEquals (children 1)\n", indent)
			fmt.Fprintf(b, "%s   ExpressionList (children 2)\n", indent)
			explainNode(b, n.Expr, depth+4)
			explainNode(b, n.Low, depth+4)
			fmt.Fprintf(b, "%s  Function lessOrEquals (children 1)\n", indent)
			fmt.Fprintf(b, "%s   ExpressionList (children 2)\n", indent)
			explainNode(b, n.Expr, depth+4)
			explainNode(b, n.High, depth+4)
		}

	case *IsNullExpr:
		funcName := "isNull"
		if n.Not {
			funcName = "isNotNull"
		}
		fmt.Fprintf(b, "%sFunction %s (children 1)\n", indent, funcName)
		fmt.Fprintf(b, "%s ExpressionList (children 1)\n", indent)
		explainNode(b, n.Expr, depth+2)

	case *LikeExpr:
		funcName := "like"
		if n.CaseInsensitive {
			funcName = "ilike"
		}
		if n.Not {
			funcName = "not" + strings.Title(funcName)
		}
		fmt.Fprintf(b, "%sFunction %s (children 1)\n", indent, funcName)
		fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
		explainNode(b, n.Expr, depth+2)
		explainNode(b, n.Pattern, depth+2)

	case *ArrayAccess:
		fmt.Fprintf(b, "%sFunction arrayElement (children 1)\n", indent)
		fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
		explainNode(b, n.Array, depth+2)
		explainNode(b, n.Index, depth+2)

	case *TupleAccess:
		fmt.Fprintf(b, "%sFunction tupleElement (children 1)\n", indent)
		fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
		explainNode(b, n.Tuple, depth+2)
		explainNode(b, n.Index, depth+2)

	case *IntervalExpr:
		fmt.Fprintf(b, "%sFunction toInterval%s (children 1)\n", indent, strings.Title(strings.ToLower(n.Unit)))
		fmt.Fprintf(b, "%s ExpressionList (children 1)\n", indent)
		explainNode(b, n.Value, depth+2)

	case *ExtractExpr:
		// EXTRACT(YEAR FROM date) becomes toYear(date)
		funcName := extractFieldToFunction(n.Field)
		if n.Alias != "" {
			fmt.Fprintf(b, "%sFunction %s (alias %s) (children 1)\n", indent, funcName, n.Alias)
		} else {
			fmt.Fprintf(b, "%sFunction %s (children 1)\n", indent, funcName)
		}
		fmt.Fprintf(b, "%s ExpressionList (children 1)\n", indent)
		explainNode(b, n.From, depth+2)

	case *AliasedExpr:
		// For aliased expressions, we need to print the inner expression with the alias
		explainNodeWithAlias(b, n.Expr, n.Alias, depth)

	case *WithElement:
		// For scalar WITH (WITH 1 AS x), output the expression with alias
		// For subquery WITH (WITH x AS (SELECT 1)), output as WithElement
		if _, isSubquery := n.Query.(*Subquery); isSubquery {
			fmt.Fprintf(b, "%sWithElement (children 1)\n", indent)
			explainNode(b, n.Query, depth+1)
		} else {
			// Scalar expression - output with alias
			explainNodeWithAlias(b, n.Query, n.Name, depth)
		}

	case *ExistsExpr:
		fmt.Fprintf(b, "%sFunction exists (children 1)\n", indent)
		fmt.Fprintf(b, "%s ExpressionList (children 1)\n", indent)
		explainNode(b, n.Query, depth+2)

	case *DataType:
		// Data types in expressions (like in CAST)
		if len(n.Parameters) > 0 {
			fmt.Fprintf(b, "%sFunction %s (children 1)\n", indent, n.Name)
			fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(n.Parameters))
			for _, p := range n.Parameters {
				explainNode(b, p, depth+2)
			}
		} else {
			fmt.Fprintf(b, "%sIdentifier %s\n", indent, n.Name)
		}

	// Non-SELECT statements
	case *UseQuery:
		fmt.Fprintf(b, "%sUseQuery %s (children 1)\n", indent, n.Database)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, n.Database)

	case *TruncateQuery:
		tableName := n.Table
		if n.Database != "" {
			tableName = n.Database + "." + tableName
		}
		fmt.Fprintf(b, "%sTruncateQuery %s (children 1)\n", indent, tableName)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, tableName)

	case *AlterQuery:
		tableName := n.Table
		if n.Database != "" {
			tableName = n.Database + "." + tableName
		}
		fmt.Fprintf(b, "%sAlterQuery  %s (children 2)\n", indent, tableName)
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(n.Commands))
		for _, cmd := range n.Commands {
			explainAlterCommand(b, cmd, depth+2)
		}
		fmt.Fprintf(b, "%s Identifier %s\n", indent, tableName)

	case *DropQuery:
		var name string
		if n.DropDatabase {
			name = n.Database
		} else if n.View != "" {
			name = n.View
		} else {
			name = n.Table
		}
		if n.Database != "" && !n.DropDatabase {
			name = n.Database + "." + name
		}
		fmt.Fprintf(b, "%sDropQuery %s (children 1)\n", indent, name)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, name)

	case *CreateQuery:
		explainCreateQuery(b, n, depth)

	case *InsertQuery:
		tableName := n.Table
		if n.Database != "" {
			tableName = n.Database + "." + tableName
		}
		children := 1
		if n.Select != nil {
			children++
		}
		fmt.Fprintf(b, "%sInsertQuery %s (children %d)\n", indent, tableName, children)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, tableName)
		if n.Select != nil {
			explainNode(b, n.Select, depth+1)
		}

	case *SystemQuery:
		fmt.Fprintf(b, "%sSystemQuery %s\n", indent, n.Command)

	case *OptimizeQuery:
		tableName := n.Table
		if n.Database != "" {
			tableName = n.Database + "." + tableName
		}
		fmt.Fprintf(b, "%sOptimizeQuery %s (children 1)\n", indent, tableName)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, tableName)

	case *DescribeQuery:
		tableName := n.Table
		if n.Database != "" {
			tableName = n.Database + "." + tableName
		}
		fmt.Fprintf(b, "%sDescribeQuery %s (children 1)\n", indent, tableName)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, tableName)

	case *ShowQuery:
		fmt.Fprintf(b, "%sShowQuery %s\n", indent, n.ShowType)

	case *SetQuery:
		fmt.Fprintf(b, "%sSetQuery (children %d)\n", indent, len(n.Settings))
		for _, s := range n.Settings {
			fmt.Fprintf(b, "%s SettingExpr %s\n", indent, s.Name)
		}

	case *RenameQuery:
		fmt.Fprintf(b, "%sRenameQuery (children 2)\n", indent)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, n.From)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, n.To)

	case *ExchangeQuery:
		fmt.Fprintf(b, "%sExchangeQuery (children 2)\n", indent)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, n.Table1)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, n.Table2)

	default:
		// For unknown types, just print the type name
		fmt.Fprintf(b, "%s%T\n", indent, n)
	}
}

// explainTableWithAlias prints a table expression (TableIdentifier, Subquery, Function) with an alias.
func explainTableWithAlias(b *strings.Builder, table interface{}, alias string, depth int) {
	indent := strings.Repeat(" ", depth)

	switch t := table.(type) {
	case *TableIdentifier:
		name := t.Table
		if t.Database != "" {
			name = t.Database + "." + name
		}
		if alias != "" {
			fmt.Fprintf(b, "%sTableIdentifier %s (alias %s)\n", indent, name, alias)
		} else if t.Alias != "" {
			fmt.Fprintf(b, "%sTableIdentifier %s (alias %s)\n", indent, name, t.Alias)
		} else {
			fmt.Fprintf(b, "%sTableIdentifier %s\n", indent, name)
		}

	case *Subquery:
		if alias != "" {
			fmt.Fprintf(b, "%sSubquery (alias %s) (children 1)\n", indent, alias)
		} else if t.Alias != "" {
			fmt.Fprintf(b, "%sSubquery (alias %s) (children 1)\n", indent, t.Alias)
		} else {
			fmt.Fprintf(b, "%sSubquery (children 1)\n", indent)
		}
		explainNode(b, t.Query, depth+1)

	case *FunctionCall:
		// For table functions like numbers(), pass alias
		if alias != "" {
			explainFunctionCallWithAlias(b, t, alias, depth)
		} else {
			explainFunctionCall(b, t, depth)
		}

	default:
		explainNode(b, table, depth)
	}
}

// explainNodeWithAlias prints a node with an alias suffix.
func explainNodeWithAlias(b *strings.Builder, node interface{}, alias string, depth int) {
	indent := strings.Repeat(" ", depth)

	switch n := node.(type) {
	case *Literal:
		explainLiteral(b, n, alias, depth)

	case *Identifier:
		name := n.Name()
		if alias != "" {
			fmt.Fprintf(b, "%sIdentifier %s (alias %s)\n", indent, name, alias)
		} else if n.Alias != "" {
			fmt.Fprintf(b, "%sIdentifier %s (alias %s)\n", indent, name, n.Alias)
		} else {
			fmt.Fprintf(b, "%sIdentifier %s\n", indent, name)
		}

	case *FunctionCall:
		explainFunctionCallWithAlias(b, n, alias, depth)

	default:
		// Fall back to regular node printing
		explainNode(b, node, depth)
	}
}

// explainLiteral formats a literal value.
func explainLiteral(b *strings.Builder, lit *Literal, alias string, depth int) {
	indent := strings.Repeat(" ", depth)
	var valueStr string

	switch lit.Type {
	case LiteralString:
		valueStr = fmt.Sprintf("\\'%v\\'", lit.Value)
	case LiteralInteger:
		valueStr = fmt.Sprintf("UInt64_%v", lit.Value)
	case LiteralFloat:
		valueStr = fmt.Sprintf("Float64_%v", lit.Value)
	case LiteralBoolean:
		if lit.Value.(bool) {
			valueStr = "Bool_1"
		} else {
			valueStr = "Bool_0"
		}
	case LiteralNull:
		valueStr = "NULL"
	case LiteralArray:
		valueStr = formatArrayLiteral(lit.Value)
	case LiteralTuple:
		valueStr = formatTupleLiteral(lit.Value)
	default:
		valueStr = fmt.Sprintf("%v", lit.Value)
	}

	if alias != "" {
		fmt.Fprintf(b, "%sLiteral %s (alias %s)\n", indent, valueStr, alias)
	} else {
		fmt.Fprintf(b, "%sLiteral %s\n", indent, valueStr)
	}
}

// formatArrayLiteral formats an array literal.
func formatArrayLiteral(value interface{}) string {
	switch v := value.(type) {
	case []interface{}:
		parts := make([]string, len(v))
		for i, elem := range v {
			parts[i] = formatLiteralElement(elem)
		}
		return fmt.Sprintf("Array_[%s]", strings.Join(parts, ", "))
	case []Expression:
		parts := make([]string, len(v))
		for i, elem := range v {
			if lit, ok := elem.(*Literal); ok {
				parts[i] = formatLiteralElement(lit.Value)
			} else {
				parts[i] = fmt.Sprintf("%v", elem)
			}
		}
		return fmt.Sprintf("Array_[%s]", strings.Join(parts, ", "))
	default:
		return fmt.Sprintf("Array_%v", value)
	}
}

// formatTupleLiteral formats a tuple literal.
func formatTupleLiteral(value interface{}) string {
	switch v := value.(type) {
	case []interface{}:
		parts := make([]string, len(v))
		for i, elem := range v {
			parts[i] = formatLiteralElement(elem)
		}
		return fmt.Sprintf("Tuple_(%s)", strings.Join(parts, ", "))
	default:
		return fmt.Sprintf("Tuple_%v", value)
	}
}

// explainInListAsTuple formats an IN list as a Tuple literal.
func explainInListAsTuple(b *strings.Builder, list []Expression, depth int) {
	indent := strings.Repeat(" ", depth)

	// Build the tuple elements
	parts := make([]string, len(list))
	for i, elem := range list {
		if lit, ok := elem.(*Literal); ok {
			switch lit.Type {
			case LiteralString:
				parts[i] = fmt.Sprintf("'%v'", lit.Value)
			case LiteralInteger:
				parts[i] = fmt.Sprintf("UInt64_%v", lit.Value)
			case LiteralFloat:
				parts[i] = fmt.Sprintf("Float64_%v", lit.Value)
			default:
				parts[i] = fmt.Sprintf("%v", lit.Value)
			}
		} else {
			parts[i] = fmt.Sprintf("%v", elem)
		}
	}

	fmt.Fprintf(b, "%sLiteral Tuple_(%s)\n", indent, strings.Join(parts, ", "))
}

// formatLiteralElement formats a single literal element.
func formatLiteralElement(elem interface{}) string {
	switch e := elem.(type) {
	case string:
		return fmt.Sprintf("\\'%s\\'", e)
	case int, int64, uint64:
		return fmt.Sprintf("UInt64_%v", e)
	case float64:
		return fmt.Sprintf("Float64_%v", e)
	case bool:
		if e {
			return "Bool_1"
		}
		return "Bool_0"
	default:
		return fmt.Sprintf("%v", e)
	}
}

// explainFunctionCall formats a function call.
func explainFunctionCall(b *strings.Builder, fn *FunctionCall, depth int) {
	explainFunctionCallWithAlias(b, fn, fn.Alias, depth)
}

// explainFunctionCallWithAlias formats a function call with an optional alias.
func explainFunctionCallWithAlias(b *strings.Builder, fn *FunctionCall, alias string, depth int) {
	indent := strings.Repeat(" ", depth)
	name := fn.Name

	// Count children: always 1 for ExpressionList, plus 1 for window spec if present
	// ClickHouse always shows (children 1) with ExpressionList even for empty arg functions
	children := 1 // Always have ExpressionList
	if fn.Over != nil {
		children++
	}

	aliasSuffix := ""
	if alias != "" {
		aliasSuffix = fmt.Sprintf(" (alias %s)", alias)
	}

	fmt.Fprintf(b, "%sFunction %s%s (children %d)\n", indent, name, aliasSuffix, children)

	// Combine parameters and arguments
	allArgs := append(fn.Parameters, fn.Arguments...)
	if len(allArgs) > 0 {
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(allArgs))
		for _, arg := range allArgs {
			explainNode(b, arg, depth+2)
		}
	} else {
		// Empty argument list
		fmt.Fprintf(b, "%s ExpressionList\n", indent)
	}

	// Window specification
	if fn.Over != nil {
		explainWindowSpec(b, fn.Over, depth+1)
	}
}

// explainWindowSpec formats a window specification.
func explainWindowSpec(b *strings.Builder, spec *WindowSpec, depth int) {
	indent := strings.Repeat(" ", depth)

	// Count children: partition by + order by + frame bounds
	children := 0
	if len(spec.PartitionBy) > 0 {
		children++
	}
	if len(spec.OrderBy) > 0 {
		children++
	}
	// Count frame bound children
	if spec.Frame != nil {
		if spec.Frame.StartBound != nil && spec.Frame.StartBound.Offset != nil {
			children++
		}
		if spec.Frame.EndBound != nil && spec.Frame.EndBound.Offset != nil {
			children++
		}
	}

	if children > 0 {
		fmt.Fprintf(b, "%sWindowDefinition (children %d)\n", indent, children)
	} else {
		fmt.Fprintf(b, "%sWindowDefinition\n", indent)
	}

	// Partition by
	if len(spec.PartitionBy) > 0 {
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(spec.PartitionBy))
		for _, expr := range spec.PartitionBy {
			explainNode(b, expr, depth+2)
		}
	}

	// Order by
	if len(spec.OrderBy) > 0 {
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(spec.OrderBy))
		for _, elem := range spec.OrderBy {
			explainOrderByElement(b, elem, depth+2)
		}
	}

	// Frame bounds
	if spec.Frame != nil {
		if spec.Frame.StartBound != nil && spec.Frame.StartBound.Offset != nil {
			explainNode(b, spec.Frame.StartBound.Offset, depth+1)
		}
		if spec.Frame.EndBound != nil && spec.Frame.EndBound.Offset != nil {
			explainNode(b, spec.Frame.EndBound.Offset, depth+1)
		}
	}
}

// explainTableJoin formats a table join.
func explainTableJoin(b *strings.Builder, join *TableJoin, depth int) {
	indent := strings.Repeat(" ", depth)
	children := 0
	if join.On != nil {
		children++
	}
	if len(join.Using) > 0 {
		children++
	}
	if children > 0 {
		fmt.Fprintf(b, "%sTableJoin (children %d)\n", indent, children)
	} else {
		fmt.Fprintf(b, "%sTableJoin\n", indent)
	}
	if join.On != nil {
		explainNode(b, join.On, depth+1)
	}
	if len(join.Using) > 0 {
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(join.Using))
		for _, col := range join.Using {
			explainNode(b, col, depth+2)
		}
	}
}

// explainArrayJoinClause formats an array join as a table element.
func explainArrayJoinClause(b *strings.Builder, aj *ArrayJoinClause, depth int) {
	// Array join is already represented in TablesInSelectQuery
	// This is just for when it's encountered directly
	indent := strings.Repeat(" ", depth)
	fmt.Fprintf(b, "%sArrayJoin (children 1)\n", indent)
	fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, len(aj.Columns))
	for _, col := range aj.Columns {
		explainNode(b, col, depth+2)
	}
}

// explainOrderByElement formats an order by element.
func explainOrderByElement(b *strings.Builder, elem *OrderByElement, depth int) {
	indent := strings.Repeat(" ", depth)

	// Count children: expression + optional FillFrom, FillTo, FillStep
	children := 1
	if elem.FillFrom != nil {
		children++
	}
	if elem.FillTo != nil {
		children++
	}
	if elem.FillStep != nil {
		children++
	}

	fmt.Fprintf(b, "%sOrderByElement (children %d)\n", indent, children)
	explainNode(b, elem.Expression, depth+1)

	if elem.FillFrom != nil {
		explainNode(b, elem.FillFrom, depth+1)
	}
	if elem.FillTo != nil {
		explainNode(b, elem.FillTo, depth+1)
	}
	if elem.FillStep != nil {
		explainNode(b, elem.FillStep, depth+1)
	}
}

// explainCaseExpr formats a CASE expression.
func explainCaseExpr(b *strings.Builder, c *CaseExpr, depth int) {
	indent := strings.Repeat(" ", depth)
	// CASE is represented as multiIf or caseWithExpression
	aliasSuffix := ""
	if c.Alias != "" {
		aliasSuffix = fmt.Sprintf(" (alias %s)", c.Alias)
	}

	if c.Operand != nil {
		// CASE x WHEN ... -> caseWithExpression
		children := 1 + len(c.Whens)*2
		if c.Else != nil {
			children++
		}
		fmt.Fprintf(b, "%sFunction caseWithExpression%s (children 1)\n", indent, aliasSuffix)
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, children)
		explainNode(b, c.Operand, depth+2)
		for _, when := range c.Whens {
			explainNode(b, when.Condition, depth+2)
			explainNode(b, when.Result, depth+2)
		}
		if c.Else != nil {
			explainNode(b, c.Else, depth+2)
		}
	} else {
		// CASE WHEN ... -> multiIf
		children := len(c.Whens) * 2
		if c.Else != nil {
			children++
		}
		fmt.Fprintf(b, "%sFunction multiIf%s (children 1)\n", indent, aliasSuffix)
		fmt.Fprintf(b, "%s ExpressionList (children %d)\n", indent, children)
		for _, when := range c.Whens {
			explainNode(b, when.Condition, depth+2)
			explainNode(b, when.Result, depth+2)
		}
		if c.Else != nil {
			explainNode(b, c.Else, depth+2)
		}
	}
}

// explainCastExpr formats a CAST expression.
func explainCastExpr(b *strings.Builder, c *CastExpr, depth int) {
	indent := strings.Repeat(" ", depth)
	aliasSuffix := ""
	if c.Alias != "" {
		aliasSuffix = fmt.Sprintf(" (alias %s)", c.Alias)
	}
	fmt.Fprintf(b, "%sFunction CAST%s (children 1)\n", indent, aliasSuffix)
	fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
	explainNode(b, c.Expr, depth+2)
	// Type is represented as a Literal string
	fmt.Fprintf(b, "%s  Literal \\'%s\\'\n", indent, c.Type.Name)
}

// explainLambda formats a lambda expression.
func explainLambda(b *strings.Builder, l *Lambda, depth int) {
	indent := strings.Repeat(" ", depth)
	fmt.Fprintf(b, "%sFunction lambda (children 1)\n", indent)
	fmt.Fprintf(b, "%s ExpressionList (children 2)\n", indent)
	// Parameters as tuple
	fmt.Fprintf(b, "%s  Function tuple (children 1)\n", indent)
	fmt.Fprintf(b, "%s   ExpressionList (children %d)\n", indent, len(l.Parameters))
	for _, param := range l.Parameters {
		fmt.Fprintf(b, "%s    Identifier %s\n", indent, param)
	}
	// Body
	explainNode(b, l.Body, depth+2)
}

// countSelectQueryChildren counts the non-nil children of a SelectQuery.
func countSelectQueryChildren(s *SelectQuery) int {
	count := 0
	if len(s.With) > 0 {
		count++
	}
	if len(s.Columns) > 0 {
		count++
	}
	// From and ArrayJoin are combined into one TablesInSelectQuery
	if s.From != nil || s.ArrayJoin != nil {
		count++
	}
	if s.Where != nil {
		count++
	}
	if len(s.GroupBy) > 0 {
		count++
	}
	if s.Having != nil {
		count++
	}
	if len(s.OrderBy) > 0 {
		count++
	}
	if s.Limit != nil {
		count++
	}
	if s.Offset != nil {
		count++
	}
	return count
}

// explainTablesWithArrayJoin outputs TablesInSelectQuery with ArrayJoin integrated.
func explainTablesWithArrayJoin(b *strings.Builder, from *TablesInSelectQuery, arrayJoin *ArrayJoinClause, depth int) {
	indent := strings.Repeat(" ", depth)

	tableCount := 0
	if from != nil {
		tableCount = len(from.Tables)
	}
	if arrayJoin != nil {
		tableCount++
	}

	fmt.Fprintf(b, "%sTablesInSelectQuery (children %d)\n", indent, tableCount)

	if from != nil {
		for _, table := range from.Tables {
			explainNode(b, table, depth+1)
		}
	}

	if arrayJoin != nil {
		// ArrayJoin is output as a TablesInSelectQueryElement
		fmt.Fprintf(b, "%s TablesInSelectQueryElement (children 1)\n", indent)
		explainArrayJoinClause(b, arrayJoin, depth+2)
	}
}

// binaryOpToFunction maps binary operators to their function names.
func binaryOpToFunction(op string) string {
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
	case "<=":
		return "lessOrEquals"
	case ">":
		return "greater"
	case ">=":
		return "greaterOrEquals"
	case "AND":
		return "and"
	case "OR":
		return "or"
	case "LIKE":
		return "like"
	case "ILIKE":
		return "ilike"
	case "NOT LIKE":
		return "notLike"
	case "NOT ILIKE":
		return "notILike"
	case "IN":
		return "in"
	case "NOT IN":
		return "notIn"
	case "GLOBAL IN":
		return "globalIn"
	case "GLOBAL NOT IN":
		return "globalNotIn"
	default:
		return op
	}
}

// unaryOpToFunction maps unary operators to their function names.
func unaryOpToFunction(op string) string {
	switch op {
	case "-":
		return "negate"
	case "NOT":
		return "not"
	case "~":
		return "bitNot"
	default:
		return op
	}
}

// extractFieldToFunction maps EXTRACT fields to function names.
func extractFieldToFunction(field string) string {
	switch strings.ToUpper(field) {
	case "YEAR":
		return "toYear"
	case "MONTH":
		return "toMonth"
	case "DAY":
		return "toDayOfMonth"
	case "HOUR":
		return "toHour"
	case "MINUTE":
		return "toMinute"
	case "SECOND":
		return "toSecond"
	default:
		return "to" + strings.Title(strings.ToLower(field))
	}
}

// explainAlterCommand formats an ALTER command.
func explainAlterCommand(b *strings.Builder, cmd *AlterCommand, depth int) {
	indent := strings.Repeat(" ", depth)

	children := 0
	if cmd.Column != nil {
		children++
	}
	if cmd.ColumnName != "" && cmd.Type != AlterAddColumn && cmd.Type != AlterModifyColumn {
		children++
	}
	if cmd.AfterColumn != "" {
		children++
	}
	if cmd.Constraint != nil {
		children++
	}
	if cmd.IndexExpr != nil {
		children++
	}

	if children > 0 {
		fmt.Fprintf(b, "%sAlterCommand %s (children %d)\n", indent, cmd.Type, children)
	} else {
		fmt.Fprintf(b, "%sAlterCommand %s\n", indent, cmd.Type)
	}

	if cmd.Column != nil {
		explainColumnDeclaration(b, cmd.Column, depth+1)
	}
	if cmd.ColumnName != "" && cmd.Type != AlterAddColumn && cmd.Type != AlterModifyColumn {
		fmt.Fprintf(b, "%s Identifier %s\n", indent, cmd.ColumnName)
	}
	if cmd.AfterColumn != "" {
		fmt.Fprintf(b, "%s Identifier %s\n", indent, cmd.AfterColumn)
	}
	if cmd.Constraint != nil {
		explainConstraint(b, cmd.Constraint, depth+1)
	}
	if cmd.IndexExpr != nil {
		fmt.Fprintf(b, "%s Index (children 2)\n", indent)
		explainNode(b, cmd.IndexExpr, depth+2)
		if cmd.IndexType != "" {
			fmt.Fprintf(b, "%s  Function %s (children 1)\n", indent, cmd.IndexType)
			fmt.Fprintf(b, "%s   ExpressionList\n", indent)
		}
	}
}

// explainColumnDeclaration formats a column declaration.
func explainColumnDeclaration(b *strings.Builder, col *ColumnDeclaration, depth int) {
	indent := strings.Repeat(" ", depth)

	children := 0
	if col.Type != nil {
		children++
	}
	if col.Default != nil {
		children++
	}

	fmt.Fprintf(b, "%sColumnDeclaration %s (children %d)\n", indent, col.Name, children)
	if col.Type != nil {
		fmt.Fprintf(b, "%s DataType %s\n", indent, col.Type.Name)
	}
	if col.Default != nil {
		explainNode(b, col.Default, depth+1)
	}
}

// explainConstraint formats a constraint.
func explainConstraint(b *strings.Builder, c *Constraint, depth int) {
	indent := strings.Repeat(" ", depth)
	fmt.Fprintf(b, "%sConstraint (children 1)\n", indent)
	explainNode(b, c.Expression, depth+1)
}

// explainCreateQuery formats a CREATE query.
func explainCreateQuery(b *strings.Builder, n *CreateQuery, depth int) {
	indent := strings.Repeat(" ", depth)

	if n.CreateDatabase {
		fmt.Fprintf(b, "%sCreateQuery %s  (children 1)\n", indent, n.Database)
		fmt.Fprintf(b, "%s Identifier %s\n", indent, n.Database)
		return
	}

	var name string
	if n.View != "" {
		name = n.View
	} else {
		name = n.Table
	}
	if n.Database != "" {
		name = n.Database + "." + name
	}

	children := 1 // identifier
	if len(n.Columns) > 0 {
		children++
	}
	if n.Engine != nil || len(n.OrderBy) > 0 {
		children++
	}
	if n.AsSelect != nil {
		children++
	}

	fmt.Fprintf(b, "%sCreateQuery %s (children %d)\n", indent, name, children)
	fmt.Fprintf(b, "%s Identifier %s\n", indent, name)

	if len(n.Columns) > 0 {
		fmt.Fprintf(b, "%s Columns definition (children 1)\n", indent)
		fmt.Fprintf(b, "%s  ExpressionList (children %d)\n", indent, len(n.Columns))
		for _, col := range n.Columns {
			explainColumnDeclaration(b, col, depth+3)
		}
	}

	if n.Engine != nil || len(n.OrderBy) > 0 {
		storageChildren := 0
		if n.Engine != nil {
			storageChildren++
		}
		if len(n.OrderBy) > 0 {
			storageChildren++
		}
		fmt.Fprintf(b, "%s Storage definition (children %d)\n", indent, storageChildren)
		if n.Engine != nil {
			fmt.Fprintf(b, "%s  Function %s (children 1)\n", indent, n.Engine.Name)
			fmt.Fprintf(b, "%s   ExpressionList\n", indent)
		}
		if len(n.OrderBy) > 0 {
			// For simple ORDER BY, just output the identifier
			if len(n.OrderBy) == 1 {
				if id, ok := n.OrderBy[0].(*Identifier); ok {
					fmt.Fprintf(b, "%s  Identifier %s\n", indent, id.Name())
				} else {
					explainNode(b, n.OrderBy[0], depth+2)
				}
			} else {
				fmt.Fprintf(b, "%s  ExpressionList (children %d)\n", indent, len(n.OrderBy))
				for _, expr := range n.OrderBy {
					explainNode(b, expr, depth+3)
				}
			}
		}
	}

	if n.AsSelect != nil {
		explainNode(b, n.AsSelect, depth+1)
	}
}
