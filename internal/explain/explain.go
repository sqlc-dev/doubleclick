// Package explain provides EXPLAIN AST output functionality for ClickHouse SQL.
package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// inSubqueryContext is a package-level flag to track when we're inside a Subquery
// This affects how negated literals with aliases are formatted
var inSubqueryContext bool

// Explain returns the EXPLAIN AST output for a statement, matching ClickHouse's format.
func Explain(stmt ast.Statement) string {
	var sb strings.Builder
	Node(&sb, stmt, 0)
	return sb.String()
}

// Node writes the EXPLAIN AST output for an AST node.
func Node(sb *strings.Builder, node interface{}, depth int) {
	if node == nil {
		// nil can represent an empty tuple in function arguments
		indent := strings.Repeat(" ", depth)
		fmt.Fprintf(sb, "%sFunction tuple (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList\n", indent)
		return
	}

	indent := strings.Repeat(" ", depth)

	switch n := node.(type) {
	// Select statements
	case *ast.SelectWithUnionQuery:
		explainSelectWithUnionQuery(sb, n, indent, depth)
	case *ast.SelectIntersectExceptQuery:
		explainSelectIntersectExceptQuery(sb, n, indent, depth)
	case *ast.SelectQuery:
		explainSelectQuery(sb, n, indent, depth)

	// Tables
	case *ast.TablesInSelectQuery:
		explainTablesInSelectQuery(sb, n, indent, depth)
	case *ast.TablesInSelectQueryElement:
		explainTablesInSelectQueryElement(sb, n, indent, depth)
	case *ast.TableExpression:
		explainTableExpression(sb, n, indent, depth)
	case *ast.TableIdentifier:
		explainTableIdentifier(sb, n, indent)
	case *ast.ArrayJoinClause:
		explainArrayJoinClause(sb, n, indent, depth)
	case *ast.TableJoin:
		explainTableJoin(sb, n, indent, depth)

	// Expressions
	case *ast.OrderByElement:
		explainOrderByElement(sb, n, indent, depth)
	case *ast.Identifier:
		explainIdentifier(sb, n, indent)
	case *ast.Literal:
		explainLiteral(sb, n, indent, depth)
	case *ast.BinaryExpr:
		explainBinaryExpr(sb, n, indent, depth)
	case *ast.UnaryExpr:
		explainUnaryExpr(sb, n, indent, depth)
	case *ast.Subquery:
		explainSubquery(sb, n, indent, depth)
	case *ast.AliasedExpr:
		explainAliasedExpr(sb, n, depth)
	case *ast.WithElement:
		explainWithElement(sb, n, indent, depth)
	case *ast.Asterisk:
		explainAsterisk(sb, n, indent, depth)
	case *ast.ColumnsMatcher:
		fmt.Fprintf(sb, "%sColumnsRegexpMatcher\n", indent)

	// Functions
	case *ast.FunctionCall:
		explainFunctionCall(sb, n, indent, depth)
	case *ast.Lambda:
		explainLambda(sb, n, indent, depth)
	case *ast.CastExpr:
		explainCastExpr(sb, n, indent, depth)
	case *ast.InExpr:
		explainInExpr(sb, n, indent, depth)
	case *ast.TernaryExpr:
		explainTernaryExpr(sb, n, indent, depth)
	case *ast.ArrayAccess:
		explainArrayAccess(sb, n, indent, depth)
	case *ast.TupleAccess:
		explainTupleAccess(sb, n, indent, depth)
	case *ast.LikeExpr:
		explainLikeExpr(sb, n, indent, depth)
	case *ast.BetweenExpr:
		explainBetweenExpr(sb, n, indent, depth)
	case *ast.IsNullExpr:
		explainIsNullExpr(sb, n, indent, depth)
	case *ast.CaseExpr:
		explainCaseExpr(sb, n, indent, depth)
	case *ast.IntervalExpr:
		explainIntervalExpr(sb, n, "", indent, depth)
	case *ast.ExistsExpr:
		explainExistsExpr(sb, n, indent, depth)
	case *ast.ExtractExpr:
		explainExtractExpr(sb, n, indent, depth)

	// DDL statements
	case *ast.InsertQuery:
		explainInsertQuery(sb, n, indent, depth)
	case *ast.CreateQuery:
		explainCreateQuery(sb, n, indent, depth)
	case *ast.DropQuery:
		explainDropQuery(sb, n, indent, depth)
	case *ast.RenameQuery:
		explainRenameQuery(sb, n, indent, depth)
	case *ast.SetQuery:
		explainSetQuery(sb, indent)
	case *ast.SystemQuery:
		explainSystemQuery(sb, n, indent)
	case *ast.ExplainQuery:
		explainExplainQuery(sb, n, indent, depth)
	case *ast.ShowQuery:
		explainShowQuery(sb, n, indent)
	case *ast.ShowPrivilegesQuery:
		fmt.Fprintf(sb, "%sShowPrivilegesQuery\n", indent)
	case *ast.ShowCreateQuotaQuery:
		fmt.Fprintf(sb, "%sSHOW CREATE QUOTA query\n", indent)
	case *ast.UseQuery:
		explainUseQuery(sb, n, indent)
	case *ast.DescribeQuery:
		explainDescribeQuery(sb, n, indent)
	case *ast.ExistsQuery:
		explainExistsTableQuery(sb, n, indent)
	case *ast.DetachQuery:
		explainDetachQuery(sb, n, indent)
	case *ast.AttachQuery:
		explainAttachQuery(sb, n, indent)
	case *ast.AlterQuery:
		explainAlterQuery(sb, n, indent, depth)
	case *ast.OptimizeQuery:
		explainOptimizeQuery(sb, n, indent)
	case *ast.TruncateQuery:
		explainTruncateQuery(sb, n, indent)

	// Types
	case *ast.DataType:
		explainDataType(sb, n, indent, depth)
	case *ast.NameTypePair:
		explainNameTypePair(sb, n, indent, depth)
	case *ast.Parameter:
		explainParameter(sb, n, indent)

	// Dictionary types
	case *ast.DictionaryAttributeDeclaration:
		explainDictionaryAttributeDeclaration(sb, n, indent, depth)
	case *ast.DictionaryDefinition:
		explainDictionaryDefinition(sb, n, indent, depth)
	case *ast.DictionarySource:
		explainDictionarySource(sb, n, indent, depth)
	case *ast.KeyValuePair:
		explainKeyValuePair(sb, n, indent, depth)
	case *ast.DictionaryLifetime:
		explainDictionaryLifetime(sb, n, indent, depth)
	case *ast.DictionaryLayout:
		explainDictionaryLayout(sb, n, indent, depth)
	case *ast.DictionaryRange:
		explainDictionaryRange(sb, n, indent, depth)

	default:
		// For unhandled types, just print the type name
		fmt.Fprintf(sb, "%s%T\n", indent, node)
	}
}

// TablesWithArrayJoin handles FROM and ARRAY JOIN together as TablesInSelectQuery
func TablesWithArrayJoin(sb *strings.Builder, from *ast.TablesInSelectQuery, arrayJoin *ast.ArrayJoinClause, depth int) {
	indent := strings.Repeat(" ", depth)

	tableCount := 0
	if from != nil {
		tableCount = len(from.Tables)
	}
	if arrayJoin != nil {
		tableCount++
	}

	fmt.Fprintf(sb, "%sTablesInSelectQuery (children %d)\n", indent, tableCount)

	if from != nil {
		for _, t := range from.Tables {
			Node(sb, t, depth+1)
		}
	}

	if arrayJoin != nil {
		// ARRAY JOIN is wrapped in TablesInSelectQueryElement
		fmt.Fprintf(sb, "%s TablesInSelectQueryElement (children %d)\n", indent, 1)
		Node(sb, arrayJoin, depth+2)
	}
}

// Column handles column declarations
func Column(sb *strings.Builder, col *ast.ColumnDeclaration, depth int) {
	indent := strings.Repeat(" ", depth)
	children := 0
	if col.Type != nil {
		children++
	}
	// EPHEMERAL columns without explicit default get defaultValueOfTypeName
	hasEphemeralDefault := col.DefaultKind == "EPHEMERAL" && col.Default == nil
	if col.Default != nil || hasEphemeralDefault {
		children++
	}
	if col.Codec != nil {
		children++
	}
	fmt.Fprintf(sb, "%sColumnDeclaration %s (children %d)\n", indent, col.Name, children)
	if col.Type != nil {
		Node(sb, col.Type, depth+1)
	}
	if col.Default != nil {
		Node(sb, col.Default, depth+1)
	} else if hasEphemeralDefault {
		// EPHEMERAL columns without explicit default value show defaultValueOfTypeName function
		fmt.Fprintf(sb, "%s Function defaultValueOfTypeName\n", indent)
	}
	if col.Codec != nil {
		explainCodecExpr(sb, col.Codec, indent+" ", depth+1)
	}
}

// explainCodecExpr handles CODEC expressions in column declarations
func explainCodecExpr(sb *strings.Builder, codec *ast.CodecExpr, indent string, depth int) {
	// CODEC is rendered as a Function with one child (ExpressionList of codecs)
	fmt.Fprintf(sb, "%sFunction CODEC (children 1)\n", indent)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(codec.Codecs))
	for _, c := range codec.Codecs {
		explainCodecFunction(sb, c, indent+"  ", depth+2)
	}
}

// explainCodecFunction handles individual codec functions (e.g., LZ4, ZSTD(10), Gorilla(1))
func explainCodecFunction(sb *strings.Builder, fn *ast.FunctionCall, indent string, depth int) {
	if len(fn.Arguments) == 0 {
		// Codec without parameters: just the function name
		fmt.Fprintf(sb, "%sFunction %s\n", indent, fn.Name)
	} else {
		// Codec with parameters: function with ExpressionList of arguments
		fmt.Fprintf(sb, "%sFunction %s (children 1)\n", indent, fn.Name)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(fn.Arguments))
		for _, arg := range fn.Arguments {
			Node(sb, arg, depth+2)
		}
	}
}

func Index(sb *strings.Builder, idx *ast.IndexDefinition, depth int) {
	indent := strings.Repeat(" ", depth)
	children := 0
	if idx.Expression != nil {
		children++
	}
	if idx.Type != nil {
		children++
	}
	fmt.Fprintf(sb, "%sIndex (children %d)\n", indent, children)
	if idx.Expression != nil {
		// Expression is typically an identifier
		if ident, ok := idx.Expression.(*ast.Identifier); ok {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, ident.Name())
		} else {
			Node(sb, idx.Expression, depth+1)
		}
	}
	if idx.Type != nil {
		// Type is a function like minmax, bloom_filter, etc.
		explainFunctionCall(sb, idx.Type, indent+" ", depth+1)
	}
}
