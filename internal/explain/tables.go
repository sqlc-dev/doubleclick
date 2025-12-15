package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

func explainTablesInSelectQuery(sb *strings.Builder, n *ast.TablesInSelectQuery, indent string, depth int) {
	fmt.Fprintf(sb, "%sTablesInSelectQuery (children %d)\n", indent, len(n.Tables))
	for _, t := range n.Tables {
		Node(sb, t, depth+1)
	}
}

func explainTablesInSelectQueryElement(sb *strings.Builder, n *ast.TablesInSelectQueryElement, indent string, depth int) {
	children := 1 // table
	if n.Join != nil {
		children++
	}
	fmt.Fprintf(sb, "%sTablesInSelectQueryElement (children %d)\n", indent, children)
	if n.Table != nil {
		Node(sb, n.Table, depth+1)
	}
	if n.Join != nil {
		Node(sb, n.Join, depth+1)
	}
}

func explainTableExpression(sb *strings.Builder, n *ast.TableExpression, indent string, depth int) {
	children := 1 // table
	fmt.Fprintf(sb, "%sTableExpression (children %d)\n", indent, children)
	// If there's a subquery with an alias, pass the alias to the subquery output
	if subq, ok := n.Table.(*ast.Subquery); ok && n.Alias != "" {
		fmt.Fprintf(sb, "%s Subquery (alias %s) (children %d)\n", indent, n.Alias, 1)
		Node(sb, subq.Query, depth+2)
	} else if fn, ok := n.Table.(*ast.FunctionCall); ok && n.Alias != "" {
		// Table function with alias
		explainFunctionCallWithAlias(sb, fn, n.Alias, indent+" ", depth+1)
	} else if ti, ok := n.Table.(*ast.TableIdentifier); ok && n.Alias != "" {
		// Table identifier with alias
		explainTableIdentifierWithAlias(sb, ti, n.Alias, indent+" ")
	} else {
		Node(sb, n.Table, depth+1)
	}
}

func explainTableIdentifierWithAlias(sb *strings.Builder, n *ast.TableIdentifier, alias string, indent string) {
	name := n.Table
	if n.Database != "" {
		name = n.Database + "." + n.Table
	}
	fmt.Fprintf(sb, "%sTableIdentifier %s (alias %s)\n", indent, name, alias)
}

func explainTableIdentifier(sb *strings.Builder, n *ast.TableIdentifier, indent string) {
	name := n.Table
	if n.Database != "" {
		name = n.Database + "." + n.Table
	}
	fmt.Fprintf(sb, "%sTableIdentifier %s\n", indent, name)
}

func explainArrayJoinClause(sb *strings.Builder, n *ast.ArrayJoinClause, indent string, depth int) {
	fmt.Fprintf(sb, "%sArrayJoin (children %d)\n", indent, 1)
	fmt.Fprintf(sb, "%s ExpressionList", indent)
	if len(n.Columns) > 0 {
		fmt.Fprintf(sb, " (children %d)", len(n.Columns))
	}
	fmt.Fprintln(sb)
	for _, col := range n.Columns {
		Node(sb, col, depth+2)
	}
}

func explainTableJoin(sb *strings.Builder, n *ast.TableJoin, indent string, depth int) {
	// TableJoin is part of TablesInSelectQueryElement
	// ClickHouse EXPLAIN AST doesn't show join type in the output
	children := 0
	if n.On != nil {
		children++
	}
	if len(n.Using) > 0 {
		children++
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sTableJoin (children %d)\n", indent, children)
	} else {
		fmt.Fprintf(sb, "%sTableJoin\n", indent)
	}
	if n.On != nil {
		Node(sb, n.On, depth+1)
	}
	if len(n.Using) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Using))
		for _, u := range n.Using {
			Node(sb, u, depth+2)
		}
	}
}
