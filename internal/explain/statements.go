package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

func explainCreateQuery(sb *strings.Builder, n *ast.CreateQuery, indent string, depth int) {
	name := n.Table
	if n.View != "" {
		name = n.View
	}
	if n.CreateDatabase {
		name = n.Database
	}
	// Count children: name + columns + engine/storage
	children := 1 // name identifier
	if len(n.Columns) > 0 {
		children++
	}
	if n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 {
		children++
	}
	if n.AsSelect != nil {
		children++
	}
	fmt.Fprintf(sb, "%sCreateQuery %s (children %d)\n", indent, name, children)
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	if len(n.Columns) > 0 {
		fmt.Fprintf(sb, "%s Columns definition (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Columns))
		for _, col := range n.Columns {
			Column(sb, col, depth+3)
		}
	}
	if n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 {
		storageChildren := 0
		if n.Engine != nil {
			storageChildren++
		}
		if len(n.OrderBy) > 0 {
			storageChildren++
		}
		if len(n.PrimaryKey) > 0 {
			storageChildren++
		}
		fmt.Fprintf(sb, "%s Storage definition (children %d)\n", indent, storageChildren)
		if n.Engine != nil {
			fmt.Fprintf(sb, "%s  Function %s\n", indent, n.Engine.Name)
		}
		if len(n.OrderBy) > 0 {
			if len(n.OrderBy) == 1 {
				if ident, ok := n.OrderBy[0].(*ast.Identifier); ok {
					fmt.Fprintf(sb, "%s  Identifier %s\n", indent, ident.Name())
				} else {
					Node(sb, n.OrderBy[0], depth+2)
				}
			} else {
				fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.OrderBy))
				for _, o := range n.OrderBy {
					Node(sb, o, depth+4)
				}
			}
		}
	}
	if n.AsSelect != nil {
		fmt.Fprintf(sb, "%s Subquery (children %d)\n", indent, 1)
		Node(sb, n.AsSelect, depth+2)
	}
}

func explainDropQuery(sb *strings.Builder, n *ast.DropQuery, indent string) {
	name := n.Table
	if n.View != "" {
		name = n.View
	}
	if n.DropDatabase {
		name = n.Database
	}
	fmt.Fprintf(sb, "%sDropQuery  %s (children %d)\n", indent, name, 1)
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
}

func explainSetQuery(sb *strings.Builder, indent string) {
	fmt.Fprintf(sb, "%sSet\n", indent)
}

func explainSystemQuery(sb *strings.Builder, indent string) {
	fmt.Fprintf(sb, "%sSYSTEM query\n", indent)
}

func explainExplainQuery(sb *strings.Builder, n *ast.ExplainQuery, indent string, depth int) {
	fmt.Fprintf(sb, "%sExplain %s (children %d)\n", indent, n.ExplainType, 1)
	Node(sb, n.Statement, depth+1)
}

func explainShowQuery(sb *strings.Builder, n *ast.ShowQuery, indent string) {
	// Capitalize ShowType correctly for display
	showType := strings.Title(strings.ToLower(string(n.ShowType)))
	fmt.Fprintf(sb, "%sShow%s\n", indent, showType)
}

func explainUseQuery(sb *strings.Builder, n *ast.UseQuery, indent string) {
	fmt.Fprintf(sb, "%sUse %s\n", indent, n.Database)
}

func explainDescribeQuery(sb *strings.Builder, n *ast.DescribeQuery, indent string) {
	name := n.Table
	if n.Database != "" {
		name = n.Database + "." + n.Table
	}
	fmt.Fprintf(sb, "%sDescribe %s\n", indent, name)
}

func explainDataType(sb *strings.Builder, n *ast.DataType, indent string) {
	fmt.Fprintf(sb, "%sDataType %s\n", indent, FormatDataType(n))
}

func explainParameter(sb *strings.Builder, n *ast.Parameter, indent string) {
	if n.Name != "" {
		fmt.Fprintf(sb, "%sQueryParameter %s\n", indent, n.Name)
	} else {
		fmt.Fprintf(sb, "%sQueryParameter\n", indent)
	}
}
