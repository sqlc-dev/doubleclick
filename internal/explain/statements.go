package explain

import (
	"fmt"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
)

func explainInsertQuery(sb *strings.Builder, n *ast.InsertQuery, indent string, depth int) {
	// Count children
	children := 0
	if n.Function != nil {
		children++
	} else if n.Table != "" {
		children++ // Table identifier
	}
	if n.Select != nil {
		children++
	}
	if n.HasSettings {
		children++
	}
	// Note: InsertQuery uses 3 spaces after name in ClickHouse explain
	fmt.Fprintf(sb, "%sInsertQuery   (children %d)\n", indent, children)

	if n.Function != nil {
		Node(sb, n.Function, depth+1)
	} else if n.Table != "" {
		name := n.Table
		if n.Database != "" {
			name = n.Database + "." + n.Table
		}
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	}

	if n.Select != nil {
		Node(sb, n.Select, depth+1)
	}

	if n.HasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

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
	if n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 || n.PartitionBy != nil {
		children++
	}
	if n.AsSelect != nil {
		children++
	}
	// ClickHouse adds an extra space before (children N) for CREATE DATABASE
	if n.CreateDatabase {
		fmt.Fprintf(sb, "%sCreateQuery %s  (children %d)\n", indent, name, children)
	} else {
		fmt.Fprintf(sb, "%sCreateQuery %s (children %d)\n", indent, name, children)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	if len(n.Columns) > 0 {
		fmt.Fprintf(sb, "%s Columns definition (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Columns))
		for _, col := range n.Columns {
			Column(sb, col, depth+3)
		}
	}
	if n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 || n.PartitionBy != nil || len(n.Settings) > 0 {
		storageChildren := 0
		if n.Engine != nil {
			storageChildren++
		}
		if n.PartitionBy != nil {
			storageChildren++
		}
		if len(n.OrderBy) > 0 {
			storageChildren++
		}
		if len(n.PrimaryKey) > 0 {
			storageChildren++
		}
		if len(n.Settings) > 0 {
			storageChildren++
		}
		fmt.Fprintf(sb, "%s Storage definition (children %d)\n", indent, storageChildren)
		if n.Engine != nil {
			if n.Engine.HasParentheses {
				fmt.Fprintf(sb, "%s  Function %s (children %d)\n", indent, n.Engine.Name, 1)
				if len(n.Engine.Parameters) > 0 {
					fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.Engine.Parameters))
					for _, param := range n.Engine.Parameters {
						Node(sb, param, depth+4)
					}
				} else {
					fmt.Fprintf(sb, "%s   ExpressionList\n", indent)
				}
			} else {
				fmt.Fprintf(sb, "%s  Function %s\n", indent, n.Engine.Name)
			}
		}
		if n.PartitionBy != nil {
			if ident, ok := n.PartitionBy.(*ast.Identifier); ok {
				fmt.Fprintf(sb, "%s  Identifier %s\n", indent, ident.Name())
			} else {
				Node(sb, n.PartitionBy, depth+2)
			}
		}
		if len(n.OrderBy) > 0 {
			if len(n.OrderBy) == 1 {
				if ident, ok := n.OrderBy[0].(*ast.Identifier); ok {
					fmt.Fprintf(sb, "%s  Identifier %s\n", indent, ident.Name())
				} else if lit, ok := n.OrderBy[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
					// Handle tuple literal (including empty tuple from ORDER BY ())
					exprs, _ := lit.Value.([]ast.Expression)
					fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
					if len(exprs) > 0 {
						fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(exprs))
						for _, e := range exprs {
							Node(sb, e, depth+4)
						}
					} else {
						fmt.Fprintf(sb, "%s   ExpressionList\n", indent)
					}
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
		if len(n.PrimaryKey) > 0 {
			if len(n.PrimaryKey) == 1 {
				if ident, ok := n.PrimaryKey[0].(*ast.Identifier); ok {
					fmt.Fprintf(sb, "%s  Identifier %s\n", indent, ident.Name())
				} else if lit, ok := n.PrimaryKey[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
					// Handle tuple literal (including empty tuple from PRIMARY KEY ())
					exprs, _ := lit.Value.([]ast.Expression)
					fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
					if len(exprs) > 0 {
						fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(exprs))
						for _, e := range exprs {
							Node(sb, e, depth+4)
						}
					} else {
						fmt.Fprintf(sb, "%s   ExpressionList\n", indent)
					}
				} else {
					Node(sb, n.PrimaryKey[0], depth+2)
				}
			} else {
				fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
				fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.PrimaryKey))
				for _, p := range n.PrimaryKey {
					Node(sb, p, depth+4)
				}
			}
		}
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s  Set\n", indent)
		}
	}
	if n.AsSelect != nil {
		// AS SELECT is output directly without Subquery wrapper
		Node(sb, n.AsSelect, depth+1)
	}
}

func explainDropQuery(sb *strings.Builder, n *ast.DropQuery, indent string, depth int) {
	// DROP USER has a special output format
	if n.User != "" {
		fmt.Fprintf(sb, "%sDROP USER query\n", indent)
		return
	}

	// Handle multiple tables: DROP TABLE t1, t2, t3
	if len(n.Tables) > 1 {
		fmt.Fprintf(sb, "%sDropQuery   (children %d)\n", indent, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Tables))
		for _, t := range n.Tables {
			Node(sb, t, depth+2)
		}
		return
	}

	name := n.Table
	if n.View != "" {
		name = n.View
	}
	if n.DropDatabase {
		name = n.Database
	}
	// DROP DATABASE uses different spacing than DROP TABLE
	if n.DropDatabase {
		fmt.Fprintf(sb, "%sDropQuery %s  (children %d)\n", indent, name, 1)
	} else {
		fmt.Fprintf(sb, "%sDropQuery  %s (children %d)\n", indent, name, 1)
	}
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
	if n.TableFunction != nil {
		// DESCRIBE on a table function
		fmt.Fprintf(sb, "%sDescribeQuery (children 1)\n", indent)
		explainFunctionCall(sb, n.TableFunction, indent+" ", 1)
	} else {
		name := n.Table
		if n.Database != "" {
			name = n.Database + "." + n.Table
		}
		fmt.Fprintf(sb, "%sDescribe %s\n", indent, name)
	}
}

func explainDataType(sb *strings.Builder, n *ast.DataType, indent string, depth int) {
	// If type has parameters, expand them as children
	if len(n.Parameters) > 0 {
		fmt.Fprintf(sb, "%sDataType %s (children %d)\n", indent, n.Name, 1)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Parameters))
		for _, p := range n.Parameters {
			Node(sb, p, depth+2)
		}
	} else if n.HasParentheses {
		// Empty parentheses, e.g., Tuple()
		fmt.Fprintf(sb, "%sDataType %s (children %d)\n", indent, n.Name, 1)
		fmt.Fprintf(sb, "%s ExpressionList\n", indent)
	} else {
		fmt.Fprintf(sb, "%sDataType %s\n", indent, n.Name)
	}
}

func explainNameTypePair(sb *strings.Builder, n *ast.NameTypePair, indent string, depth int) {
	fmt.Fprintf(sb, "%sNameTypePair %s (children %d)\n", indent, n.Name, 1)
	Node(sb, n.Type, depth+1)
}

func explainParameter(sb *strings.Builder, n *ast.Parameter, indent string) {
	if n.Name != "" {
		fmt.Fprintf(sb, "%sQueryParameter %s\n", indent, n.Name)
	} else {
		fmt.Fprintf(sb, "%sQueryParameter\n", indent)
	}
}
