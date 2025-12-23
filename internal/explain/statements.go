package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
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
	// Handle special CREATE types
	if n.CreateFunction {
		children := 2 // identifier + lambda
		fmt.Fprintf(sb, "%sCreateFunctionQuery %s (children %d)\n", indent, n.FunctionName, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.FunctionName)
		if n.FunctionBody != nil {
			Node(sb, n.FunctionBody, depth+1)
		}
		return
	}
	if n.CreateUser {
		fmt.Fprintf(sb, "%sCreateUserQuery %s\n", indent, n.UserName)
		return
	}
	if n.CreateDictionary {
		fmt.Fprintf(sb, "%sCreateDictionaryQuery %s (children 1)\n", indent, n.Table)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		return
	}

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
	if len(n.Columns) > 0 || len(n.Indexes) > 0 {
		childrenCount := 0
		if len(n.Columns) > 0 {
			childrenCount++
		}
		if len(n.Indexes) > 0 {
			childrenCount++
		}
		fmt.Fprintf(sb, "%s Columns definition (children %d)\n", indent, childrenCount)
		if len(n.Columns) > 0 {
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Columns))
			for _, col := range n.Columns {
				Column(sb, col, depth+3)
			}
		}
		if len(n.Indexes) > 0 {
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Indexes))
			for _, idx := range n.Indexes {
				Index(sb, idx, depth+3)
			}
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

	// DROP FUNCTION has a special output format
	if n.Function != "" {
		fmt.Fprintf(sb, "%sDropFunctionQuery\n", indent)
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
	// Check if we have a database-qualified name (for DROP TABLE db.table)
	hasDatabase := n.Database != "" && !n.DropDatabase
	if hasDatabase {
		// Database-qualified: DropQuery db table (children 2)
		fmt.Fprintf(sb, "%sDropQuery %s %s (children %d)\n", indent, n.Database, name, 2)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	} else if n.DropDatabase {
		// DROP DATABASE uses different spacing
		fmt.Fprintf(sb, "%sDropQuery %s  (children %d)\n", indent, name, 1)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	} else {
		fmt.Fprintf(sb, "%sDropQuery  %s (children %d)\n", indent, name, 1)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	}
}

func explainRenameQuery(sb *strings.Builder, n *ast.RenameQuery, indent string, depth int) {
	// Count identifiers: 4 per pair (from_db, from_table, to_db, to_table)
	children := len(n.Pairs) * 4
	fmt.Fprintf(sb, "%sRename (children %d)\n", indent, children)
	for _, pair := range n.Pairs {
		// From database
		fromDB := pair.FromDatabase
		if fromDB == "" {
			fromDB = "default"
		}
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, fromDB)
		// From table
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, pair.FromTable)
		// To database
		toDB := pair.ToDatabase
		if toDB == "" {
			toDB = "default"
		}
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, toDB)
		// To table
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, pair.ToTable)
	}
}

func explainSetQuery(sb *strings.Builder, indent string) {
	fmt.Fprintf(sb, "%sSet\n", indent)
}

func explainSystemQuery(sb *strings.Builder, indent string) {
	fmt.Fprintf(sb, "%sSYSTEM query\n", indent)
}

func explainExplainQuery(sb *strings.Builder, n *ast.ExplainQuery, indent string, depth int) {
	// EXPLAIN CURRENT TRANSACTION has no children
	if n.ExplainType == ast.ExplainCurrentTransaction {
		// At top level (depth 0), ClickHouse outputs "Explain EXPLAIN <TYPE>"
		if depth == 0 {
			fmt.Fprintf(sb, "%sExplain EXPLAIN %s\n", indent, n.ExplainType)
		} else {
			fmt.Fprintf(sb, "%sExplain %s\n", indent, n.ExplainType)
		}
		return
	}
	// At top level (depth 0), ClickHouse outputs "Explain EXPLAIN <TYPE>"
	// Nested in subqueries, it outputs "Explain <TYPE>"
	if depth == 0 {
		fmt.Fprintf(sb, "%sExplain EXPLAIN %s (children %d)\n", indent, n.ExplainType, 1)
	} else {
		fmt.Fprintf(sb, "%sExplain %s (children %d)\n", indent, n.ExplainType, 1)
	}
	Node(sb, n.Statement, depth+1)
}

func explainShowQuery(sb *strings.Builder, n *ast.ShowQuery, indent string) {
	// ClickHouse maps certain SHOW types to ShowTables in EXPLAIN AST
	showType := strings.Title(strings.ToLower(string(n.ShowType)))
	// SHOW SETTINGS and SHOW DATABASES are displayed as ShowTables in ClickHouse
	if showType == "Settings" || showType == "Databases" {
		showType = "Tables"
	}
	fmt.Fprintf(sb, "%sShow%s\n", indent, showType)
}

func explainUseQuery(sb *strings.Builder, n *ast.UseQuery, indent string) {
	fmt.Fprintf(sb, "%sUse %s\n", indent, n.Database)
}

func explainDescribeQuery(sb *strings.Builder, n *ast.DescribeQuery, indent string) {
	if n.TableFunction != nil {
		// DESCRIBE on a table function - wrap in TableExpression
		children := 1
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sDescribeQuery (children %d)\n", indent, children)
		fmt.Fprintf(sb, "%s TableExpression (children 1)\n", indent)
		explainFunctionCall(sb, n.TableFunction, indent+"  ", 2)
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
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
