package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

func explainInsertQuery(sb *strings.Builder, n *ast.InsertQuery, indent string, depth int) {
	// Count children
	children := 0
	if n.Infile != "" {
		children++
	}
	if n.Compression != "" {
		children++
	}
	if n.Function != nil {
		children++
	} else if n.Table != "" {
		children++ // Table identifier
	}
	if len(n.Columns) > 0 {
		children++ // Column list
	}
	if n.Select != nil {
		children++
	}
	if n.HasSettings {
		children++
	}
	// Note: InsertQuery uses 3 spaces after name in ClickHouse explain
	fmt.Fprintf(sb, "%sInsertQuery   (children %d)\n", indent, children)

	// FROM INFILE path comes first
	if n.Infile != "" {
		fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, n.Infile)
	}
	// COMPRESSION value comes next
	if n.Compression != "" {
		fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, n.Compression)
	}

	if n.Function != nil {
		Node(sb, n.Function, depth+1)
	} else if n.Table != "" {
		name := n.Table
		if n.Database != "" {
			name = n.Database + "." + n.Table
		}
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	}

	// Column list
	if len(n.Columns) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Columns))
		for _, col := range n.Columns {
			fmt.Fprintf(sb, "%s  Identifier %s\n", indent, col.Parts[len(col.Parts)-1])
		}
	}

	if n.Select != nil {
		Node(sb, n.Select, depth+1)
	}

	if n.HasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainCreateQuery(sb *strings.Builder, n *ast.CreateQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.CreateQuery\n", indent)
		return
	}
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
		fmt.Fprintf(sb, "%sCreateUserQuery\n", indent)
		return
	}
	if n.CreateDictionary {
		// Dictionary: count children = identifier + attributes (if any) + definition (if any)
		children := 1 // identifier
		if len(n.DictionaryAttrs) > 0 {
			children++
		}
		if n.DictionaryDef != nil {
			children++
		}
		fmt.Fprintf(sb, "%sCreateQuery %s (children %d)\n", indent, n.Table, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		// Dictionary attributes
		if len(n.DictionaryAttrs) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.DictionaryAttrs))
			for _, attr := range n.DictionaryAttrs {
				explainDictionaryAttributeDeclaration(sb, attr, indent+"  ", depth+2)
			}
		}
		// Dictionary definition
		if n.DictionaryDef != nil {
			explainDictionaryDefinition(sb, n.DictionaryDef, indent+" ", depth+1)
		}
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
	if n.AsTableFunction != nil {
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
		// Check for PRIMARY KEY constraints in column declarations
		var primaryKeyColumns []string
		for _, col := range n.Columns {
			if col.PrimaryKey {
				primaryKeyColumns = append(primaryKeyColumns, col.Name)
			}
		}
		if len(primaryKeyColumns) > 0 {
			childrenCount++ // Add for Function tuple containing PRIMARY KEY columns
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
		// Output PRIMARY KEY columns as Function tuple
		if len(primaryKeyColumns) > 0 {
			fmt.Fprintf(sb, "%s  Function tuple (children %d)\n", indent, 1)
			fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(primaryKeyColumns))
			for _, colName := range primaryKeyColumns {
				fmt.Fprintf(sb, "%s    Identifier %s\n", indent, colName)
			}
		}
	}
	if n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 || n.PartitionBy != nil || n.SampleBy != nil || len(n.Settings) > 0 {
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
		// SAMPLE BY is only shown in EXPLAIN AST when it's a function (not a simple identifier)
		// and when it's different from ORDER BY
		if n.SampleBy != nil {
			if _, isIdent := n.SampleBy.(*ast.Identifier); !isIdent {
				// Check if SAMPLE BY equals ORDER BY - if so, don't show it
				showSampleBy := true
				if len(n.OrderBy) == 1 {
					var orderBySb, sampleBySb strings.Builder
					Node(&orderBySb, n.OrderBy[0], 0)
					Node(&sampleBySb, n.SampleBy, 0)
					if orderBySb.String() == sampleBySb.String() {
						showSampleBy = false
					}
				}
				if showSampleBy {
					storageChildren++
				}
			}
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
		// SAMPLE BY is only shown in EXPLAIN AST when it's a function (not a simple identifier)
		// and when it's different from ORDER BY
		if n.SampleBy != nil {
			if _, isIdent := n.SampleBy.(*ast.Identifier); !isIdent {
				// Check if SAMPLE BY equals ORDER BY - if so, don't show it
				showSampleBy := true
				if len(n.OrderBy) == 1 {
					var orderBySb, sampleBySb strings.Builder
					Node(&orderBySb, n.OrderBy[0], 0)
					Node(&sampleBySb, n.SampleBy, 0)
					if orderBySb.String() == sampleBySb.String() {
						showSampleBy = false
					}
				}
				if showSampleBy {
					Node(sb, n.SampleBy, depth+2)
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
	if n.AsTableFunction != nil {
		// AS table_function(...) is output directly
		Node(sb, n.AsTableFunction, depth+1)
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
	if n.Dictionary != "" {
		name = n.Dictionary
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
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.RenameQuery\n", indent)
		return
	}
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

func explainSystemQuery(sb *strings.Builder, n *ast.SystemQuery, indent string) {
	fmt.Fprintf(sb, "%sSYSTEM query\n", indent)
}

func explainExplainQuery(sb *strings.Builder, n *ast.ExplainQuery, indent string, depth int) {
	// Determine the type string - only show if explicitly specified
	typeStr := ""
	if n.ExplicitType {
		typeStr = " " + string(n.ExplainType)
	}

	// EXPLAIN CURRENT TRANSACTION has no children
	if n.ExplainType == ast.ExplainCurrentTransaction {
		// At top level (depth 0), ClickHouse outputs "Explain EXPLAIN <TYPE>"
		if depth == 0 {
			fmt.Fprintf(sb, "%sExplain EXPLAIN%s\n", indent, typeStr)
		} else {
			fmt.Fprintf(sb, "%sExplain%s\n", indent, typeStr)
		}
		return
	}
	// Count children: settings (if present) + statement
	children := 1
	if n.HasSettings {
		children++
	}
	// At top level (depth 0), ClickHouse outputs "Explain EXPLAIN <TYPE>"
	// Nested in subqueries, it outputs "Explain <TYPE>"
	if depth == 0 {
		fmt.Fprintf(sb, "%sExplain EXPLAIN%s (children %d)\n", indent, typeStr, children)
	} else {
		fmt.Fprintf(sb, "%sExplain%s (children %d)\n", indent, typeStr, children)
	}
	if n.HasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
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

	// SHOW CREATE DATABASE has special output format
	if n.ShowType == ast.ShowCreateDB && n.From != "" {
		fmt.Fprintf(sb, "%sShowCreateDatabaseQuery %s  (children 1)\n", indent, n.From)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
		return
	}

	// SHOW CREATE TABLE has special output format with database and table identifiers
	if n.ShowType == ast.ShowCreate && (n.Database != "" || n.From != "") {
		// Format: ShowCreateTableQuery database table (children 2)
		name := n.From
		if n.Database != "" && n.From != "" {
			fmt.Fprintf(sb, "%sShowCreateTableQuery %s %s (children 2)\n", indent, n.Database, n.From)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
		} else if n.From != "" {
			fmt.Fprintf(sb, "%sShowCreateTableQuery  %s (children 1)\n", indent, name)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
		} else if n.Database != "" {
			fmt.Fprintf(sb, "%sShowCreateTableQuery  %s (children 1)\n", indent, n.Database)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		} else {
			fmt.Fprintf(sb, "%sShow%s\n", indent, showType)
		}
		return
	}

	// SHOW TABLES FROM database - include database as child
	if n.ShowType == ast.ShowTables && n.From != "" {
		fmt.Fprintf(sb, "%sShowTables (children 1)\n", indent)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
		return
	}

	fmt.Fprintf(sb, "%sShow%s\n", indent, showType)
}

func explainUseQuery(sb *strings.Builder, n *ast.UseQuery, indent string) {
	fmt.Fprintf(sb, "%sUseQuery %s (children %d)\n", indent, n.Database, 1)
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
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
		// Regular table describe
		name := n.Table
		if n.Database != "" {
			name = n.Database + "." + n.Table
		}
		children := 1
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sDescribeQuery (children %d)\n", indent, children)
		fmt.Fprintf(sb, "%s TableExpression (children 1)\n", indent)
		fmt.Fprintf(sb, "%s  TableIdentifier %s\n", indent, name)
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
	}
}

func explainExistsTableQuery(sb *strings.Builder, n *ast.ExistsQuery, indent string) {
	// EXISTS TABLE/DATABASE/DICTIONARY query
	name := n.Table
	if n.Database != "" {
		name = n.Database + " " + n.Table
	}
	fmt.Fprintf(sb, "%sExistsTableQuery %s (children %d)\n", indent, name, 2)
	if n.Database != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
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

func explainDetachQuery(sb *strings.Builder, n *ast.DetachQuery, indent string) {
	name := n.Table
	if name == "" {
		name = n.Database
	}
	if name != "" {
		fmt.Fprintf(sb, "%sDetachQuery  %s (children 1)\n", indent, name)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	} else {
		fmt.Fprintf(sb, "%sDetachQuery\n", indent)
	}
}

func explainAttachQuery(sb *strings.Builder, n *ast.AttachQuery, indent string) {
	name := n.Table
	if name == "" {
		name = n.Database
	}
	if name != "" {
		fmt.Fprintf(sb, "%sAttachQuery %s (children 1)\n", indent, name)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
	} else {
		fmt.Fprintf(sb, "%sAttachQuery\n", indent)
	}
}

func explainAlterQuery(sb *strings.Builder, n *ast.AlterQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.AlterQuery\n", indent)
		return
	}

	name := n.Table
	if n.Database != "" {
		name = n.Database + "." + n.Table
	}

	children := 2
	fmt.Fprintf(sb, "%sAlterQuery  %s (children %d)\n", indent, name, children)
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Commands))
	for _, cmd := range n.Commands {
		explainAlterCommand(sb, cmd, indent+"  ", depth+2)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
}

func explainAlterCommand(sb *strings.Builder, cmd *ast.AlterCommand, indent string, depth int) {
	children := countAlterCommandChildren(cmd)
	fmt.Fprintf(sb, "%sAlterCommand %s (children %d)\n", indent, cmd.Type, children)

	switch cmd.Type {
	case ast.AlterAddColumn:
		if cmd.Column != nil {
			Column(sb, cmd.Column, depth+1)
		}
		if cmd.AfterColumn != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.AfterColumn)
		}
	case ast.AlterModifyColumn:
		if cmd.Column != nil {
			Column(sb, cmd.Column, depth+1)
		}
		if cmd.AfterColumn != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.AfterColumn)
		}
	case ast.AlterDropColumn:
		if cmd.ColumnName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ColumnName)
		}
	case ast.AlterRenameColumn:
		if cmd.ColumnName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ColumnName)
		}
		if cmd.NewName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.NewName)
		}
	case ast.AlterClearColumn:
		if cmd.ColumnName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ColumnName)
		}
		if cmd.Partition != nil {
			Node(sb, cmd.Partition, depth+1)
		}
	case ast.AlterCommentColumn:
		if cmd.ColumnName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ColumnName)
		}
	case ast.AlterAddIndex, ast.AlterDropIndex, ast.AlterClearIndex, ast.AlterMaterializeIndex:
		if cmd.Index != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.Index)
		}
	case ast.AlterAddConstraint:
		if cmd.Constraint != nil {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.Constraint.Name)
		}
	case ast.AlterDropConstraint:
		if cmd.ConstraintName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ConstraintName)
		}
	case ast.AlterModifyTTL:
		if cmd.TTL != nil && cmd.TTL.Expression != nil {
			Node(sb, cmd.TTL.Expression, depth+1)
		}
	case ast.AlterModifySetting:
		fmt.Fprintf(sb, "%s Set\n", indent)
	case ast.AlterDropPartition, ast.AlterDetachPartition, ast.AlterAttachPartition,
		ast.AlterReplacePartition, ast.AlterFreezePartition:
		if cmd.Partition != nil {
			Node(sb, cmd.Partition, depth+1)
		}
	case ast.AlterFreeze:
		// No children
	case ast.AlterDeleteWhere:
		if cmd.Where != nil {
			Node(sb, cmd.Where, depth+1)
		}
	case ast.AlterUpdate:
		if len(cmd.Assignments) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(cmd.Assignments))
			for _, assign := range cmd.Assignments {
				fmt.Fprintf(sb, "%s  Function equals (children 1)\n", indent)
				fmt.Fprintf(sb, "%s   ExpressionList (children 2)\n", indent)
				fmt.Fprintf(sb, "%s    Identifier %s\n", indent, assign.Column)
				Node(sb, assign.Value, depth+4)
			}
		}
		if cmd.Where != nil {
			Node(sb, cmd.Where, depth+1)
		}
	case ast.AlterAddProjection:
		if cmd.Projection != nil {
			explainProjection(sb, cmd.Projection, indent, depth+1)
		}
	case ast.AlterDropProjection, ast.AlterMaterializeProjection, ast.AlterClearProjection:
		if cmd.ProjectionName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ProjectionName)
		}
	default:
		if cmd.Partition != nil {
			Node(sb, cmd.Partition, depth+1)
		}
	}
}

func explainProjection(sb *strings.Builder, p *ast.Projection, indent string, depth int) {
	children := 0
	if p.Select != nil {
		children++
	}
	fmt.Fprintf(sb, "%s Projection (children %d)\n", indent, children)
	if p.Select != nil {
		explainProjectionSelectQuery(sb, p.Select, indent+"  ", depth+1)
	}
}

func explainProjectionSelectQuery(sb *strings.Builder, q *ast.ProjectionSelectQuery, indent string, depth int) {
	children := 0
	if len(q.Columns) > 0 {
		children++
	}
	if q.OrderBy != nil {
		children++
	}
	if len(q.GroupBy) > 0 {
		children++
	}
	fmt.Fprintf(sb, "%sProjectionSelectQuery (children %d)\n", indent, children)
	if len(q.Columns) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(q.Columns))
		for _, col := range q.Columns {
			Node(sb, col, depth+2)
		}
	}
	if q.OrderBy != nil {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, q.OrderBy.Parts[len(q.OrderBy.Parts)-1])
	}
	if len(q.GroupBy) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(q.GroupBy))
		for _, expr := range q.GroupBy {
			Node(sb, expr, depth+2)
		}
	}
}

func countAlterCommandChildren(cmd *ast.AlterCommand) int {
	children := 0
	switch cmd.Type {
	case ast.AlterAddColumn, ast.AlterModifyColumn:
		if cmd.Column != nil {
			children++
		}
		if cmd.AfterColumn != "" {
			children++
		}
	case ast.AlterDropColumn, ast.AlterCommentColumn:
		if cmd.ColumnName != "" {
			children++
		}
	case ast.AlterRenameColumn:
		if cmd.ColumnName != "" {
			children++
		}
		if cmd.NewName != "" {
			children++
		}
	case ast.AlterClearColumn:
		if cmd.ColumnName != "" {
			children++
		}
		if cmd.Partition != nil {
			children++
		}
	case ast.AlterAddIndex, ast.AlterDropIndex, ast.AlterClearIndex, ast.AlterMaterializeIndex:
		if cmd.Index != "" {
			children++
		}
	case ast.AlterAddConstraint:
		if cmd.Constraint != nil {
			children++
		}
	case ast.AlterDropConstraint:
		if cmd.ConstraintName != "" {
			children++
		}
	case ast.AlterModifyTTL:
		if cmd.TTL != nil && cmd.TTL.Expression != nil {
			children++
		}
	case ast.AlterModifySetting:
		children = 1
	case ast.AlterDropPartition, ast.AlterDetachPartition, ast.AlterAttachPartition,
		ast.AlterReplacePartition, ast.AlterFreezePartition:
		if cmd.Partition != nil {
			children++
		}
	case ast.AlterFreeze:
		// No children
	case ast.AlterDeleteWhere:
		if cmd.Where != nil {
			children++
		}
	case ast.AlterUpdate:
		if len(cmd.Assignments) > 0 {
			children++
		}
		if cmd.Where != nil {
			children++
		}
	case ast.AlterAddProjection:
		if cmd.Projection != nil {
			children++
		}
	case ast.AlterDropProjection, ast.AlterMaterializeProjection, ast.AlterClearProjection:
		if cmd.ProjectionName != "" {
			children++
		}
	default:
		if cmd.Partition != nil {
			children++
		}
	}
	return children
}

func explainOptimizeQuery(sb *strings.Builder, n *ast.OptimizeQuery, indent string) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.OptimizeQuery\n", indent)
		return
	}

	name := n.Table
	if n.Final {
		name += "_final"
	}

	fmt.Fprintf(sb, "%sOptimizeQuery  %s (children %d)\n", indent, name, 1)
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
}

func explainTruncateQuery(sb *strings.Builder, n *ast.TruncateQuery, indent string) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.TruncateQuery\n", indent)
		return
	}

	name := n.Table
	if n.Database != "" {
		name = n.Database + "." + n.Table
	}

	fmt.Fprintf(sb, "%sTruncateQuery  %s (children %d)\n", indent, name, 1)
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
}
