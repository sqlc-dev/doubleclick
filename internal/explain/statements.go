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
		if n.Database != "" {
			children++ // Database identifier (separate from table)
		}
	}
	if len(n.ColumnExpressions) > 0 || len(n.Columns) > 0 || n.AllColumns {
		children++ // Column list
	}
	if n.Select != nil {
		children++
	}
	if n.HasSettings {
		children++
	}
	if n.PartitionBy != nil {
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
		if n.Database != "" {
			// Database-qualified: output separate identifiers
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		} else {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		}
	}

	// PARTITION BY clause (output after Function/Table)
	if n.PartitionBy != nil {
		if ident, ok := n.PartitionBy.(*ast.Identifier); ok {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, ident.Name())
		} else {
			Node(sb, n.PartitionBy, depth+1)
		}
	}

	// Column list
	if len(n.ColumnExpressions) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.ColumnExpressions))
		for _, expr := range n.ColumnExpressions {
			Node(sb, expr, depth+2)
		}
	} else if n.AllColumns {
		fmt.Fprintf(sb, "%s ExpressionList (children 1)\n", indent)
		fmt.Fprintf(sb, "%s  Asterisk\n", indent)
	} else if len(n.Columns) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Columns))
		for _, col := range n.Columns {
			fmt.Fprintf(sb, "%s  Identifier %s\n", indent, col.Parts[len(col.Parts)-1])
		}
	}

	if n.Select != nil {
		// For INSERT with SELECT, temporarily clear Format from the SELECT
		// (FORMAT in INSERT belongs to INSERT, not SELECT, and shouldn't be output in EXPLAIN)
		if swu, ok := n.Select.(*ast.SelectWithUnionQuery); ok {
			for _, sel := range swu.Selects {
				if sq, ok := sel.(*ast.SelectQuery); ok && sq.Format != nil {
					savedFormat := sq.Format
					sq.Format = nil
					defer func() { sq.Format = savedFormat }()
				}
			}
		}
		// If this INSERT has an inherited WITH clause (from WITH ... INSERT syntax),
		// use the special explain function that outputs WITH at the end of each SelectQuery
		if len(n.With) > 0 {
			ExplainSelectWithInheritedWith(sb, n.Select, n.With, depth+1)
		} else {
			Node(sb, n.Select, depth+1)
		}
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
	if n.CreateUser || n.AlterUser {
		if n.HasAuthenticationData {
			// Each authentication value is a separate AuthenticationData child
			if len(n.AuthenticationValues) > 0 {
				fmt.Fprintf(sb, "%sCreateUserQuery (children %d)\n", indent, len(n.AuthenticationValues))
				for _, val := range n.AuthenticationValues {
					// Each AuthenticationData has 1 child (the Literal value)
					fmt.Fprintf(sb, "%s AuthenticationData (children 1)\n", indent)
					// Escape the value - strings need \' escaping
					escaped := escapeStringLiteral(val)
					fmt.Fprintf(sb, "%s  Literal \\'%s\\'\n", indent, escaped)
				}
			} else if n.SSHKeyCount > 0 {
				// SSH key authentication - each key is a PublicSSHKey child
				fmt.Fprintf(sb, "%sCreateUserQuery (children 1)\n", indent)
				fmt.Fprintf(sb, "%s AuthenticationData (children %d)\n", indent, n.SSHKeyCount)
				for i := 0; i < n.SSHKeyCount; i++ {
					fmt.Fprintf(sb, "%s  PublicSSHKey\n", indent)
				}
			} else {
				// No values - just output CreateUserQuery with 1 child
				fmt.Fprintf(sb, "%sCreateUserQuery (children 1)\n", indent)
				fmt.Fprintf(sb, "%s AuthenticationData\n", indent)
			}
		} else {
			fmt.Fprintf(sb, "%sCreateUserQuery\n", indent)
		}
		return
	}
	if n.CreateDictionary {
		// Dictionary: count children = database identifier (if any) + table identifier + attributes (if any) + definition (if any)
		children := 1 // table identifier
		hasDatabase := n.Database != ""
		if hasDatabase {
			children++ // database identifier
		}
		if len(n.DictionaryAttrs) > 0 {
			children++
		}
		if n.DictionaryDef != nil {
			children++
		}
		// Format: "CreateQuery [database] [table] (children N)"
		if hasDatabase {
			fmt.Fprintf(sb, "%sCreateQuery %s %s (children %d)\n", indent, n.Database, n.Table, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		} else {
			fmt.Fprintf(sb, "%sCreateQuery %s (children %d)\n", indent, n.Table, children)
		}
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
	// Check for database-qualified table/view name
	hasDatabase := n.Database != "" && !n.CreateDatabase && (n.Table != "" || n.View != "")
	// Check for column-level PRIMARY KEY modifiers (e.g., "a String PRIMARY KEY")
	hasColumnPrimaryKey := false
	for _, col := range n.Columns {
		if col.PrimaryKey {
			hasColumnPrimaryKey = true
			break
		}
	}
	// Count children: name + columns + engine/storage
	children := 1 // name identifier
	if hasDatabase {
		children++ // additional identifier for database
	}
	if len(n.Columns) > 0 || len(n.Indexes) > 0 || len(n.Projections) > 0 || len(n.Constraints) > 0 {
		children++
	}
	hasStorageChild := n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 || n.PartitionBy != nil || n.SampleBy != nil || n.TTL != nil || len(n.Settings) > 0 || len(n.ColumnsPrimaryKey) > 0 || hasColumnPrimaryKey
	if hasStorageChild {
		children++
	}
	// For materialized views with TO clause but no storage, count ViewTargets as a child
	if n.Materialized && n.To != "" && !hasStorageChild {
		children++ // ViewTargets
	}
	if n.AsSelect != nil {
		children++
	}
	if n.AsTableFunction != nil {
		children++
	}
	// Count Format as a child if present
	hasFormat := n.Format != ""
	if hasFormat {
		children++
	}
	// Count Comment as a child if present
	if n.Comment != "" {
		children++
	}
	// ClickHouse adds an extra space before (children N) for CREATE DATABASE
	if n.CreateDatabase {
		fmt.Fprintf(sb, "%sCreateQuery %s  (children %d)\n", indent, EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
	} else if hasDatabase {
		// Database-qualified: CreateQuery db table (children N)
		fmt.Fprintf(sb, "%sCreateQuery %s %s (children %d)\n", indent, EscapeIdentifier(n.Database), EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(n.Database))
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
	} else {
		fmt.Fprintf(sb, "%sCreateQuery %s (children %d)\n", indent, EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
	}
	if len(n.Columns) > 0 || len(n.Indexes) > 0 || len(n.Projections) > 0 || len(n.Constraints) > 0 {
		childrenCount := 0
		if len(n.Columns) > 0 {
			childrenCount++
		}
		if len(n.Indexes) > 0 {
			childrenCount++
		}
		if len(n.Projections) > 0 {
			childrenCount++
		}
		if len(n.Constraints) > 0 {
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
		// Check for inline PRIMARY KEY (from column list, e.g., "n int, primary key n")
		if len(n.ColumnsPrimaryKey) > 0 {
			childrenCount++ // Add for the primary key identifier(s)
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
		if len(n.Projections) > 0 {
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Projections))
			for _, proj := range n.Projections {
				explainProjection(sb, proj, indent+"   ", depth+3)
			}
		}
		// Output constraints wrapped in Constraint nodes
		if len(n.Constraints) > 0 {
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Constraints))
			for _, constraint := range n.Constraints {
				fmt.Fprintf(sb, "%s   Constraint (children 1)\n", indent)
				Node(sb, constraint.Expression, depth+4)
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
		// Output inline PRIMARY KEY (from column list)
		if len(n.ColumnsPrimaryKey) > 0 {
			if len(n.ColumnsPrimaryKey) > 1 {
				// Multiple columns: wrap in Function tuple
				fmt.Fprintf(sb, "%s  Function tuple (children 1)\n", indent)
				fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.ColumnsPrimaryKey))
				for _, pk := range n.ColumnsPrimaryKey {
					Node(sb, pk, depth+4)
				}
			} else {
				// Single column: output directly
				for _, pk := range n.ColumnsPrimaryKey {
					Node(sb, pk, depth+2)
				}
			}
		}
	}
	// For materialized views, output AsSelect before storage definition
	if n.Materialized && n.AsSelect != nil {
		// Set context flag to prevent Format from being output at SelectWithUnionQuery level
		// (it will be output at CreateQuery level instead)
		if hasFormat {
			inCreateQueryContext = true
		}
		Node(sb, n.AsSelect, depth+1)
		if hasFormat {
			inCreateQueryContext = false
		}
	}
	hasStorage := n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 || n.PartitionBy != nil || n.SampleBy != nil || n.TTL != nil || len(n.Settings) > 0 || len(n.ColumnsPrimaryKey) > 0 || hasColumnPrimaryKey
	if hasStorage {
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
		// SAMPLE BY is always shown in EXPLAIN AST when present
		if n.SampleBy != nil {
			storageChildren++
		}
		if n.TTL != nil {
			storageChildren++
		}
		if len(n.Settings) > 0 {
			storageChildren++
		}
		// For materialized views, wrap storage definition in ViewTargets
		// and use extra indentation for storage children
		storageIndent := indent + " " // 1 space for regular storage (format strings add 1 more)
		storageChildDepth := depth + 2
		if n.Materialized {
			fmt.Fprintf(sb, "%s ViewTargets (children %d)\n", indent, 1)
			if storageChildren > 0 {
				fmt.Fprintf(sb, "%s  Storage definition (children %d)\n", indent, storageChildren)
			} else {
				fmt.Fprintf(sb, "%s  Storage definition\n", indent)
			}
			storageIndent = indent + "  " // 2 spaces for materialized (format strings add 1 more = 3 total)
			storageChildDepth = depth + 3
		} else {
			if storageChildren > 0 {
				fmt.Fprintf(sb, "%s Storage definition (children %d)\n", indent, storageChildren)
			} else {
				fmt.Fprintf(sb, "%s Storage definition\n", indent)
			}
		}
		if n.Engine != nil {
			if n.Engine.HasParentheses {
				fmt.Fprintf(sb, "%s Function %s (children %d)\n", storageIndent, n.Engine.Name, 1)
				if len(n.Engine.Parameters) > 0 {
					fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", storageIndent, len(n.Engine.Parameters))
					for _, param := range n.Engine.Parameters {
						Node(sb, param, storageChildDepth+2)
					}
				} else {
					fmt.Fprintf(sb, "%s  ExpressionList\n", storageIndent)
				}
			} else {
				fmt.Fprintf(sb, "%s Function %s\n", storageIndent, n.Engine.Name)
			}
		}
		if n.PartitionBy != nil {
			if ident, ok := n.PartitionBy.(*ast.Identifier); ok {
				fmt.Fprintf(sb, "%s Identifier %s\n", storageIndent, ident.Name())
			} else {
				Node(sb, n.PartitionBy, storageChildDepth)
			}
		}
		// PRIMARY KEY comes before ORDER BY in EXPLAIN output
		if len(n.PrimaryKey) > 0 {
			if len(n.PrimaryKey) == 1 {
				if ident, ok := n.PrimaryKey[0].(*ast.Identifier); ok {
					fmt.Fprintf(sb, "%s Identifier %s\n", storageIndent, ident.Name())
				} else if lit, ok := n.PrimaryKey[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
					// Handle tuple literal (including empty tuple from PRIMARY KEY ())
					exprs, _ := lit.Value.([]ast.Expression)
					fmt.Fprintf(sb, "%s Function tuple (children %d)\n", storageIndent, 1)
					if len(exprs) > 0 {
						fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", storageIndent, len(exprs))
						for _, e := range exprs {
							Node(sb, e, storageChildDepth+2)
						}
					} else {
						fmt.Fprintf(sb, "%s  ExpressionList\n", storageIndent)
					}
				} else {
					Node(sb, n.PrimaryKey[0], storageChildDepth)
				}
			} else {
				fmt.Fprintf(sb, "%s Function tuple (children %d)\n", storageIndent, 1)
				fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", storageIndent, len(n.PrimaryKey))
				for _, p := range n.PrimaryKey {
					Node(sb, p, storageChildDepth+2)
				}
			}
		}
		// ORDER BY comes after PRIMARY KEY in EXPLAIN output
		if len(n.OrderBy) > 0 {
			if len(n.OrderBy) == 1 {
				if ident, ok := n.OrderBy[0].(*ast.Identifier); ok {
					fmt.Fprintf(sb, "%s Identifier %s\n", storageIndent, ident.Name())
				} else if lit, ok := n.OrderBy[0].(*ast.Literal); ok && lit.Type == ast.LiteralTuple {
					// Handle tuple literal - for ORDER BY with modifiers (DESC/ASC),
					// ClickHouse outputs just "Function tuple" without children
					// For empty tuples or regular tuples without modifiers, output children
					if n.OrderByHasModifiers {
						fmt.Fprintf(sb, "%s Function tuple\n", storageIndent)
					} else {
						exprs, _ := lit.Value.([]ast.Expression)
						fmt.Fprintf(sb, "%s Function tuple (children %d)\n", storageIndent, 1)
						if len(exprs) > 0 {
							fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", storageIndent, len(exprs))
							for _, e := range exprs {
								Node(sb, e, storageChildDepth+2)
							}
						} else {
							fmt.Fprintf(sb, "%s  ExpressionList\n", storageIndent)
						}
					}
				} else {
					Node(sb, n.OrderBy[0], storageChildDepth)
				}
			} else {
				// Multiple ORDER BY expressions without modifiers
				fmt.Fprintf(sb, "%s Function tuple (children %d)\n", storageIndent, 1)
				fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", storageIndent, len(n.OrderBy))
				for _, o := range n.OrderBy {
					Node(sb, o, storageChildDepth+2)
				}
			}
		}
		// SAMPLE BY is always shown in EXPLAIN AST when present
		if n.SampleBy != nil {
			Node(sb, n.SampleBy, storageChildDepth)
		}
		if n.TTL != nil {
			// Count total TTL elements (1 for Expression + len(Expressions))
			ttlCount := 1 + len(n.TTL.Expressions)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", storageIndent, ttlCount)
			fmt.Fprintf(sb, "%s  TTLElement (children 1)\n", storageIndent)
			Node(sb, n.TTL.Expression, storageChildDepth+2)
			for _, expr := range n.TTL.Expressions {
				fmt.Fprintf(sb, "%s  TTLElement (children 1)\n", storageIndent)
				Node(sb, expr, storageChildDepth+2)
			}
		}
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", storageIndent)
		}
	} else if n.Materialized && n.To != "" {
		// For materialized views with TO clause but no storage definition,
		// output just ViewTargets without children
		fmt.Fprintf(sb, "%s ViewTargets\n", indent)
	}
	// For non-materialized views, output AsSelect after storage
	if n.AsSelect != nil && !n.Materialized {
		// Set context flag to prevent Format from being output at SelectWithUnionQuery level
		// (it will be output at CreateQuery level instead)
		if hasFormat {
			inCreateQueryContext = true
		}
		// AS SELECT is output directly without Subquery wrapper
		Node(sb, n.AsSelect, depth+1)
		if hasFormat {
			inCreateQueryContext = false
		}
	}
	if n.AsTableFunction != nil {
		// AS table_function(...) is output directly
		Node(sb, n.AsTableFunction, depth+1)
	}
	// Output FORMAT clause if present
	if hasFormat {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
	}
	// Output COMMENT clause if present
	if n.Comment != "" {
		fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, escapeStringLiteral(n.Comment))
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

	// DROP ROLE
	if n.Role != "" {
		fmt.Fprintf(sb, "%sDROP ROLE query\n", indent)
		return
	}

	// DROP QUOTA
	if n.Quota != "" {
		fmt.Fprintf(sb, "%sDROP QUOTA query\n", indent)
		return
	}

	// DROP POLICY
	if n.Policy != "" {
		fmt.Fprintf(sb, "%sDROP POLICY query\n", indent)
		return
	}

	// DROP ROW POLICY
	if n.RowPolicy != "" {
		fmt.Fprintf(sb, "%sDROP ROW POLICY query\n", indent)
		return
	}

	// DROP SETTINGS PROFILE
	if n.SettingsProfile != "" {
		fmt.Fprintf(sb, "%sDROP SETTINGS PROFILE query\n", indent)
		return
	}

	// DROP INDEX - outputs as DropIndexQuery with two spaces before table name
	if n.Index != "" {
		fmt.Fprintf(sb, "%sDropIndexQuery  %s (children %d)\n", indent, n.Table, 2)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Index)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
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
	hasFormat := n.Format != ""

	if hasDatabase {
		// Database-qualified: DropQuery db table (children 2 or 3)
		children := 2
		if hasFormat {
			children = 3
		}
		fmt.Fprintf(sb, "%sDropQuery %s %s (children %d)\n", indent, EscapeIdentifier(n.Database), EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(n.Database))
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
		if hasFormat {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
	} else if n.DropDatabase {
		// DROP DATABASE uses different spacing
		children := 1
		if hasFormat {
			children = 2
		}
		fmt.Fprintf(sb, "%sDropQuery %s  (children %d)\n", indent, EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
		if hasFormat {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
	} else {
		children := 1
		if hasFormat {
			children++
		}
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sDropQuery  %s (children %d)\n", indent, EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
		if hasFormat {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
	}
}

func explainUndropQuery(sb *strings.Builder, n *ast.UndropQuery, indent string, depth int) {
	name := n.Table
	// Check if we have a database-qualified name (for UNDROP TABLE db.table)
	hasDatabase := n.Database != ""
	hasFormat := n.Format != ""
	if hasDatabase {
		// Database-qualified: UndropQuery db table (children 2 or 3)
		children := 2
		if hasFormat {
			children = 3
		}
		fmt.Fprintf(sb, "%sUndropQuery %s %s (children %d)\n", indent, EscapeIdentifier(n.Database), EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(n.Database))
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
		if hasFormat {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
	} else {
		children := 1
		if hasFormat {
			children = 2
		}
		fmt.Fprintf(sb, "%sUndropQuery  %s (children %d)\n", indent, EscapeIdentifier(name), children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, EscapeIdentifier(name))
		if hasFormat {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
	}
}

func explainRenameQuery(sb *strings.Builder, n *ast.RenameQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.RenameQuery\n", indent)
		return
	}
	// Count identifiers: 2 per pair if no database, 4 per pair if databases specified
	hasSettings := len(n.Settings) > 0
	children := 0
	for _, pair := range n.Pairs {
		if pair.FromDatabase != "" {
			children++
		}
		children++ // from table
		if pair.ToDatabase != "" {
			children++
		}
		children++ // to table
	}
	if hasSettings {
		children++
	}
	fmt.Fprintf(sb, "%sRename (children %d)\n", indent, children)
	for _, pair := range n.Pairs {
		// From database (only if specified)
		if pair.FromDatabase != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, pair.FromDatabase)
		}
		// From table
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, pair.FromTable)
		// To database (only if specified)
		if pair.ToDatabase != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, pair.ToDatabase)
		}
		// To table
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, pair.ToTable)
	}
	if hasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainExchangeQuery(sb *strings.Builder, n *ast.ExchangeQuery, indent string) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.ExchangeQuery\n", indent)
		return
	}
	// Count identifiers: 2 per table (db + table if qualified, or just table)
	// EXCHANGE TABLES outputs as "Rename" in ClickHouse
	children := 0
	if n.Database1 != "" {
		children += 2 // db1 + table1
	} else {
		children += 1 // just table1
	}
	if n.Database2 != "" {
		children += 2 // db2 + table2
	} else {
		children += 1 // just table2
	}
	fmt.Fprintf(sb, "%sRename (children %d)\n", indent, children)
	// First table
	if n.Database1 != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database1)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table1)
	// Second table
	if n.Database2 != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database2)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table2)
}

func explainSetQuery(sb *strings.Builder, indent string) {
	fmt.Fprintf(sb, "%sSet\n", indent)
}

func explainSystemQuery(sb *strings.Builder, n *ast.SystemQuery, indent string) {
	// Some commands like FLUSH LOGS don't show the log name as a child
	// For other commands, table/database names are shown as children
	isFlushLogs := strings.HasPrefix(strings.ToUpper(n.Command), "FLUSH LOGS")

	// Count children - database and table are children if present and not FLUSH LOGS
	children := 0
	if !isFlushLogs {
		if n.Database != "" {
			children++
		}
		if n.Table != "" {
			children++
		}
		// For commands that need duplicate output, double the count
		if n.DuplicateTableOutput && children > 0 {
			children *= 2
		}
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sSYSTEM query (children %d)\n", indent, children)
		if n.Database != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		}
		if n.Table != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		}
		// Output again for duplicate commands
		if n.DuplicateTableOutput {
			if n.Database != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			}
			if n.Table != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
			}
		}
	} else {
		fmt.Fprintf(sb, "%sSYSTEM query\n", indent)
	}
}

func explainExplainQuery(sb *strings.Builder, n *ast.ExplainQuery, indent string, depth int) {
	// Determine the type string - only show if explicitly specified AND not PLAN (default)
	typeStr := ""
	if n.ExplicitType && n.ExplainType != ast.ExplainPlan {
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

	// Check if inner statement has FORMAT clause - this should be output as child of Explain
	// Also check for SETTINGS after FORMAT (these are at the EXPLAIN level, not part of the SELECT)
	var format *ast.Identifier
	var hasSettingsAfterFormat bool
	var savedSettings []*ast.SettingExpr
	if swu, ok := n.Statement.(*ast.SelectWithUnionQuery); ok {
		// Check for union-level settings after format
		if swu.SettingsAfterFormat && len(swu.Settings) > 0 {
			hasSettingsAfterFormat = true
			savedSettings = swu.Settings
			swu.Settings = nil
			defer func() { swu.Settings = savedSettings }()
		}
		for _, sel := range swu.Selects {
			if sq, ok := sel.(*ast.SelectQuery); ok {
				if sq.Format != nil {
					format = sq.Format
					// Temporarily nil out the format so it's not output by SelectWithUnionQuery
					sq.Format = nil
					defer func() { sq.Format = format }()
				}
				// Check for settings after format in the SelectQuery
				if sq.SettingsAfterFormat && len(sq.Settings) > 0 && !hasSettingsAfterFormat {
					hasSettingsAfterFormat = true
					savedSettings = sq.Settings
					sq.Settings = nil
					defer func() { sq.Settings = savedSettings }()
				}
				break
			}
		}
	}

	// Count children: statement + format (if present) + settings (if present)
	children := 1
	if format != nil {
		children++
	}
	if n.HasSettings || hasSettingsAfterFormat {
		children++
	}

	// At top level (depth 0), ClickHouse outputs "Explain EXPLAIN <TYPE>"
	// Nested in subqueries, it outputs "Explain <TYPE>"
	if depth == 0 {
		fmt.Fprintf(sb, "%sExplain EXPLAIN%s (children %d)\n", indent, typeStr, children)
	} else {
		fmt.Fprintf(sb, "%sExplain%s (children %d)\n", indent, typeStr, children)
	}
	// EXPLAIN-level settings (like header = 0) come BEFORE the statement
	if n.HasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
	// Output the statement
	Node(sb, n.Statement, depth+1)
	// Format comes after statement
	if format != nil {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, format.Parts[len(format.Parts)-1])
	}
	// Settings after format (at the query level, e.g., FORMAT Null SETTINGS ...) come last
	if hasSettingsAfterFormat {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
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

	// SHOW CREATE DICTIONARY has special output format
	if n.ShowType == ast.ShowCreateDictionary && (n.Database != "" || n.From != "") {
		if n.Database != "" && n.From != "" {
			children := 2
			if n.Format != "" {
				children++
			}
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateDictionaryQuery %s %s (children %d)\n", indent, n.Database, n.From, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
			if n.Format != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
			}
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else if n.From != "" {
			children := 1
			if n.Format != "" {
				children++
			}
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateDictionaryQuery  %s (children %d)\n", indent, n.From, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
			if n.Format != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
			}
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else if n.Database != "" {
			children := 1
			if n.Format != "" {
				children++
			}
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateDictionaryQuery  %s (children %d)\n", indent, n.Database, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			if n.Format != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
			}
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		}
		return
	}

	// SHOW CREATE VIEW has special output format
	if n.ShowType == ast.ShowCreateView && (n.Database != "" || n.From != "") {
		if n.Database != "" && n.From != "" {
			children := 2
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateViewQuery %s %s (children %d)\n", indent, n.Database, n.From, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else if n.From != "" {
			children := 1
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateViewQuery  %s (children %d)\n", indent, n.From, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else if n.Database != "" {
			children := 1
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateViewQuery  %s (children %d)\n", indent, n.Database, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		}
		return
	}

	// SHOW CREATE TABLE has special output format with database and table identifiers
	if n.ShowType == ast.ShowCreate && (n.Database != "" || n.From != "") {
		// Format: ShowCreateTableQuery database table (children 2) or with FORMAT
		name := n.From
		if n.Database != "" && n.From != "" {
			children := 2
			if n.Format != "" {
				children++
			}
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateTableQuery %s %s (children %d)\n", indent, n.Database, n.From, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
			if n.Format != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
			}
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else if n.From != "" {
			children := 1
			if n.Format != "" {
				children++
			}
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateTableQuery  %s (children %d)\n", indent, name, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, name)
			if n.Format != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
			}
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else if n.Database != "" {
			children := 1
			if n.Format != "" {
				children++
			}
			if n.HasSettings {
				children++
			}
			fmt.Fprintf(sb, "%sShowCreateTableQuery  %s (children %d)\n", indent, n.Database, children)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
			if n.Format != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
			}
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else {
			fmt.Fprintf(sb, "%sShow%s\n", indent, showType)
		}
		return
	}

	// SHOW CREATE USER has special output format
	if n.ShowType == ast.ShowCreateUser {
		userWord := "USER"
		if n.MultipleUsers {
			userWord = "USERS"
		}
		if n.Format != "" {
			fmt.Fprintf(sb, "%sSHOW CREATE %s query (children 1)\n", indent, userWord)
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		} else {
			fmt.Fprintf(sb, "%sSHOW CREATE %s query\n", indent, userWord)
		}
		return
	}

	// SHOW TABLES/DATABASES/DICTIONARIES - include FROM and FORMAT as children
	if n.ShowType == ast.ShowTables || n.ShowType == ast.ShowDatabases || n.ShowType == ast.ShowDictionaries {
		children := 0
		if n.From != "" {
			children++
		}
		if n.Format != "" {
			children++
		}
		if n.HasSettings {
			children++
		}
		if children > 0 {
			fmt.Fprintf(sb, "%sShowTables (children %d)\n", indent, children)
			if n.From != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.From)
			}
			if n.Format != "" {
				fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
			}
			if n.HasSettings {
				fmt.Fprintf(sb, "%s Set\n", indent)
			}
		} else {
			fmt.Fprintf(sb, "%sShowTables\n", indent)
		}
		return
	}

	fmt.Fprintf(sb, "%sShow%s\n", indent, showType)
}

func explainUseQuery(sb *strings.Builder, n *ast.UseQuery, indent string) {
	fmt.Fprintf(sb, "%sUseQuery %s (children %d)\n", indent, n.Database, 1)
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
}

func explainDescribeQuery(sb *strings.Builder, n *ast.DescribeQuery, indent string, depth int) {
	if n.TableExpr != nil {
		// DESCRIBE on a subquery - TableExpr contains a TableExpression with a Subquery
		children := 1
		if n.Format != "" {
			children++
		}
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sDescribeQuery (children %d)\n", indent, children)
		Node(sb, n.TableExpr, depth+1)
		if n.Format != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
	} else if n.TableFunction != nil {
		// DESCRIBE on a table function - wrap in TableExpression
		children := 1
		if n.Format != "" {
			children++
		}
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sDescribeQuery (children %d)\n", indent, children)
		fmt.Fprintf(sb, "%s TableExpression (children 1)\n", indent)
		explainFunctionCall(sb, n.TableFunction, indent+"  ", 2)
		if n.Format != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
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
		if n.Format != "" {
			children++
		}
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sDescribeQuery (children %d)\n", indent, children)
		fmt.Fprintf(sb, "%s TableExpression (children 1)\n", indent)
		fmt.Fprintf(sb, "%s  TableIdentifier %s\n", indent, name)
		if n.Format != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
	}
}

func explainExistsTableQuery(sb *strings.Builder, n *ast.ExistsQuery, indent string) {
	// Determine query type name based on ExistsType
	queryType := "ExistsTableQuery"
	switch n.ExistsType {
	case ast.ExistsDictionary:
		queryType = "ExistsDictionaryQuery"
	case ast.ExistsDatabase:
		queryType = "ExistsDatabaseQuery"
	case ast.ExistsView:
		queryType = "ExistsViewQuery"
	}

	hasSettings := len(n.Settings) > 0

	// EXISTS DATABASE has only one child (the database name stored in Table)
	if n.ExistsType == ast.ExistsDatabase {
		name := n.Table
		children := 1
		if hasSettings {
			children++
		}
		fmt.Fprintf(sb, "%s%s %s  (children %d)\n", indent, queryType, name, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		if hasSettings {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
		return
	}

	// For TABLE/DICTIONARY/VIEW, show database and object name
	name := " " + n.Table // Prefix with space for alignment (where database would be)
	children := 1
	if n.Database != "" {
		name = n.Database + " " + n.Table
		children = 2
	}
	if hasSettings {
		children++
	}
	fmt.Fprintf(sb, "%s%s %s (children %d)\n", indent, queryType, name, children)
	if n.Database != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	if hasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
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

func explainObjectTypeArgument(sb *strings.Builder, n *ast.ObjectTypeArgument, indent string, depth int) {
	fmt.Fprintf(sb, "%sASTObjectTypeArgument (children %d)\n", indent, 1)
	// SKIP function calls are unwrapped - only the path/pattern is shown
	if fn, ok := n.Expr.(*ast.FunctionCall); ok {
		if strings.ToUpper(fn.Name) == "SKIP" || strings.ToUpper(fn.Name) == "SKIP REGEXP" {
			if len(fn.Arguments) > 0 {
				Node(sb, fn.Arguments[0], depth+1)
				return
			}
		}
	}
	Node(sb, n.Expr, depth+1)
}

func explainNameTypePair(sb *strings.Builder, n *ast.NameTypePair, indent string, depth int) {
	fmt.Fprintf(sb, "%sNameTypePair %s (children %d)\n", indent, n.Name, 1)
	Node(sb, n.Type, depth+1)
}

func explainParameter(sb *strings.Builder, n *ast.Parameter, indent string) {
	if n.Name != "" {
		if n.Type != nil {
			fmt.Fprintf(sb, "%sQueryParameter %s:%s\n", indent, n.Name, FormatDataType(n.Type))
		} else {
			fmt.Fprintf(sb, "%sQueryParameter %s\n", indent, n.Name)
		}
	} else {
		fmt.Fprintf(sb, "%sQueryParameter\n", indent)
	}
}

func explainDetachQuery(sb *strings.Builder, n *ast.DetachQuery, indent string) {
	// Check for database-qualified table name
	if n.Database != "" && n.Table != "" {
		// Database-qualified: DetachQuery db table (children 2)
		fmt.Fprintf(sb, "%sDetachQuery %s %s (children 2)\n", indent, n.Database, n.Table)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		return
	}
	// DETACH DATABASE db: Database set, Table empty -> "DetachQuery db  (children 1)"
	if n.Database != "" && n.Table == "" {
		fmt.Fprintf(sb, "%sDetachQuery %s  (children 1)\n", indent, n.Database)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		return
	}
	// DETACH TABLE table: Database empty, Table set -> "DetachQuery  table (children 1)"
	if n.Table != "" {
		fmt.Fprintf(sb, "%sDetachQuery  %s (children 1)\n", indent, n.Table)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		return
	}
	// DETACH DICTIONARY dict: Dictionary set -> "DetachQuery  dict (children 1)"
	if n.Dictionary != "" {
		fmt.Fprintf(sb, "%sDetachQuery  %s (children 1)\n", indent, n.Dictionary)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Dictionary)
		return
	}
	// No name
	fmt.Fprintf(sb, "%sDetachQuery\n", indent)
}

func explainAttachQuery(sb *strings.Builder, n *ast.AttachQuery, indent string, depth int) {
	// Count children: identifier + columns definition (if any) + select query (if any) + storage/view targets (if any)
	children := 1 // table/database identifier
	if n.Database != "" && n.Table != "" {
		children++ // extra identifier for database
	}
	hasColumns := len(n.Columns) > 0 || len(n.ColumnsPrimaryKey) > 0 || len(n.Indexes) > 0
	if hasColumns {
		children++
	}
	hasSelectQuery := n.SelectQuery != nil
	if hasSelectQuery {
		children++
	}
	hasStorage := n.Engine != nil || len(n.OrderBy) > 0 || len(n.PrimaryKey) > 0 || n.PartitionBy != nil || len(n.Settings) > 0
	if hasStorage {
		children++ // ViewTargets or Storage definition
	}

	// Output header
	if n.Database != "" && n.Table != "" {
		fmt.Fprintf(sb, "%sAttachQuery %s %s (children %d)\n", indent, n.Database, n.Table, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	} else if n.Database != "" && n.Table == "" {
		fmt.Fprintf(sb, "%sAttachQuery %s  (children %d)\n", indent, n.Database, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
	} else if n.Table != "" {
		fmt.Fprintf(sb, "%sAttachQuery %s (children %d)\n", indent, n.Table, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	} else if n.Dictionary != "" {
		fmt.Fprintf(sb, "%sAttachQuery %s (children %d)\n", indent, n.Dictionary, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Dictionary)
		return // Dictionary doesn't have columns or storage
	} else {
		fmt.Fprintf(sb, "%sAttachQuery\n", indent)
		return
	}

	// Output columns definition
	if hasColumns {
		columnsChildren := 0
		if len(n.Columns) > 0 {
			columnsChildren++
		}
		if len(n.Indexes) > 0 {
			columnsChildren++
		}
		if len(n.ColumnsPrimaryKey) > 0 {
			columnsChildren++
		}
		fmt.Fprintf(sb, "%s Columns definition (children %d)\n", indent, columnsChildren)
		if len(n.Columns) > 0 {
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Columns))
			for _, col := range n.Columns {
				Column(sb, col, depth+3)
			}
		}
		// Output indexes
		if len(n.Indexes) > 0 {
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(n.Indexes))
			for _, idx := range n.Indexes {
				Index(sb, idx, depth+3)
			}
		}
		// Output inline PRIMARY KEY (from column list)
		if len(n.ColumnsPrimaryKey) > 0 {
			if len(n.ColumnsPrimaryKey) > 1 {
				// Multiple columns: wrap in Function tuple
				fmt.Fprintf(sb, "%s  Function tuple (children 1)\n", indent)
				fmt.Fprintf(sb, "%s   ExpressionList (children %d)\n", indent, len(n.ColumnsPrimaryKey))
				for _, pk := range n.ColumnsPrimaryKey {
					Node(sb, pk, depth+4)
				}
			} else {
				// Single column: output directly
				for _, pk := range n.ColumnsPrimaryKey {
					Node(sb, pk, depth+2)
				}
			}
		}
	}

	// Output select query (for materialized views)
	if hasSelectQuery {
		Node(sb, n.SelectQuery, depth+1)
	}

	// Output storage definition (or ViewTargets for materialized views)
	if hasStorage {
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

		// For materialized views, wrap in ViewTargets
		if n.IsMaterializedView {
			fmt.Fprintf(sb, "%s ViewTargets (children 1)\n", indent)
			fmt.Fprintf(sb, "%s  Storage definition (children %d)\n", indent, storageChildren)
			if n.Engine != nil {
				if n.Engine.HasParentheses {
					fmt.Fprintf(sb, "%s   Function %s (children 1)\n", indent, n.Engine.Name)
					if len(n.Engine.Parameters) > 0 {
						fmt.Fprintf(sb, "%s    ExpressionList (children %d)\n", indent, len(n.Engine.Parameters))
						for _, param := range n.Engine.Parameters {
							Node(sb, param, depth+5)
						}
					} else {
						fmt.Fprintf(sb, "%s    ExpressionList\n", indent)
					}
				} else {
					fmt.Fprintf(sb, "%s   Function %s\n", indent, n.Engine.Name)
				}
			}
			if n.PartitionBy != nil {
				Node(sb, n.PartitionBy, depth+3)
			}
			if len(n.OrderBy) > 0 {
				for _, expr := range n.OrderBy {
					Node(sb, expr, depth+3)
				}
			}
			if len(n.PrimaryKey) > 0 {
				for _, expr := range n.PrimaryKey {
					Node(sb, expr, depth+3)
				}
			}
			if len(n.Settings) > 0 {
				fmt.Fprintf(sb, "%s   Set\n", indent)
			}
		} else {
			fmt.Fprintf(sb, "%s Storage definition (children %d)\n", indent, storageChildren)
			if n.Engine != nil {
				if n.Engine.HasParentheses {
					fmt.Fprintf(sb, "%s  Function %s (children 1)\n", indent, n.Engine.Name)
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
				Node(sb, n.PartitionBy, depth+2)
			}
			if len(n.OrderBy) > 0 {
				for _, expr := range n.OrderBy {
					Node(sb, expr, depth+2)
				}
			}
			if len(n.PrimaryKey) > 0 {
				for _, expr := range n.PrimaryKey {
					Node(sb, expr, depth+2)
				}
			}
			if len(n.Settings) > 0 {
				fmt.Fprintf(sb, "%s  Set\n", indent)
			}
		}
	}
}

func explainBackupQuery(sb *strings.Builder, n *ast.BackupQuery, indent string) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.BackupQuery\n", indent)
		return
	}

	// Count children: function target + format identifier
	children := 0
	if n.Target != nil {
		children++
	}
	if n.Format != "" {
		children++
	}

	if children > 0 {
		fmt.Fprintf(sb, "%sBackupQuery (children %d)\n", indent, children)
	} else {
		fmt.Fprintf(sb, "%sBackupQuery\n", indent)
	}

	// Output target function (e.g., Null, Disk('path'))
	if n.Target != nil {
		fmt.Fprintf(sb, "%s Function %s\n", indent, n.Target.Name)
	}

	// Output format identifier
	if n.Format != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
	}
}

func explainRestoreQuery(sb *strings.Builder, n *ast.RestoreQuery, indent string) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.RestoreQuery\n", indent)
		return
	}

	// Count children: function source + format identifier
	children := 0
	if n.Source != nil {
		children++
	}
	if n.Format != "" {
		children++
	}

	if children > 0 {
		fmt.Fprintf(sb, "%sRestoreQuery (children %d)\n", indent, children)
	} else {
		fmt.Fprintf(sb, "%sRestoreQuery\n", indent)
	}

	// Output source function (e.g., Null, Disk('path'))
	if n.Source != nil {
		fmt.Fprintf(sb, "%s Function %s\n", indent, n.Source.Name)
	}

	// Output format identifier
	if n.Format != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
	}
}

func explainAlterQuery(sb *strings.Builder, n *ast.AlterQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.AlterQuery\n", indent)
		return
	}

	children := 2 // ExpressionList + Identifier for table
	if n.Database != "" {
		children = 3 // ExpressionList + Identifier for database + Identifier for table
	}
	if len(n.Settings) > 0 {
		children++ // Add Set child for SETTINGS
	}
	hasFormat := n.Format != ""
	if hasFormat {
		children++ // Add Identifier for FORMAT
	}
	if n.Database != "" {
		fmt.Fprintf(sb, "%sAlterQuery %s %s (children %d)\n", indent, n.Database, n.Table, children)
	} else {
		fmt.Fprintf(sb, "%sAlterQuery  %s (children %d)\n", indent, n.Table, children)
	}

	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Commands))
	for _, cmd := range n.Commands {
		explainAlterCommand(sb, cmd, indent+"  ", depth+2)
	}
	if n.Database != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	if hasFormat {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
	}
	if len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainAlterCommand(sb *strings.Builder, cmd *ast.AlterCommand, indent string, depth int) {
	children := countAlterCommandChildren(cmd)
	// Normalize command types to match ClickHouse EXPLAIN AST output
	cmdType := cmd.Type
	if cmdType == ast.AlterClearStatistics {
		cmdType = ast.AlterDropStatistics
	}
	// DETACH_PARTITION is shown as DROP_PARTITION in EXPLAIN AST
	if cmdType == ast.AlterDetachPartition {
		cmdType = ast.AlterDropPartition
	}
	// CLEAR_COLUMN is shown as DROP_COLUMN in EXPLAIN AST
	if cmdType == ast.AlterClearColumn {
		cmdType = ast.AlterDropColumn
	}
	// CLEAR_INDEX is shown as DROP_INDEX in EXPLAIN AST
	if cmdType == ast.AlterClearIndex {
		cmdType = ast.AlterDropIndex
	}
	// DELETE_WHERE is shown as DELETE in EXPLAIN AST
	if cmdType == ast.AlterDeleteWhere {
		cmdType = "DELETE"
	}
	// FREEZE (without partition) is shown as FREEZE_ALL in EXPLAIN AST
	if cmdType == ast.AlterFreeze {
		cmdType = "FREEZE_ALL"
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sAlterCommand %s (children %d)\n", indent, cmdType, children)
	} else {
		fmt.Fprintf(sb, "%sAlterCommand %s\n", indent, cmdType)
	}

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
		// For MODIFY COLUMN ... MODIFY SETTING
		if len(cmd.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
		// For MODIFY COLUMN ... RESET SETTING (outputs ExpressionList with Identifiers)
		if len(cmd.ResetSettings) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(cmd.ResetSettings))
			for _, name := range cmd.ResetSettings {
				fmt.Fprintf(sb, "%s  Identifier %s\n", indent, name)
			}
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
			// PARTITION ALL is shown as Partition_ID (empty) in EXPLAIN AST
			if ident, ok := cmd.Partition.(*ast.Identifier); ok && strings.ToUpper(ident.Name()) == "ALL" {
				fmt.Fprintf(sb, "%s Partition_ID \n", indent)
			} else {
				fmt.Fprintf(sb, "%s Partition (children 1)\n", indent)
				Node(sb, cmd.Partition, depth+2)
			}
		}
	case ast.AlterCommentColumn:
		if cmd.ColumnName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ColumnName)
		}
		if cmd.Comment != "" {
			fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, escapeStringLiteral(cmd.Comment))
		}
	case ast.AlterModifyComment:
		if cmd.Comment != "" {
			fmt.Fprintf(sb, "%s Literal \\'%s\\'\n", indent, escapeStringLiteral(cmd.Comment))
		}
	case ast.AlterAddIndex:
		// ADD INDEX outputs the full Index definition with expression and type
		if cmd.IndexDef != nil && (cmd.IndexDef.Expression != nil || cmd.IndexDef.Type != nil) {
			Index(sb, cmd.IndexDef, depth+1)
		} else if cmd.Index != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.Index)
		}
		// AFTER clause
		if cmd.AfterIndex != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.AfterIndex)
		}
	case ast.AlterDropIndex, ast.AlterClearIndex:
		if cmd.Index != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.Index)
		}
		// CLEAR INDEX IN PARTITION clause
		if cmd.Partition != nil {
			// PARTITION ALL is shown as Partition_ID (empty) in EXPLAIN AST
			if ident, ok := cmd.Partition.(*ast.Identifier); ok && strings.ToUpper(ident.Name()) == "ALL" {
				fmt.Fprintf(sb, "%s Partition_ID \n", indent)
			} else {
				fmt.Fprintf(sb, "%s Partition (children 1)\n", indent)
				Node(sb, cmd.Partition, depth+2)
			}
		}
	case ast.AlterMaterializeIndex:
		if cmd.Index != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.Index)
		}
		// MATERIALIZE INDEX can have IN PARTITION or IN PARTITION ID clause
		if cmd.Partition != nil {
			if cmd.PartitionIsID {
				if lit, ok := cmd.Partition.(*ast.Literal); ok {
					fmt.Fprintf(sb, "%s Partition_ID Literal_\\'%s\\' (children 1)\n", indent, lit.Value)
					Node(sb, cmd.Partition, depth+2)
				} else {
					fmt.Fprintf(sb, "%s Partition_ID (children 1)\n", indent)
					Node(sb, cmd.Partition, depth+2)
				}
			} else {
				fmt.Fprintf(sb, "%s Partition (children 1)\n", indent)
				Node(sb, cmd.Partition, depth+2)
			}
		}
	case ast.AlterMaterializeColumn:
		if cmd.ColumnName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ColumnName)
		}
		if cmd.Partition != nil {
			fmt.Fprintf(sb, "%s Partition (children 1)\n", indent)
			Node(sb, cmd.Partition, depth+2)
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
			// TTL is wrapped in ExpressionList and TTLElement
			// Count total TTL elements (1 for Expression + len(Expressions))
			ttlCount := 1 + len(cmd.TTL.Expressions)
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, ttlCount)
			fmt.Fprintf(sb, "%s  TTLElement (children 1)\n", indent)
			Node(sb, cmd.TTL.Expression, depth+3)
			for _, expr := range cmd.TTL.Expressions {
				fmt.Fprintf(sb, "%s  TTLElement (children 1)\n", indent)
				Node(sb, expr, depth+3)
			}
		}
	case ast.AlterModifySetting:
		fmt.Fprintf(sb, "%s Set\n", indent)
	case ast.AlterDropPartition, ast.AlterDetachPartition, ast.AlterAttachPartition,
		ast.AlterReplacePartition, ast.AlterFetchPartition, ast.AlterMovePartition, ast.AlterFreezePartition, ast.AlterApplyPatches:
		if cmd.Partition != nil {
			// PARTITION ALL is shown as Partition_ID (empty) in EXPLAIN AST
			if ident, ok := cmd.Partition.(*ast.Identifier); ok && strings.ToUpper(ident.Name()) == "ALL" {
				fmt.Fprintf(sb, "%s Partition_ID \n", indent)
			} else if cmd.PartitionIsID {
				// PARTITION ID 'value' is shown as Partition_ID Literal_'value' (children 1)
				if lit, ok := cmd.Partition.(*ast.Literal); ok {
					fmt.Fprintf(sb, "%s Partition_ID Literal_\\'%s\\' (children 1)\n", indent, lit.Value)
					Node(sb, cmd.Partition, depth+2)
				} else {
					fmt.Fprintf(sb, "%s Partition_ID (children 1)\n", indent)
					Node(sb, cmd.Partition, depth+2)
				}
			} else if cmd.IsPart {
				// PART expressions are output directly without Partition wrapper
				Node(sb, cmd.Partition, depth+1)
			} else {
				fmt.Fprintf(sb, "%s Partition (children 1)\n", indent)
				Node(sb, cmd.Partition, depth+2)
			}
		}
	case ast.AlterFreeze:
		// No children
	case ast.AlterDeleteWhere:
		if cmd.Where != nil {
			Node(sb, cmd.Where, depth+1)
		}
	case ast.AlterUpdate:
		// Output order: Partition, Where, Assignments
		if cmd.Partition != nil {
			// PARTITION ALL is shown as Partition_ID (empty) in EXPLAIN AST
			if ident, ok := cmd.Partition.(*ast.Identifier); ok && strings.ToUpper(ident.Name()) == "ALL" {
				fmt.Fprintf(sb, "%s Partition_ID \n", indent)
			} else {
				fmt.Fprintf(sb, "%s Partition (children 1)\n", indent)
				Node(sb, cmd.Partition, depth+2)
			}
		}
		if cmd.Where != nil {
			Node(sb, cmd.Where, depth+1)
		}
		if len(cmd.Assignments) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(cmd.Assignments))
			for _, assign := range cmd.Assignments {
				fmt.Fprintf(sb, "%s  Assignment %s (children 1)\n", indent, assign.Column)
				Node(sb, assign.Value, depth+3)
			}
		}
	case ast.AlterAddProjection:
		if cmd.Projection != nil {
			explainProjection(sb, cmd.Projection, indent+" ", depth+1)
		}
	case ast.AlterDropProjection, ast.AlterMaterializeProjection, ast.AlterClearProjection:
		if cmd.ProjectionName != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, cmd.ProjectionName)
		}
	case ast.AlterAddStatistics, ast.AlterModifyStatistics:
		explainStatisticsCommand(sb, cmd, indent, depth)
	case ast.AlterDropStatistics, ast.AlterClearStatistics, ast.AlterMaterializeStatistics:
		explainStatisticsCommand(sb, cmd, indent, depth)
	case ast.AlterModifyOrderBy:
		// When there are multiple expressions, wrap them in a tuple function
		if len(cmd.OrderByExpr) > 1 {
			fmt.Fprintf(sb, "%s Function tuple (children 1)\n", indent)
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(cmd.OrderByExpr))
			for _, expr := range cmd.OrderByExpr {
				Node(sb, expr, depth+3)
			}
		} else {
			// Single expression - output directly
			for _, expr := range cmd.OrderByExpr {
				Node(sb, expr, depth+1)
			}
		}
	case ast.AlterModifySampleBy:
		// Single expression - output directly
		if cmd.SampleByExpr != nil {
			Node(sb, cmd.SampleByExpr, depth+1)
		}
	case ast.AlterResetSetting:
		// RESET SETTING outputs ExpressionList with Identifier children
		if len(cmd.ResetSettings) > 0 {
			fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(cmd.ResetSettings))
			for _, name := range cmd.ResetSettings {
				fmt.Fprintf(sb, "%s  Identifier %s\n", indent, name)
			}
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
	fmt.Fprintf(sb, "%sProjection (children %d)\n", indent, children)
	if p.Select != nil {
		explainProjectionSelectQuery(sb, p.Select, indent+" ", depth+1)
	}
}

func explainProjectionSelectQuery(sb *strings.Builder, q *ast.ProjectionSelectQuery, indent string, depth int) {
	children := 0
	if len(q.Columns) > 0 {
		children++
	}
	if len(q.OrderBy) > 0 {
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
	if len(q.OrderBy) > 0 {
		if len(q.OrderBy) == 1 {
			// Single column: just output as Identifier
			Node(sb, q.OrderBy[0], depth+1)
		} else {
			// Multiple columns: wrap in Function tuple
			fmt.Fprintf(sb, "%s Function tuple (children 1)\n", indent)
			fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(q.OrderBy))
			for _, col := range q.OrderBy {
				Node(sb, col, depth+3)
			}
		}
	}
	if len(q.GroupBy) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(q.GroupBy))
		for _, expr := range q.GroupBy {
			Node(sb, expr, depth+2)
		}
	}
}

func explainStatisticsCommand(sb *strings.Builder, cmd *ast.AlterCommand, indent string, depth int) {
	// Stat node has 1 child (columns only) or 2 children (columns + types)
	statChildren := 0
	if len(cmd.StatisticsColumns) > 0 {
		statChildren++
	}
	if len(cmd.StatisticsTypes) > 0 {
		statChildren++
	}

	fmt.Fprintf(sb, "%s Stat (children %d)\n", indent, statChildren)

	// First: column names as ExpressionList of Identifiers
	if len(cmd.StatisticsColumns) > 0 {
		fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(cmd.StatisticsColumns))
		for _, col := range cmd.StatisticsColumns {
			fmt.Fprintf(sb, "%s   Identifier %s\n", indent, col)
		}
	}

	// Second: statistics types as ExpressionList of Functions
	if len(cmd.StatisticsTypes) > 0 {
		fmt.Fprintf(sb, "%s  ExpressionList (children %d)\n", indent, len(cmd.StatisticsTypes))
		for _, t := range cmd.StatisticsTypes {
			explainStatisticsTypeFunction(sb, t, indent+"   ", depth+3)
		}
	}
}

func explainStatisticsTypeFunction(sb *strings.Builder, fn *ast.FunctionCall, indent string, depth int) {
	// Statistics type functions always have (children 1) even if no actual arguments
	// because ClickHouse shows them with an empty ExpressionList
	fmt.Fprintf(sb, "%sFunction %s (children 1)\n", indent, fn.Name)
	if len(fn.Arguments) == 0 {
		fmt.Fprintf(sb, "%s ExpressionList\n", indent)
	} else {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(fn.Arguments))
		for _, arg := range fn.Arguments {
			Node(sb, arg, depth+1)
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
		// For MODIFY COLUMN ... MODIFY SETTING
		if len(cmd.Settings) > 0 {
			children++
		}
		// For MODIFY COLUMN ... RESET SETTING
		if len(cmd.ResetSettings) > 0 {
			children++
		}
	case ast.AlterDropColumn:
		if cmd.ColumnName != "" {
			children++
		}
	case ast.AlterCommentColumn:
		if cmd.ColumnName != "" {
			children++
		}
		if cmd.Comment != "" {
			children++
		}
	case ast.AlterModifyComment:
		if cmd.Comment != "" {
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
	case ast.AlterAddIndex:
		// ADD INDEX with IndexDef has 1 child (the Index node)
		if cmd.IndexDef != nil && (cmd.IndexDef.Expression != nil || cmd.IndexDef.Type != nil) {
			children = 1
		} else if cmd.Index != "" {
			children++
		}
		// AFTER clause adds another child
		if cmd.AfterIndex != "" {
			children++
		}
	case ast.AlterDropIndex, ast.AlterClearIndex:
		if cmd.Index != "" {
			children++
		}
		if cmd.Partition != nil {
			children++
		}
	case ast.AlterMaterializeIndex:
		if cmd.Index != "" {
			children++
		}
		// MATERIALIZE INDEX can have IN PARTITION or IN PARTITION ID clause
		if cmd.Partition != nil {
			children++
		}
	case ast.AlterMaterializeColumn:
		if cmd.ColumnName != "" {
			children++
		}
		if cmd.Partition != nil {
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
		ast.AlterReplacePartition, ast.AlterFetchPartition, ast.AlterMovePartition, ast.AlterFreezePartition, ast.AlterApplyPatches:
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
		if cmd.Partition != nil {
			children++
		}
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
	case ast.AlterAddStatistics, ast.AlterModifyStatistics:
		// Statistics commands with TYPE have one child (Stat node)
		if len(cmd.StatisticsColumns) > 0 || len(cmd.StatisticsTypes) > 0 {
			children = 1
		}
	case ast.AlterDropStatistics, ast.AlterClearStatistics, ast.AlterMaterializeStatistics:
		// Statistics commands without TYPE have one child (Stat node with just columns)
		if len(cmd.StatisticsColumns) > 0 {
			children = 1
		}
	case ast.AlterModifyOrderBy:
		// MODIFY ORDER BY: multiple expressions wrapped in tuple (1 child), single expression (1 child)
		if len(cmd.OrderByExpr) > 0 {
			children = 1
		}
	case ast.AlterModifySampleBy:
		// MODIFY SAMPLE BY: single expression (1 child)
		if cmd.SampleByExpr != nil {
			children = 1
		}
	case ast.AlterResetSetting:
		// RESET SETTING: ExpressionList with setting names (1 child)
		if len(cmd.ResetSettings) > 0 {
			children = 1
		}
	default:
		if cmd.Partition != nil {
			children++
		}
	}
	return children
}

func explainOptimizeQuery(sb *strings.Builder, n *ast.OptimizeQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.OptimizeQuery\n", indent)
		return
	}

	name := n.Table
	if n.Final {
		name += "_final"
	}
	if n.Cleanup {
		name += "_cleanup"
	}
	if n.Dedupe {
		name += "_deduplicate"
	}

	hasSettings := len(n.Settings) > 0
	children := 1 // identifier
	if n.Database != "" {
		children++ // extra identifier for database
	}
	if n.Partition != nil {
		children++
	}
	if hasSettings {
		children++
	}

	if n.Database != "" {
		// Database-qualified: OptimizeQuery db table (children N)
		fmt.Fprintf(sb, "%sOptimizeQuery %s %s (children %d)\n", indent, n.Database, name, children)
	} else {
		fmt.Fprintf(sb, "%sOptimizeQuery  %s (children %d)\n", indent, name, children)
	}
	if n.Partition != nil {
		// PARTITION ALL is shown as Partition_ID (empty) in EXPLAIN AST
		if ident, ok := n.Partition.(*ast.Identifier); ok && strings.ToUpper(ident.Name()) == "ALL" {
			fmt.Fprintf(sb, "%s Partition_ID \n", indent)
		} else {
			fmt.Fprintf(sb, "%s Partition (children 1)\n", indent)
			Node(sb, n.Partition, depth+2)
		}
	}
	if n.Database != "" {
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	if hasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainTruncateQuery(sb *strings.Builder, n *ast.TruncateQuery, indent string) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.TruncateQuery\n", indent)
		return
	}

	// Count children (table identifiers + settings)
	hasSettings := len(n.Settings) > 0

	if n.Database != "" {
		// Database-qualified: TruncateQuery db table (children 2 or 3)
		children := 2
		if hasSettings {
			children++
		}
		fmt.Fprintf(sb, "%sTruncateQuery %s %s (children %d)\n", indent, n.Database, n.Table, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	} else {
		children := 1
		if hasSettings {
			children++
		}
		fmt.Fprintf(sb, "%sTruncateQuery  %s (children %d)\n", indent, n.Table, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	}
	if hasSettings {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainDeleteQuery(sb *strings.Builder, n *ast.DeleteQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.DeleteQuery\n", indent)
		return
	}

	// Count children: Where expression + table identifier + settings
	children := 1 // table identifier
	if n.Where != nil {
		children++
	}
	if len(n.Settings) > 0 {
		children++
	}

	fmt.Fprintf(sb, "%sDeleteQuery  %s (children %d)\n", indent, n.Table, children)
	if n.Where != nil {
		Node(sb, n.Where, depth+1)
	}
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
	if len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

func explainCheckQuery(sb *strings.Builder, n *ast.CheckQuery, indent string) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.CheckQuery\n", indent)
		return
	}

	if n.Database != "" {
		// Database-qualified: CheckQuery db table (children N)
		children := 2 // database + table identifiers
		if n.Format != "" {
			children++
		}
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sCheckQuery %s %s (children %d)\n", indent, n.Database, n.Table, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Database)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		if n.Format != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
	} else {
		children := 1 // table identifier
		if n.Format != "" {
			children++
		}
		if len(n.Settings) > 0 {
			children++
		}
		fmt.Fprintf(sb, "%sCheckQuery  %s (children %d)\n", indent, n.Table, children)
		fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
		if n.Format != "" {
			fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Format)
		}
		if len(n.Settings) > 0 {
			fmt.Fprintf(sb, "%s Set\n", indent)
		}
	}
}

func explainCreateIndexQuery(sb *strings.Builder, n *ast.CreateIndexQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.CreateIndexQuery\n", indent)
		return
	}

	// CreateIndexQuery with two spaces before table name, always 3 children
	fmt.Fprintf(sb, "%sCreateIndexQuery  %s (children %d)\n", indent, n.Table, 3)

	// Child 1: Index name
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.IndexName)

	// Child 2: Index wrapper with columns and type
	// Index has 1 child for columns-only, 2 children if TYPE is specified
	indexChildren := 1
	if n.Type != "" {
		indexChildren = 2
	}
	fmt.Fprintf(sb, "%s Index (children %d)\n", indent, indexChildren)

	// For single column, output as Identifier
	// For multiple columns or if there are any special cases, output as Function tuple
	if len(n.Columns) == 1 {
		if ident, ok := n.Columns[0].(*ast.Identifier); ok {
			fmt.Fprintf(sb, "%s  Identifier %s\n", indent, ident.Name())
		} else {
			// Non-identifier expression - wrap in tuple
			fmt.Fprintf(sb, "%s  Function tuple (children 1)\n", indent)
			fmt.Fprintf(sb, "%s   ExpressionList\n", indent)
		}
	} else {
		// Multiple columns or empty - always Function tuple with ExpressionList
		fmt.Fprintf(sb, "%s  Function tuple (children 1)\n", indent)
		fmt.Fprintf(sb, "%s   ExpressionList\n", indent)
	}

	// Output TYPE as Function with empty ExpressionList
	if n.Type != "" {
		fmt.Fprintf(sb, "%s  Function %s (children 1)\n", indent, n.Type)
		fmt.Fprintf(sb, "%s   ExpressionList\n", indent)
	}

	// Child 3: Table name
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)
}

func explainAssignment(sb *strings.Builder, n *ast.Assignment, indent string, depth int) {
	if n == nil {
		return
	}
	// Assignment col_name (children 1)
	fmt.Fprintf(sb, "%sAssignment %s (children 1)\n", indent, n.Column)
	if n.Value != nil {
		Node(sb, n.Value, depth+1)
	}
}

func explainUpdateQuery(sb *strings.Builder, n *ast.UpdateQuery, indent string, depth int) {
	if n == nil {
		fmt.Fprintf(sb, "%s*ast.UpdateQuery\n", indent)
		return
	}

	// Count children: always 3 (identifier, where condition, assignments)
	children := 3

	// UpdateQuery with two spaces before table name
	if n.Database != "" {
		fmt.Fprintf(sb, "%sUpdateQuery %s %s (children %d)\n", indent, n.Database, n.Table, children)
	} else {
		fmt.Fprintf(sb, "%sUpdateQuery  %s (children %d)\n", indent, n.Table, children)
	}

	// Child 1: Table identifier
	fmt.Fprintf(sb, "%s Identifier %s\n", indent, n.Table)

	// Child 2: WHERE condition
	if n.Where != nil {
		Node(sb, n.Where, depth+1)
	}

	// Child 3: Assignments wrapped in ExpressionList
	fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Assignments))
	for _, assign := range n.Assignments {
		Node(sb, assign, depth+2)
	}
}

func explainParallelWithQuery(sb *strings.Builder, n *ast.ParallelWithQuery, indent string, depth int) {
	if n == nil || len(n.Statements) == 0 {
		fmt.Fprintf(sb, "%sParallelWithQuery\n", indent)
		return
	}

	// Build the name from the first statement
	name := getParallelWithName(n.Statements[0])
	count := len(n.Statements)

	fmt.Fprintf(sb, "%sParallelWithQuery %d %s (children %d)\n", indent, count, name, count)

	for _, stmt := range n.Statements {
		Node(sb, stmt, depth+1)
	}
}

func getParallelWithName(stmt ast.Statement) string {
	switch s := stmt.(type) {
	case *ast.DropQuery:
		tableName := ""
		if len(s.Tables) > 0 {
			if s.Tables[0].Table != "" {
				tableName = s.Tables[0].Table
			}
		}
		return "DropQuery__" + tableName
	case *ast.CreateQuery:
		return "CreateQuery_" + s.Table
	case *ast.InsertQuery:
		return "InsertQuery__"
	default:
		return "Statement"
	}
}
