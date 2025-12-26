// Package ast defines the abstract syntax tree for ClickHouse SQL.
package ast

import (
	"encoding/json"
	"math"

	"github.com/sqlc-dev/doubleclick/token"
)

// Node is the interface implemented by all AST nodes.
type Node interface {
	Pos() token.Position
	End() token.Position
}

// Statement is the interface implemented by all statement nodes.
type Statement interface {
	Node
	statementNode()
}

// Expression is the interface implemented by all expression nodes.
type Expression interface {
	Node
	expressionNode()
}

// -----------------------------------------------------------------------------
// Statements

// SelectWithUnionQuery represents a SELECT query possibly with UNION.
type SelectWithUnionQuery struct {
	Position     token.Position `json:"-"`
	Selects      []Statement    `json:"selects"`
	UnionAll     bool           `json:"union_all,omitempty"`
	UnionModes   []string       `json:"union_modes,omitempty"` // "ALL", "DISTINCT", or "" for each union
}

func (s *SelectWithUnionQuery) Pos() token.Position { return s.Position }
func (s *SelectWithUnionQuery) End() token.Position { return s.Position }
func (s *SelectWithUnionQuery) statementNode()      {}

// SelectIntersectExceptQuery represents SELECT ... INTERSECT/EXCEPT ... queries.
type SelectIntersectExceptQuery struct {
	Position token.Position `json:"-"`
	Selects  []Statement    `json:"selects"`
}

func (s *SelectIntersectExceptQuery) Pos() token.Position { return s.Position }
func (s *SelectIntersectExceptQuery) End() token.Position { return s.Position }
func (s *SelectIntersectExceptQuery) statementNode()      {}

// SelectQuery represents a SELECT statement.
type SelectQuery struct {
	Position    token.Position        `json:"-"`
	With        []Expression          `json:"with,omitempty"`
	Distinct    bool                  `json:"distinct,omitempty"`
	Top         Expression            `json:"top,omitempty"`
	Columns     []Expression          `json:"columns"`
	From        *TablesInSelectQuery  `json:"from,omitempty"`
	ArrayJoin   *ArrayJoinClause      `json:"array_join,omitempty"`
	PreWhere    Expression            `json:"prewhere,omitempty"`
	Where       Expression            `json:"where,omitempty"`
	GroupBy     []Expression          `json:"group_by,omitempty"`
	WithRollup  bool                  `json:"with_rollup,omitempty"`
	WithCube    bool                  `json:"with_cube,omitempty"`
	WithTotals  bool                  `json:"with_totals,omitempty"`
	Having      Expression            `json:"having,omitempty"`
	Qualify     Expression            `json:"qualify,omitempty"`
	Window      []*WindowDefinition   `json:"window,omitempty"`
	OrderBy     []*OrderByElement     `json:"order_by,omitempty"`
	Limit            Expression            `json:"limit,omitempty"`
	LimitBy          []Expression          `json:"limit_by,omitempty"`
	LimitByHasLimit  bool                  `json:"limit_by_has_limit,omitempty"` // true if LIMIT BY was followed by another LIMIT
	Offset           Expression            `json:"offset,omitempty"`
	Settings    []*SettingExpr        `json:"settings,omitempty"`
	IntoOutfile *IntoOutfileClause    `json:"into_outfile,omitempty"`
	Format      *Identifier           `json:"format,omitempty"`
}

// ArrayJoinClause represents an ARRAY JOIN clause.
type ArrayJoinClause struct {
	Position token.Position `json:"-"`
	Left     bool           `json:"left,omitempty"`
	Columns  []Expression   `json:"columns"`
}

func (a *ArrayJoinClause) Pos() token.Position { return a.Position }
func (a *ArrayJoinClause) End() token.Position { return a.Position }

// WindowDefinition represents a named window definition in the WINDOW clause.
type WindowDefinition struct {
	Position token.Position `json:"-"`
	Name     string         `json:"name"`
	Spec     *WindowSpec    `json:"spec"`
}

func (w *WindowDefinition) Pos() token.Position { return w.Position }
func (w *WindowDefinition) End() token.Position { return w.Position }

// IntoOutfileClause represents INTO OUTFILE clause.
type IntoOutfileClause struct {
	Position token.Position `json:"-"`
	Filename string         `json:"filename"`
	Truncate bool           `json:"truncate,omitempty"`
}

func (i *IntoOutfileClause) Pos() token.Position { return i.Position }
func (i *IntoOutfileClause) End() token.Position { return i.Position }

func (s *SelectQuery) Pos() token.Position { return s.Position }
func (s *SelectQuery) End() token.Position { return s.Position }
func (s *SelectQuery) statementNode()      {}

// TablesInSelectQuery represents the tables in a SELECT query.
type TablesInSelectQuery struct {
	Position token.Position              `json:"-"`
	Tables   []*TablesInSelectQueryElement `json:"tables"`
}

func (t *TablesInSelectQuery) Pos() token.Position { return t.Position }
func (t *TablesInSelectQuery) End() token.Position { return t.Position }

// TablesInSelectQueryElement represents a single table element in a SELECT.
type TablesInSelectQueryElement struct {
	Position token.Position   `json:"-"`
	Table    *TableExpression `json:"table"`
	Join     *TableJoin       `json:"join,omitempty"`
}

func (t *TablesInSelectQueryElement) Pos() token.Position { return t.Position }
func (t *TablesInSelectQueryElement) End() token.Position { return t.Position }

// TableExpression represents a table reference.
type TableExpression struct {
	Position token.Position `json:"-"`
	Table    Expression     `json:"table"` // TableIdentifier, Subquery, or Function
	Alias    string         `json:"alias,omitempty"`
	Final    bool           `json:"final,omitempty"`
	Sample   *SampleClause  `json:"sample,omitempty"`
}

func (t *TableExpression) Pos() token.Position { return t.Position }
func (t *TableExpression) End() token.Position { return t.Position }

// SampleClause represents a SAMPLE clause.
type SampleClause struct {
	Position token.Position `json:"-"`
	Ratio    Expression     `json:"ratio"`
	Offset   Expression     `json:"offset,omitempty"`
}

func (s *SampleClause) Pos() token.Position { return s.Position }
func (s *SampleClause) End() token.Position { return s.Position }

// TableJoin represents a JOIN clause.
type TableJoin struct {
	Position  token.Position `json:"-"`
	Type      JoinType       `json:"type"`
	Strictness JoinStrictness `json:"strictness,omitempty"`
	Global    bool           `json:"global,omitempty"`
	On        Expression     `json:"on,omitempty"`
	Using     []Expression   `json:"using,omitempty"`
}

func (t *TableJoin) Pos() token.Position { return t.Position }
func (t *TableJoin) End() token.Position { return t.Position }

// JoinType represents the type of join.
type JoinType string

const (
	JoinInner JoinType = "INNER"
	JoinLeft  JoinType = "LEFT"
	JoinRight JoinType = "RIGHT"
	JoinFull  JoinType = "FULL"
	JoinCross JoinType = "CROSS"
	JoinPaste JoinType = "PASTE"
)

// JoinStrictness represents the join strictness.
type JoinStrictness string

const (
	JoinStrictAny  JoinStrictness = "ANY"
	JoinStrictAll  JoinStrictness = "ALL"
	JoinStrictAsof JoinStrictness = "ASOF"
	JoinStrictSemi JoinStrictness = "SEMI"
	JoinStrictAnti JoinStrictness = "ANTI"
)

// OrderByElement represents an ORDER BY element.
type OrderByElement struct {
	Position   token.Position `json:"-"`
	Expression Expression     `json:"expression"`
	Descending bool           `json:"descending,omitempty"`
	NullsFirst *bool          `json:"nulls_first,omitempty"`
	Collate    string         `json:"collate,omitempty"`
	WithFill   bool           `json:"with_fill,omitempty"`
	FillFrom   Expression     `json:"fill_from,omitempty"`
	FillTo     Expression     `json:"fill_to,omitempty"`
	FillStep   Expression     `json:"fill_step,omitempty"`
}

func (o *OrderByElement) Pos() token.Position { return o.Position }
func (o *OrderByElement) End() token.Position { return o.Position }

// SettingExpr represents a setting expression.
type SettingExpr struct {
	Position token.Position `json:"-"`
	Name     string         `json:"name"`
	Value    Expression     `json:"value"`
}

func (s *SettingExpr) Pos() token.Position { return s.Position }
func (s *SettingExpr) End() token.Position { return s.Position }

// InsertQuery represents an INSERT statement.
type InsertQuery struct {
	Position    token.Position `json:"-"`
	Database    string         `json:"database,omitempty"`
	Table       string         `json:"table,omitempty"`
	Function    *FunctionCall  `json:"function,omitempty"` // For INSERT INTO FUNCTION syntax
	Columns     []*Identifier  `json:"columns,omitempty"`
	PartitionBy Expression     `json:"partition_by,omitempty"` // For PARTITION BY clause
	Infile      string         `json:"infile,omitempty"`       // For FROM INFILE clause
	Compression string         `json:"compression,omitempty"`  // For COMPRESSION clause
	Select      Statement      `json:"select,omitempty"`
	Format      *Identifier    `json:"format,omitempty"`
	HasSettings bool           `json:"has_settings,omitempty"` // For SETTINGS clause
}

func (i *InsertQuery) Pos() token.Position { return i.Position }
func (i *InsertQuery) End() token.Position { return i.Position }
func (i *InsertQuery) statementNode()      {}

// CreateQuery represents a CREATE statement.
type CreateQuery struct {
	Position         token.Position       `json:"-"`
	OrReplace        bool                 `json:"or_replace,omitempty"`
	IfNotExists      bool                 `json:"if_not_exists,omitempty"`
	Temporary        bool                 `json:"temporary,omitempty"`
	Database         string               `json:"database,omitempty"`
	Table            string               `json:"table,omitempty"`
	View             string               `json:"view,omitempty"`
	Materialized     bool                 `json:"materialized,omitempty"`
	To               string               `json:"to,omitempty"`       // Target table for materialized views
	Populate         bool                 `json:"populate,omitempty"` // POPULATE for materialized views
	Columns          []*ColumnDeclaration `json:"columns,omitempty"`
	Indexes          []*IndexDefinition   `json:"indexes,omitempty"`
	Constraints      []*Constraint        `json:"constraints,omitempty"`
	Engine           *EngineClause        `json:"engine,omitempty"`
	OrderBy          []Expression         `json:"order_by,omitempty"`
	PartitionBy      Expression           `json:"partition_by,omitempty"`
	PrimaryKey       []Expression         `json:"primary_key,omitempty"`
	SampleBy         Expression           `json:"sample_by,omitempty"`
	TTL              *TTLClause           `json:"ttl,omitempty"`
	Settings         []*SettingExpr       `json:"settings,omitempty"`
	AsSelect         Statement            `json:"as_select,omitempty"`
	Comment          string               `json:"comment,omitempty"`
	OnCluster        string               `json:"on_cluster,omitempty"`
	CreateDatabase   bool                 `json:"create_database,omitempty"`
	CreateFunction   bool                 `json:"create_function,omitempty"`
	CreateUser       bool                 `json:"create_user,omitempty"`
	CreateDictionary bool                 `json:"create_dictionary,omitempty"`
	FunctionName     string               `json:"function_name,omitempty"`
	FunctionBody     Expression           `json:"function_body,omitempty"`
	UserName         string               `json:"user_name,omitempty"`
}

func (c *CreateQuery) Pos() token.Position { return c.Position }
func (c *CreateQuery) End() token.Position { return c.Position }
func (c *CreateQuery) statementNode()      {}

// ColumnDeclaration represents a column definition.
type ColumnDeclaration struct {
	Position      token.Position `json:"-"`
	Name          string         `json:"name"`
	Type          *DataType      `json:"type"`
	Nullable      *bool          `json:"nullable,omitempty"`
	Default       Expression     `json:"default,omitempty"`
	DefaultKind   string         `json:"default_kind,omitempty"` // DEFAULT, MATERIALIZED, ALIAS, EPHEMERAL
	Codec         *CodecExpr     `json:"codec,omitempty"`
	TTL           Expression     `json:"ttl,omitempty"`
	PrimaryKey    bool           `json:"primary_key,omitempty"` // PRIMARY KEY constraint
	Comment       string         `json:"comment,omitempty"`
}

func (c *ColumnDeclaration) Pos() token.Position { return c.Position }
func (c *ColumnDeclaration) End() token.Position { return c.Position }

// DataType represents a data type.
type DataType struct {
	Position       token.Position `json:"-"`
	Name           string         `json:"name"`
	Parameters     []Expression   `json:"parameters,omitempty"`
	HasParentheses bool           `json:"has_parentheses,omitempty"`
}

func (d *DataType) Pos() token.Position { return d.Position }
func (d *DataType) End() token.Position { return d.Position }
func (d *DataType) expressionNode()     {}

// NameTypePair represents a named type pair, used in Nested types.
type NameTypePair struct {
	Position token.Position `json:"-"`
	Name     string         `json:"name"`
	Type     *DataType      `json:"type"`
}

func (n *NameTypePair) Pos() token.Position { return n.Position }
func (n *NameTypePair) End() token.Position { return n.Position }
func (n *NameTypePair) expressionNode()     {}

// CodecExpr represents a CODEC expression.
type CodecExpr struct {
	Position token.Position  `json:"-"`
	Codecs   []*FunctionCall `json:"codecs"`
}

func (c *CodecExpr) Pos() token.Position { return c.Position }
func (c *CodecExpr) End() token.Position { return c.Position }

// IndexDefinition represents an INDEX definition in CREATE TABLE.
type IndexDefinition struct {
	Position    token.Position `json:"-"`
	Name        string         `json:"name"`
	Expression  Expression     `json:"expression"`
	Type        *FunctionCall  `json:"type"`
	Granularity Expression     `json:"granularity,omitempty"`
}

func (i *IndexDefinition) Pos() token.Position { return i.Position }
func (i *IndexDefinition) End() token.Position { return i.Position }
func (i *IndexDefinition) expressionNode()     {}

// Constraint represents a table constraint.
type Constraint struct {
	Position   token.Position `json:"-"`
	Name       string         `json:"name,omitempty"`
	Expression Expression     `json:"expression"`
}

func (c *Constraint) Pos() token.Position { return c.Position }
func (c *Constraint) End() token.Position { return c.Position }

// EngineClause represents an ENGINE clause.
type EngineClause struct {
	Position      token.Position `json:"-"`
	Name          string         `json:"name"`
	Parameters    []Expression   `json:"parameters,omitempty"`
	HasParentheses bool          `json:"has_parentheses,omitempty"` // true if called with ()
}

func (e *EngineClause) Pos() token.Position { return e.Position }
func (e *EngineClause) End() token.Position { return e.Position }

// TTLClause represents a TTL clause.
type TTLClause struct {
	Position   token.Position `json:"-"`
	Expression Expression     `json:"expression"`
}

func (t *TTLClause) Pos() token.Position { return t.Position }
func (t *TTLClause) End() token.Position { return t.Position }

// DropQuery represents a DROP statement.
type DropQuery struct {
	Position     token.Position     `json:"-"`
	IfExists     bool               `json:"if_exists,omitempty"`
	Database     string             `json:"database,omitempty"`
	Table        string             `json:"table,omitempty"`
	Tables       []*TableIdentifier `json:"tables,omitempty"` // For DROP TABLE t1, t2, t3
	View         string             `json:"view,omitempty"`
	User         string             `json:"user,omitempty"`
	Function     string             `json:"function,omitempty"` // For DROP FUNCTION
	Temporary    bool               `json:"temporary,omitempty"`
	OnCluster    string             `json:"on_cluster,omitempty"`
	DropDatabase bool               `json:"drop_database,omitempty"`
	Sync         bool               `json:"sync,omitempty"`
}

func (d *DropQuery) Pos() token.Position { return d.Position }
func (d *DropQuery) End() token.Position { return d.Position }
func (d *DropQuery) statementNode()      {}

// AlterQuery represents an ALTER statement.
type AlterQuery struct {
	Position  token.Position  `json:"-"`
	Database  string          `json:"database,omitempty"`
	Table     string          `json:"table"`
	Commands  []*AlterCommand `json:"commands"`
	OnCluster string          `json:"on_cluster,omitempty"`
}

func (a *AlterQuery) Pos() token.Position { return a.Position }
func (a *AlterQuery) End() token.Position { return a.Position }
func (a *AlterQuery) statementNode()      {}

// AlterCommand represents an ALTER command.
type AlterCommand struct {
	Position       token.Position       `json:"-"`
	Type           AlterCommandType     `json:"type"`
	Column         *ColumnDeclaration   `json:"column,omitempty"`
	ColumnName     string               `json:"column_name,omitempty"`
	AfterColumn    string               `json:"after_column,omitempty"`
	NewName        string               `json:"new_name,omitempty"`
	IfNotExists    bool                 `json:"if_not_exists,omitempty"`
	IfExists       bool                 `json:"if_exists,omitempty"`
	Index          string               `json:"index,omitempty"`
	IndexExpr      Expression           `json:"index_expr,omitempty"`
	IndexType      string               `json:"index_type,omitempty"`
	Granularity    int                  `json:"granularity,omitempty"`
	Constraint     *Constraint          `json:"constraint,omitempty"`
	ConstraintName string               `json:"constraint_name,omitempty"`
	Partition      Expression           `json:"partition,omitempty"`
	FromTable      string               `json:"from_table,omitempty"`
	TTL            *TTLClause           `json:"ttl,omitempty"`
	Settings       []*SettingExpr       `json:"settings,omitempty"`
	Where          Expression           `json:"where,omitempty"`       // For DELETE WHERE
	Assignments    []*Assignment        `json:"assignments,omitempty"` // For UPDATE
}

// Assignment represents a column assignment in UPDATE.
type Assignment struct {
	Position token.Position `json:"-"`
	Column   string         `json:"column"`
	Value    Expression     `json:"value"`
}

func (a *Assignment) Pos() token.Position { return a.Position }
func (a *Assignment) End() token.Position { return a.Position }

func (a *AlterCommand) Pos() token.Position { return a.Position }
func (a *AlterCommand) End() token.Position { return a.Position }

// AlterCommandType represents the type of ALTER command.
type AlterCommandType string

const (
	AlterAddColumn         AlterCommandType = "ADD_COLUMN"
	AlterDropColumn        AlterCommandType = "DROP_COLUMN"
	AlterModifyColumn      AlterCommandType = "MODIFY_COLUMN"
	AlterRenameColumn      AlterCommandType = "RENAME_COLUMN"
	AlterClearColumn       AlterCommandType = "CLEAR_COLUMN"
	AlterCommentColumn     AlterCommandType = "COMMENT_COLUMN"
	AlterAddIndex          AlterCommandType = "ADD_INDEX"
	AlterDropIndex         AlterCommandType = "DROP_INDEX"
	AlterClearIndex        AlterCommandType = "CLEAR_INDEX"
	AlterMaterializeIndex  AlterCommandType = "MATERIALIZE_INDEX"
	AlterAddConstraint     AlterCommandType = "ADD_CONSTRAINT"
	AlterDropConstraint    AlterCommandType = "DROP_CONSTRAINT"
	AlterModifyTTL         AlterCommandType = "MODIFY_TTL"
	AlterModifySetting     AlterCommandType = "MODIFY_SETTING"
	AlterDropPartition     AlterCommandType = "DROP_PARTITION"
	AlterDetachPartition   AlterCommandType = "DETACH_PARTITION"
	AlterAttachPartition   AlterCommandType = "ATTACH_PARTITION"
	AlterReplacePartition  AlterCommandType = "REPLACE_PARTITION"
	AlterFreezePartition   AlterCommandType = "FREEZE_PARTITION"
	AlterFreeze            AlterCommandType = "FREEZE"
	AlterDeleteWhere       AlterCommandType = "DELETE_WHERE"
	AlterUpdate            AlterCommandType = "UPDATE"
)

// TruncateQuery represents a TRUNCATE statement.
type TruncateQuery struct {
	Position  token.Position `json:"-"`
	IfExists  bool           `json:"if_exists,omitempty"`
	Database  string         `json:"database,omitempty"`
	Table     string         `json:"table"`
	OnCluster string         `json:"on_cluster,omitempty"`
}

func (t *TruncateQuery) Pos() token.Position { return t.Position }
func (t *TruncateQuery) End() token.Position { return t.Position }
func (t *TruncateQuery) statementNode()      {}

// UseQuery represents a USE statement.
type UseQuery struct {
	Position token.Position `json:"-"`
	Database string         `json:"database"`
}

func (u *UseQuery) Pos() token.Position { return u.Position }
func (u *UseQuery) End() token.Position { return u.Position }
func (u *UseQuery) statementNode()      {}

// DescribeQuery represents a DESCRIBE statement.
type DescribeQuery struct {
	Position      token.Position `json:"-"`
	Database      string         `json:"database,omitempty"`
	Table         string         `json:"table,omitempty"`
	TableFunction *FunctionCall  `json:"table_function,omitempty"`
	Settings      []*SettingExpr `json:"settings,omitempty"`
	Format        string         `json:"format,omitempty"`
}

func (d *DescribeQuery) Pos() token.Position { return d.Position }
func (d *DescribeQuery) End() token.Position { return d.Position }
func (d *DescribeQuery) statementNode()      {}

// ShowQuery represents a SHOW statement.
type ShowQuery struct {
	Position  token.Position `json:"-"`
	ShowType  ShowType       `json:"show_type"`
	Database  string         `json:"database,omitempty"`
	From      string         `json:"from,omitempty"`
	Like      string         `json:"like,omitempty"`
	Where     Expression     `json:"where,omitempty"`
	Limit     Expression     `json:"limit,omitempty"`
}

func (s *ShowQuery) Pos() token.Position { return s.Position }
func (s *ShowQuery) End() token.Position { return s.Position }
func (s *ShowQuery) statementNode()      {}

// ShowType represents the type of SHOW statement.
type ShowType string

const (
	ShowTables        ShowType = "TABLES"
	ShowDatabases     ShowType = "DATABASES"
	ShowProcesses     ShowType = "PROCESSLIST"
	ShowCreate        ShowType = "CREATE"
	ShowCreateDB      ShowType = "CREATE_DATABASE"
	ShowColumns       ShowType = "COLUMNS"
	ShowDictionaries  ShowType = "DICTIONARIES"
	ShowFunctions     ShowType = "FUNCTIONS"
	ShowSettings      ShowType = "SETTINGS"
)

// ExplainQuery represents an EXPLAIN statement.
type ExplainQuery struct {
	Position    token.Position `json:"-"`
	ExplainType ExplainType    `json:"explain_type"`
	Statement   Statement      `json:"statement"`
}

func (e *ExplainQuery) Pos() token.Position { return e.Position }
func (e *ExplainQuery) End() token.Position { return e.Position }
func (e *ExplainQuery) statementNode()      {}

// ExplainType represents the type of EXPLAIN.
type ExplainType string

const (
	ExplainAST                ExplainType = "AST"
	ExplainSyntax             ExplainType = "SYNTAX"
	ExplainPlan               ExplainType = "PLAN"
	ExplainPipeline           ExplainType = "PIPELINE"
	ExplainEstimate           ExplainType = "ESTIMATE"
	ExplainCurrentTransaction ExplainType = "CURRENT TRANSACTION"
)

// SetQuery represents a SET statement.
type SetQuery struct {
	Position token.Position `json:"-"`
	Settings []*SettingExpr `json:"settings"`
}

func (s *SetQuery) Pos() token.Position { return s.Position }
func (s *SetQuery) End() token.Position { return s.Position }
func (s *SetQuery) statementNode()      {}

// OptimizeQuery represents an OPTIMIZE statement.
type OptimizeQuery struct {
	Position  token.Position `json:"-"`
	Database  string         `json:"database,omitempty"`
	Table     string         `json:"table"`
	Partition Expression     `json:"partition,omitempty"`
	Final     bool           `json:"final,omitempty"`
	Dedupe    bool           `json:"dedupe,omitempty"`
	OnCluster string         `json:"on_cluster,omitempty"`
}

func (o *OptimizeQuery) Pos() token.Position { return o.Position }
func (o *OptimizeQuery) End() token.Position { return o.Position }
func (o *OptimizeQuery) statementNode()      {}

// SystemQuery represents a SYSTEM statement.
type SystemQuery struct {
	Position token.Position `json:"-"`
	Command  string         `json:"command"`
	Database string         `json:"database,omitempty"`
	Table    string         `json:"table,omitempty"`
}

func (s *SystemQuery) Pos() token.Position { return s.Position }
func (s *SystemQuery) End() token.Position { return s.Position }
func (s *SystemQuery) statementNode()      {}

// RenamePair represents a single rename pair in RENAME TABLE.
type RenamePair struct {
	FromDatabase string `json:"from_database,omitempty"`
	FromTable    string `json:"from_table"`
	ToDatabase   string `json:"to_database,omitempty"`
	ToTable      string `json:"to_table"`
}

// RenameQuery represents a RENAME TABLE statement.
type RenameQuery struct {
	Position  token.Position `json:"-"`
	Pairs     []*RenamePair  `json:"pairs"`             // Multiple rename pairs
	From      string         `json:"from,omitempty"`    // Deprecated: for backward compat
	To        string         `json:"to,omitempty"`      // Deprecated: for backward compat
	OnCluster string         `json:"on_cluster,omitempty"`
}

func (r *RenameQuery) Pos() token.Position { return r.Position }
func (r *RenameQuery) End() token.Position { return r.Position }
func (r *RenameQuery) statementNode()      {}

// ExchangeQuery represents an EXCHANGE TABLES statement.
type ExchangeQuery struct {
	Position  token.Position `json:"-"`
	Table1    string         `json:"table1"`
	Table2    string         `json:"table2"`
	OnCluster string         `json:"on_cluster,omitempty"`
}

func (e *ExchangeQuery) Pos() token.Position { return e.Position }
func (e *ExchangeQuery) End() token.Position { return e.Position }
func (e *ExchangeQuery) statementNode()      {}

// ExistsQuery represents an EXISTS table_name statement (check if table exists).
type ExistsQuery struct {
	Position token.Position `json:"-"`
	Database string         `json:"database,omitempty"`
	Table    string         `json:"table"`
}

func (e *ExistsQuery) Pos() token.Position { return e.Position }
func (e *ExistsQuery) End() token.Position { return e.Position }
func (e *ExistsQuery) statementNode()      {}

// ShowPrivilegesQuery represents a SHOW PRIVILEGES statement.
type ShowPrivilegesQuery struct {
	Position token.Position `json:"-"`
}

func (s *ShowPrivilegesQuery) Pos() token.Position { return s.Position }
func (s *ShowPrivilegesQuery) End() token.Position { return s.Position }
func (s *ShowPrivilegesQuery) statementNode()      {}

// ShowCreateQuotaQuery represents a SHOW CREATE QUOTA statement.
type ShowCreateQuotaQuery struct {
	Position token.Position `json:"-"`
	Name     string         `json:"name,omitempty"`
}

func (s *ShowCreateQuotaQuery) Pos() token.Position { return s.Position }
func (s *ShowCreateQuotaQuery) End() token.Position { return s.Position }
func (s *ShowCreateQuotaQuery) statementNode()      {}

// -----------------------------------------------------------------------------
// Expressions

// Identifier represents an identifier.
type Identifier struct {
	Position token.Position `json:"-"`
	Parts    []string       `json:"parts"` // e.g., ["db", "table", "column"] for db.table.column
	Alias    string         `json:"alias,omitempty"`
}

func (i *Identifier) Pos() token.Position { return i.Position }
func (i *Identifier) End() token.Position { return i.Position }
func (i *Identifier) expressionNode()     {}

// Name returns the full identifier name.
func (i *Identifier) Name() string {
	if len(i.Parts) == 0 {
		return ""
	}
	if len(i.Parts) == 1 {
		return i.Parts[0]
	}
	result := i.Parts[0]
	for _, p := range i.Parts[1:] {
		result += "." + p
	}
	return result
}

// TableIdentifier represents a table identifier.
type TableIdentifier struct {
	Position token.Position `json:"-"`
	Database string         `json:"database,omitempty"`
	Table    string         `json:"table"`
	Alias    string         `json:"alias,omitempty"`
}

func (t *TableIdentifier) Pos() token.Position { return t.Position }
func (t *TableIdentifier) End() token.Position { return t.Position }
func (t *TableIdentifier) expressionNode()     {}

// Literal represents a literal value.
type Literal struct {
	Position token.Position `json:"-"`
	Type     LiteralType    `json:"type"`
	Value    interface{}    `json:"value"`
}

func (l *Literal) Pos() token.Position { return l.Position }
func (l *Literal) End() token.Position { return l.Position }
func (l *Literal) expressionNode()     {}

// MarshalJSON handles special float values (NaN, +Inf, -Inf) that JSON doesn't support.
func (l *Literal) MarshalJSON() ([]byte, error) {
	type literalAlias Literal
	// Handle special float values
	if f, ok := l.Value.(float64); ok {
		if math.IsNaN(f) {
			return json.Marshal(&struct {
				*literalAlias
				Value string `json:"value"`
			}{
				literalAlias: (*literalAlias)(l),
				Value:        "NaN",
			})
		}
		if math.IsInf(f, 1) {
			return json.Marshal(&struct {
				*literalAlias
				Value string `json:"value"`
			}{
				literalAlias: (*literalAlias)(l),
				Value:        "+Inf",
			})
		}
		if math.IsInf(f, -1) {
			return json.Marshal(&struct {
				*literalAlias
				Value string `json:"value"`
			}{
				literalAlias: (*literalAlias)(l),
				Value:        "-Inf",
			})
		}
	}
	return json.Marshal((*literalAlias)(l))
}

// LiteralType represents the type of a literal.
type LiteralType string

const (
	LiteralString  LiteralType = "String"
	LiteralInteger LiteralType = "Integer"
	LiteralFloat   LiteralType = "Float"
	LiteralBoolean LiteralType = "Boolean"
	LiteralNull    LiteralType = "Null"
	LiteralArray   LiteralType = "Array"
	LiteralTuple   LiteralType = "Tuple"
)

// Asterisk represents a *.
type Asterisk struct {
	Position token.Position  `json:"-"`
	Table    string          `json:"table,omitempty"`   // for table.*
	Except   []string        `json:"except,omitempty"`  // for * EXCEPT (col1, col2)
	Replace  []*ReplaceExpr  `json:"replace,omitempty"` // for * REPLACE (expr AS col)
}

func (a *Asterisk) Pos() token.Position { return a.Position }
func (a *Asterisk) End() token.Position { return a.Position }
func (a *Asterisk) expressionNode()     {}

// ReplaceExpr represents an expression in REPLACE clause.
type ReplaceExpr struct {
	Position token.Position `json:"-"`
	Expr     Expression     `json:"expr"`
	Name     string         `json:"name"`
}

func (r *ReplaceExpr) Pos() token.Position { return r.Position }
func (r *ReplaceExpr) End() token.Position { return r.Position }

// ColumnsMatcher represents COLUMNS('pattern') expression.
type ColumnsMatcher struct {
	Position token.Position `json:"-"`
	Pattern  string         `json:"pattern"`
	Except   []string       `json:"except,omitempty"`
}

func (c *ColumnsMatcher) Pos() token.Position { return c.Position }
func (c *ColumnsMatcher) End() token.Position { return c.Position }
func (c *ColumnsMatcher) expressionNode()     {}

// FunctionCall represents a function call.
type FunctionCall struct {
	Position   token.Position `json:"-"`
	Name       string         `json:"name"`
	Parameters []Expression   `json:"parameters,omitempty"` // For parametric functions like quantile(0.9)(x)
	Arguments  []Expression   `json:"arguments,omitempty"`
	Settings   []*SettingExpr `json:"settings,omitempty"` // For table functions with SETTINGS
	Distinct   bool           `json:"distinct,omitempty"`
	Over       *WindowSpec    `json:"over,omitempty"`
	Alias      string         `json:"alias,omitempty"`
}

func (f *FunctionCall) Pos() token.Position { return f.Position }
func (f *FunctionCall) End() token.Position { return f.Position }
func (f *FunctionCall) expressionNode()     {}

// WindowSpec represents a window specification.
type WindowSpec struct {
	Position    token.Position     `json:"-"`
	Name        string             `json:"name,omitempty"`
	PartitionBy []Expression       `json:"partition_by,omitempty"`
	OrderBy     []*OrderByElement  `json:"order_by,omitempty"`
	Frame       *WindowFrame       `json:"frame,omitempty"`
}

func (w *WindowSpec) Pos() token.Position { return w.Position }
func (w *WindowSpec) End() token.Position { return w.Position }

// WindowFrame represents a window frame.
type WindowFrame struct {
	Position   token.Position  `json:"-"`
	Type       WindowFrameType `json:"type"`
	StartBound *FrameBound     `json:"start"`
	EndBound   *FrameBound     `json:"end,omitempty"`
}

func (w *WindowFrame) Pos() token.Position { return w.Position }
func (w *WindowFrame) End() token.Position { return w.Position }

// WindowFrameType represents the type of window frame.
type WindowFrameType string

const (
	FrameRows   WindowFrameType = "ROWS"
	FrameRange  WindowFrameType = "RANGE"
	FrameGroups WindowFrameType = "GROUPS"
)

// FrameBound represents a window frame bound.
type FrameBound struct {
	Position     token.Position  `json:"-"`
	Type         FrameBoundType  `json:"type"`
	Offset       Expression      `json:"offset,omitempty"`
}

func (f *FrameBound) Pos() token.Position { return f.Position }
func (f *FrameBound) End() token.Position { return f.Position }

// FrameBoundType represents the type of frame bound.
type FrameBoundType string

const (
	BoundCurrentRow   FrameBoundType = "CURRENT_ROW"
	BoundUnboundedPre FrameBoundType = "UNBOUNDED_PRECEDING"
	BoundUnboundedFol FrameBoundType = "UNBOUNDED_FOLLOWING"
	BoundPreceding    FrameBoundType = "PRECEDING"
	BoundFollowing    FrameBoundType = "FOLLOWING"
)

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	Position token.Position `json:"-"`
	Left     Expression     `json:"left"`
	Op       string         `json:"op"`
	Right    Expression     `json:"right"`
}

func (b *BinaryExpr) Pos() token.Position { return b.Position }
func (b *BinaryExpr) End() token.Position { return b.Position }
func (b *BinaryExpr) expressionNode()     {}

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Position token.Position `json:"-"`
	Op       string         `json:"op"`
	Operand  Expression     `json:"operand"`
}

func (u *UnaryExpr) Pos() token.Position { return u.Position }
func (u *UnaryExpr) End() token.Position { return u.Position }
func (u *UnaryExpr) expressionNode()     {}

// TernaryExpr represents a ternary conditional expression (cond ? then : else).
type TernaryExpr struct {
	Position  token.Position `json:"-"`
	Condition Expression     `json:"condition"`
	Then      Expression     `json:"then"`
	Else      Expression     `json:"else"`
}

func (t *TernaryExpr) Pos() token.Position { return t.Position }
func (t *TernaryExpr) End() token.Position { return t.Position }
func (t *TernaryExpr) expressionNode()     {}

// Subquery represents a subquery.
type Subquery struct {
	Position token.Position `json:"-"`
	Query    Statement      `json:"query"`
	Alias    string         `json:"alias,omitempty"`
}

func (s *Subquery) Pos() token.Position { return s.Position }
func (s *Subquery) End() token.Position { return s.Position }
func (s *Subquery) expressionNode()     {}

// WithElement represents a WITH element (CTE).
type WithElement struct {
	Position token.Position `json:"-"`
	Name     string         `json:"name"`
	Query    Expression     `json:"query"` // Subquery or Expression
}

func (w *WithElement) Pos() token.Position { return w.Position }
func (w *WithElement) End() token.Position { return w.Position }
func (w *WithElement) expressionNode()     {}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Position    token.Position  `json:"-"`
	Operand     Expression      `json:"operand,omitempty"` // for CASE x WHEN ...
	Whens       []*WhenClause   `json:"whens"`
	Else        Expression      `json:"else,omitempty"`
	Alias       string          `json:"alias,omitempty"`
}

func (c *CaseExpr) Pos() token.Position { return c.Position }
func (c *CaseExpr) End() token.Position { return c.Position }
func (c *CaseExpr) expressionNode()     {}

// WhenClause represents a WHEN clause in a CASE expression.
type WhenClause struct {
	Position  token.Position `json:"-"`
	Condition Expression     `json:"condition"`
	Result    Expression     `json:"result"`
}

func (w *WhenClause) Pos() token.Position { return w.Position }
func (w *WhenClause) End() token.Position { return w.Position }

// CastExpr represents a CAST expression.
type CastExpr struct {
	Position       token.Position `json:"-"`
	Expr           Expression     `json:"expr"`
	Type           *DataType      `json:"type,omitempty"`
	TypeExpr       Expression     `json:"type_expr,omitempty"` // For dynamic type like CAST(x, if(cond, 'Type1', 'Type2'))
	Alias          string         `json:"alias,omitempty"`
	OperatorSyntax bool           `json:"operator_syntax,omitempty"` // true if using :: syntax
	UsedASSyntax   bool           `json:"-"`                         // true if CAST(x AS Type) syntax used (not CAST(x, 'Type'))
}

func (c *CastExpr) Pos() token.Position { return c.Position }
func (c *CastExpr) End() token.Position { return c.Position }
func (c *CastExpr) expressionNode()     {}

// ExtractExpr represents an EXTRACT expression.
type ExtractExpr struct {
	Position token.Position `json:"-"`
	Field    string         `json:"field"` // YEAR, MONTH, DAY, etc.
	From     Expression     `json:"from"`
	Alias    string         `json:"alias,omitempty"`
}

func (e *ExtractExpr) Pos() token.Position { return e.Position }
func (e *ExtractExpr) End() token.Position { return e.Position }
func (e *ExtractExpr) expressionNode()     {}

// IntervalExpr represents an INTERVAL expression.
type IntervalExpr struct {
	Position token.Position `json:"-"`
	Value    Expression     `json:"value"`
	Unit     string         `json:"unit"` // YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.
}

func (i *IntervalExpr) Pos() token.Position { return i.Position }
func (i *IntervalExpr) End() token.Position { return i.Position }
func (i *IntervalExpr) expressionNode()     {}

// ArrayAccess represents array element access.
type ArrayAccess struct {
	Position token.Position `json:"-"`
	Array    Expression     `json:"array"`
	Index    Expression     `json:"index"`
}

func (a *ArrayAccess) Pos() token.Position { return a.Position }
func (a *ArrayAccess) End() token.Position { return a.Position }
func (a *ArrayAccess) expressionNode()     {}

// TupleAccess represents tuple element access.
type TupleAccess struct {
	Position token.Position `json:"-"`
	Tuple    Expression     `json:"tuple"`
	Index    Expression     `json:"index"`
}

func (t *TupleAccess) Pos() token.Position { return t.Position }
func (t *TupleAccess) End() token.Position { return t.Position }
func (t *TupleAccess) expressionNode()     {}

// Lambda represents a lambda expression.
type Lambda struct {
	Position   token.Position `json:"-"`
	Parameters []string       `json:"parameters"`
	Body       Expression     `json:"body"`
}

func (l *Lambda) Pos() token.Position { return l.Position }
func (l *Lambda) End() token.Position { return l.Position }
func (l *Lambda) expressionNode()     {}

// Parameter represents a parameter placeholder.
type Parameter struct {
	Position token.Position `json:"-"`
	Name     string         `json:"name,omitempty"`
	Type     *DataType      `json:"type,omitempty"`
}

func (p *Parameter) Pos() token.Position { return p.Position }
func (p *Parameter) End() token.Position { return p.Position }
func (p *Parameter) expressionNode()     {}

// AliasedExpr represents an expression with an alias.
type AliasedExpr struct {
	Position token.Position `json:"-"`
	Expr     Expression     `json:"expr"`
	Alias    string         `json:"alias"`
}

func (a *AliasedExpr) Pos() token.Position { return a.Position }
func (a *AliasedExpr) End() token.Position { return a.Position }
func (a *AliasedExpr) expressionNode()     {}

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Position token.Position `json:"-"`
	Expr     Expression     `json:"expr"`
	Not      bool           `json:"not,omitempty"`
	Low      Expression     `json:"low"`
	High     Expression     `json:"high"`
}

func (b *BetweenExpr) Pos() token.Position { return b.Position }
func (b *BetweenExpr) End() token.Position { return b.Position }
func (b *BetweenExpr) expressionNode()     {}

// InExpr represents an IN expression.
type InExpr struct {
	Position token.Position `json:"-"`
	Expr     Expression     `json:"expr"`
	Not      bool           `json:"not,omitempty"`
	Global   bool           `json:"global,omitempty"`
	List     []Expression   `json:"list,omitempty"`
	Query    Statement      `json:"query,omitempty"`
}

func (i *InExpr) Pos() token.Position { return i.Position }
func (i *InExpr) End() token.Position { return i.Position }
func (i *InExpr) expressionNode()     {}

// IsNullExpr represents an IS NULL or IS NOT NULL expression.
type IsNullExpr struct {
	Position token.Position `json:"-"`
	Expr     Expression     `json:"expr"`
	Not      bool           `json:"not,omitempty"`
}

func (i *IsNullExpr) Pos() token.Position { return i.Position }
func (i *IsNullExpr) End() token.Position { return i.Position }
func (i *IsNullExpr) expressionNode()     {}

// LikeExpr represents a LIKE or ILIKE expression.
type LikeExpr struct {
	Position        token.Position `json:"-"`
	Expr            Expression     `json:"expr"`
	Not             bool           `json:"not,omitempty"`
	CaseInsensitive bool           `json:"case_insensitive,omitempty"` // true for ILIKE
	Pattern         Expression     `json:"pattern"`
	Alias           string         `json:"alias,omitempty"`
}

func (l *LikeExpr) Pos() token.Position { return l.Position }
func (l *LikeExpr) End() token.Position { return l.Position }
func (l *LikeExpr) expressionNode()     {}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Position token.Position `json:"-"`
	Query    Statement      `json:"query"`
}

func (e *ExistsExpr) Pos() token.Position { return e.Position }
func (e *ExistsExpr) End() token.Position { return e.Position }
func (e *ExistsExpr) expressionNode()     {}
