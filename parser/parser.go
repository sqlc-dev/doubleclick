// Package parser implements a parser for ClickHouse SQL.
package parser

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
	"github.com/kyleconroy/doubleclick/lexer"
	"github.com/kyleconroy/doubleclick/token"
)

// Parser parses ClickHouse SQL statements.
type Parser struct {
	lexer   *lexer.Lexer
	current lexer.Item
	peek    lexer.Item
	errors  []error
}

// New creates a new Parser from an io.Reader.
func New(r io.Reader) *Parser {
	p := &Parser{
		lexer: lexer.New(r),
	}
	// Read two tokens to initialize current and peek
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.current = p.peek
	for {
		p.peek = p.lexer.NextToken()
		// Skip comments and whitespace
		if p.peek.Token != token.COMMENT && p.peek.Token != token.WHITESPACE {
			break
		}
	}
}

func (p *Parser) currentIs(t token.Token) bool {
	return p.current.Token == t
}

func (p *Parser) peekIs(t token.Token) bool {
	return p.peek.Token == t
}

func (p *Parser) expect(t token.Token) bool {
	if p.currentIs(t) {
		p.nextToken()
		return true
	}
	p.errors = append(p.errors, fmt.Errorf("expected %s, got %s at line %d, column %d",
		t, p.current.Token, p.current.Pos.Line, p.current.Pos.Column))
	return false
}

func (p *Parser) expectPeek(t token.Token) bool {
	if p.peekIs(t) {
		p.nextToken()
		return true
	}
	p.errors = append(p.errors, fmt.Errorf("expected %s, got %s at line %d, column %d",
		t, p.peek.Token, p.peek.Pos.Line, p.peek.Pos.Column))
	return false
}

// Parse parses SQL statements from the input.
func Parse(ctx context.Context, r io.Reader) ([]ast.Statement, error) {
	p := New(r)
	return p.ParseStatements(ctx)
}

// ParseStatements parses multiple SQL statements.
func (p *Parser) ParseStatements(ctx context.Context) ([]ast.Statement, error) {
	var statements []ast.Statement

	for !p.currentIs(token.EOF) {
		select {
		case <-ctx.Done():
			return statements, ctx.Err()
		default:
		}

		stmt := p.parseStatement()
		if stmt != nil {
			statements = append(statements, stmt)
		}

		// Skip semicolons between statements
		for p.currentIs(token.SEMICOLON) {
			p.nextToken()
		}
	}

	if len(p.errors) > 0 {
		return statements, fmt.Errorf("parse errors: %v", p.errors)
	}
	return statements, nil
}

func (p *Parser) parseStatement() ast.Statement {
	switch p.current.Token {
	case token.SELECT:
		return p.parseSelectWithUnion()
	case token.WITH:
		return p.parseSelectWithUnion()
	case token.INSERT:
		return p.parseInsert()
	case token.CREATE:
		return p.parseCreate()
	case token.DROP:
		return p.parseDrop()
	case token.ALTER:
		return p.parseAlter()
	case token.TRUNCATE:
		return p.parseTruncate()
	case token.USE:
		return p.parseUse()
	case token.DESCRIBE:
		return p.parseDescribe()
	case token.SHOW:
		return p.parseShow()
	case token.EXPLAIN:
		return p.parseExplain()
	case token.SET:
		return p.parseSet()
	case token.OPTIMIZE:
		return p.parseOptimize()
	case token.SYSTEM:
		return p.parseSystem()
	default:
		p.errors = append(p.errors, fmt.Errorf("unexpected token %s at line %d, column %d",
			p.current.Token, p.current.Pos.Line, p.current.Pos.Column))
		p.nextToken()
		return nil
	}
}

// parseSelectWithUnion parses SELECT ... UNION ... queries
func (p *Parser) parseSelectWithUnion() *ast.SelectWithUnionQuery {
	query := &ast.SelectWithUnionQuery{
		Position: p.current.Pos,
	}

	// Parse first SELECT
	sel := p.parseSelect()
	if sel == nil {
		return nil
	}
	query.Selects = append(query.Selects, sel)

	// Parse UNION clauses
	for p.currentIs(token.UNION) {
		p.nextToken() // skip UNION
		if p.currentIs(token.ALL) {
			query.UnionAll = true
			p.nextToken()
		}
		sel := p.parseSelect()
		if sel == nil {
			break
		}
		query.Selects = append(query.Selects, sel)
	}

	return query
}

func (p *Parser) parseSelect() *ast.SelectQuery {
	sel := &ast.SelectQuery{
		Position: p.current.Pos,
	}

	// Handle WITH clause
	if p.currentIs(token.WITH) {
		p.nextToken()
		sel.With = p.parseWithClause()
	}

	if !p.expect(token.SELECT) {
		return nil
	}

	// Handle DISTINCT
	if p.currentIs(token.DISTINCT) {
		sel.Distinct = true
		p.nextToken()
	}

	// Handle TOP
	if p.currentIs(token.TOP) {
		p.nextToken()
		sel.Top = p.parseExpression(LOWEST)
	}

	// Parse column list
	sel.Columns = p.parseExpressionList()

	// Parse FROM clause
	if p.currentIs(token.FROM) {
		p.nextToken()
		sel.From = p.parseTablesInSelect()
	}

	// Parse PREWHERE clause
	if p.currentIs(token.PREWHERE) {
		p.nextToken()
		sel.PreWhere = p.parseExpression(LOWEST)
	}

	// Parse WHERE clause
	if p.currentIs(token.WHERE) {
		p.nextToken()
		sel.Where = p.parseExpression(LOWEST)
	}

	// Parse GROUP BY clause
	if p.currentIs(token.GROUP) {
		p.nextToken()
		if !p.expect(token.BY) {
			return nil
		}
		sel.GroupBy = p.parseExpressionList()

		// WITH ROLLUP
		if p.currentIs(token.WITH) && p.peekIs(token.ROLLUP) {
			p.nextToken()
			p.nextToken()
			sel.WithRollup = true
		}

		// WITH TOTALS
		if p.currentIs(token.WITH) && p.peekIs(token.TOTALS) {
			p.nextToken()
			p.nextToken()
			sel.WithTotals = true
		}
	}

	// Parse HAVING clause
	if p.currentIs(token.HAVING) {
		p.nextToken()
		sel.Having = p.parseExpression(LOWEST)
	}

	// Parse ORDER BY clause
	if p.currentIs(token.ORDER) {
		p.nextToken()
		if !p.expect(token.BY) {
			return nil
		}
		sel.OrderBy = p.parseOrderByList()
	}

	// Parse LIMIT clause
	if p.currentIs(token.LIMIT) {
		p.nextToken()
		sel.Limit = p.parseExpression(LOWEST)

		// LIMIT n, m syntax (offset, limit)
		if p.currentIs(token.COMMA) {
			p.nextToken()
			sel.Offset = sel.Limit
			sel.Limit = p.parseExpression(LOWEST)
		}
	}

	// Parse OFFSET clause
	if p.currentIs(token.OFFSET) {
		p.nextToken()
		sel.Offset = p.parseExpression(LOWEST)
	}

	// Parse SETTINGS clause
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		sel.Settings = p.parseSettingsList()
	}

	// Parse FORMAT clause
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			sel.Format = &ast.Identifier{
				Position: p.current.Pos,
				Parts:    []string{p.current.Value},
			}
			p.nextToken()
		}
	}

	return sel
}

func (p *Parser) parseWithClause() []ast.Expression {
	var elements []ast.Expression

	for {
		elem := &ast.WithElement{
			Position: p.current.Pos,
		}

		// Check if it's a subquery or expression
		if p.currentIs(token.LPAREN) {
			// Subquery
			p.nextToken()
			subquery := p.parseSelectWithUnion()
			if !p.expect(token.RPAREN) {
				return nil
			}
			elem.Query = &ast.Subquery{Query: subquery}
		} else {
			// Expression
			elem.Query = p.parseExpression(LOWEST)
		}

		if !p.expect(token.AS) {
			return nil
		}

		if p.currentIs(token.IDENT) {
			elem.Name = p.current.Value
			p.nextToken()
		}

		elements = append(elements, elem)

		if !p.currentIs(token.COMMA) {
			break
		}
		p.nextToken()
	}

	return elements
}

func (p *Parser) parseTablesInSelect() *ast.TablesInSelectQuery {
	tables := &ast.TablesInSelectQuery{
		Position: p.current.Pos,
	}

	// Parse first table
	elem := p.parseTableElement()
	if elem == nil {
		return nil
	}
	tables.Tables = append(tables.Tables, elem)

	// Parse JOINs
	for p.isJoinKeyword() {
		elem := p.parseTableElementWithJoin()
		if elem == nil {
			break
		}
		tables.Tables = append(tables.Tables, elem)
	}

	return tables
}

func (p *Parser) isJoinKeyword() bool {
	switch p.current.Token {
	case token.JOIN, token.INNER, token.LEFT, token.RIGHT, token.FULL, token.CROSS,
		token.GLOBAL, token.ANY, token.ALL, token.ASOF, token.SEMI, token.ANTI:
		return true
	case token.COMMA:
		return true
	}
	return false
}

func (p *Parser) parseTableElement() *ast.TablesInSelectQueryElement {
	elem := &ast.TablesInSelectQueryElement{
		Position: p.current.Pos,
	}

	elem.Table = p.parseTableExpression()
	return elem
}

func (p *Parser) parseTableElementWithJoin() *ast.TablesInSelectQueryElement {
	elem := &ast.TablesInSelectQueryElement{
		Position: p.current.Pos,
	}

	// Handle comma join (implicit cross join)
	if p.currentIs(token.COMMA) {
		p.nextToken()
		elem.Table = p.parseTableExpression()
		return elem
	}

	// Parse JOIN
	join := &ast.TableJoin{
		Position: p.current.Pos,
	}

	// Parse join modifiers
	if p.currentIs(token.GLOBAL) {
		join.Global = true
		p.nextToken()
	}

	// Parse strictness
	switch p.current.Token {
	case token.ANY:
		join.Strictness = ast.JoinStrictAny
		p.nextToken()
	case token.ALL:
		join.Strictness = ast.JoinStrictAll
		p.nextToken()
	case token.ASOF:
		join.Strictness = ast.JoinStrictAsof
		p.nextToken()
	case token.SEMI:
		join.Strictness = ast.JoinStrictSemi
		p.nextToken()
	case token.ANTI:
		join.Strictness = ast.JoinStrictAnti
		p.nextToken()
	}

	// Parse join type
	switch p.current.Token {
	case token.INNER:
		join.Type = ast.JoinInner
		p.nextToken()
	case token.LEFT:
		join.Type = ast.JoinLeft
		p.nextToken()
		if p.currentIs(token.OUTER) {
			p.nextToken()
		}
	case token.RIGHT:
		join.Type = ast.JoinRight
		p.nextToken()
		if p.currentIs(token.OUTER) {
			p.nextToken()
		}
	case token.FULL:
		join.Type = ast.JoinFull
		p.nextToken()
		if p.currentIs(token.OUTER) {
			p.nextToken()
		}
	case token.CROSS:
		join.Type = ast.JoinCross
		p.nextToken()
	default:
		join.Type = ast.JoinInner
	}

	if !p.expect(token.JOIN) {
		return nil
	}

	elem.Table = p.parseTableExpression()

	// Parse ON or USING clause
	if p.currentIs(token.ON) {
		p.nextToken()
		join.On = p.parseExpression(LOWEST)
	} else if p.currentIs(token.USING) {
		p.nextToken()
		if p.currentIs(token.LPAREN) {
			p.nextToken()
			join.Using = p.parseExpressionList()
			p.expect(token.RPAREN)
		} else {
			join.Using = p.parseExpressionList()
		}
	}

	elem.Join = join
	return elem
}

func (p *Parser) parseTableExpression() *ast.TableExpression {
	expr := &ast.TableExpression{
		Position: p.current.Pos,
	}

	// Handle subquery
	if p.currentIs(token.LPAREN) {
		p.nextToken()
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
			subquery := p.parseSelectWithUnion()
			expr.Table = &ast.Subquery{Query: subquery}
		} else {
			// Table function or expression
			expr.Table = p.parseExpression(LOWEST)
		}
		p.expect(token.RPAREN)
	} else if p.currentIs(token.IDENT) {
		// Table identifier or function
		ident := p.current.Value
		pos := p.current.Pos
		p.nextToken()

		if p.currentIs(token.LPAREN) {
			// Table function
			expr.Table = p.parseFunctionCall(ident, pos)
		} else if p.currentIs(token.DOT) {
			// database.table
			p.nextToken()
			tableName := ""
			if p.currentIs(token.IDENT) {
				tableName = p.current.Value
				p.nextToken()
			}
			expr.Table = &ast.TableIdentifier{
				Position: pos,
				Database: ident,
				Table:    tableName,
			}
		} else {
			expr.Table = &ast.TableIdentifier{
				Position: pos,
				Table:    ident,
			}
		}
	}

	// Handle FINAL
	if p.currentIs(token.FINAL) {
		expr.Final = true
		p.nextToken()
	}

	// Handle SAMPLE
	if p.currentIs(token.SAMPLE) {
		p.nextToken()
		expr.Sample = &ast.SampleClause{
			Position: p.current.Pos,
			Ratio:    p.parseExpression(LOWEST),
		}
		if p.currentIs(token.OFFSET) {
			p.nextToken()
			expr.Sample.Offset = p.parseExpression(LOWEST)
		}
	}

	// Handle alias
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			expr.Alias = p.current.Value
			p.nextToken()
		}
	} else if p.currentIs(token.IDENT) && !p.isKeywordForClause() {
		expr.Alias = p.current.Value
		p.nextToken()
	}

	return expr
}

func (p *Parser) isKeywordForClause() bool {
	switch p.current.Token {
	case token.WHERE, token.GROUP, token.HAVING, token.ORDER, token.LIMIT,
		token.OFFSET, token.UNION, token.EXCEPT, token.SETTINGS, token.FORMAT,
		token.PREWHERE, token.JOIN, token.LEFT, token.RIGHT, token.INNER,
		token.FULL, token.CROSS, token.ON, token.USING, token.GLOBAL,
		token.ANY, token.ALL, token.SEMI, token.ANTI, token.ASOF:
		return true
	}
	return false
}

func (p *Parser) parseOrderByList() []*ast.OrderByElement {
	var elements []*ast.OrderByElement

	for {
		elem := &ast.OrderByElement{
			Position:   p.current.Pos,
			Expression: p.parseExpression(LOWEST),
		}

		// Handle ASC/DESC
		if p.currentIs(token.ASC) {
			p.nextToken()
		} else if p.currentIs(token.DESC) {
			elem.Descending = true
			p.nextToken()
		}

		// Handle NULLS FIRST/LAST
		if p.currentIs(token.NULLS) {
			p.nextToken()
			if p.currentIs(token.FIRST) {
				t := true
				elem.NullsFirst = &t
				p.nextToken()
			} else {
				// NULLS LAST
				f := false
				elem.NullsFirst = &f
				p.nextToken()
			}
		}

		// Handle COLLATE
		if p.currentIs(token.COLLATE) {
			p.nextToken()
			if p.currentIs(token.STRING) || p.currentIs(token.IDENT) {
				elem.Collate = p.current.Value
				p.nextToken()
			}
		}

		elements = append(elements, elem)

		if !p.currentIs(token.COMMA) {
			break
		}
		p.nextToken()
	}

	return elements
}

func (p *Parser) parseSettingsList() []*ast.SettingExpr {
	var settings []*ast.SettingExpr

	for {
		if !p.currentIs(token.IDENT) {
			break
		}

		setting := &ast.SettingExpr{
			Position: p.current.Pos,
			Name:     p.current.Value,
		}
		p.nextToken()

		if !p.expect(token.EQ) {
			break
		}

		setting.Value = p.parseExpression(LOWEST)
		settings = append(settings, setting)

		if !p.currentIs(token.COMMA) {
			break
		}
		p.nextToken()
	}

	return settings
}

func (p *Parser) parseInsert() *ast.InsertQuery {
	ins := &ast.InsertQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip INSERT

	if !p.expect(token.INTO) {
		return nil
	}

	// Skip optional TABLE keyword
	if p.currentIs(token.TABLE) {
		p.nextToken()
	}

	// Parse table name
	if p.currentIs(token.IDENT) {
		tableName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			ins.Database = tableName
			if p.currentIs(token.IDENT) {
				ins.Table = p.current.Value
				p.nextToken()
			}
		} else {
			ins.Table = tableName
		}
	}

	// Parse column list
	if p.currentIs(token.LPAREN) {
		p.nextToken()
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			if p.currentIs(token.IDENT) {
				ins.Columns = append(ins.Columns, &ast.Identifier{
					Position: p.current.Pos,
					Parts:    []string{p.current.Value},
				})
				p.nextToken()
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
		p.expect(token.RPAREN)
	}

	// Parse VALUES or SELECT
	if p.currentIs(token.VALUES) {
		p.nextToken()
		// VALUES are typically provided externally, skip for now
	} else if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
		ins.Select = p.parseSelectWithUnion()
	}

	// Parse FORMAT
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			ins.Format = &ast.Identifier{
				Position: p.current.Pos,
				Parts:    []string{p.current.Value},
			}
			p.nextToken()
		}
	}

	return ins
}

func (p *Parser) parseCreate() *ast.CreateQuery {
	create := &ast.CreateQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip CREATE

	// Handle OR REPLACE
	if p.currentIs(token.OR) {
		p.nextToken()
		if p.currentIs(token.REPLACE) {
			create.OrReplace = true
			p.nextToken()
		}
	}

	// Handle TEMPORARY
	if p.currentIs(token.TEMPORARY) {
		create.Temporary = true
		p.nextToken()
	}

	// Handle MATERIALIZED
	if p.currentIs(token.MATERIALIZED) {
		create.Materialized = true
		p.nextToken()
	}

	// What are we creating?
	switch p.current.Token {
	case token.TABLE:
		p.nextToken()
		p.parseCreateTable(create)
	case token.DATABASE:
		create.CreateDatabase = true
		p.nextToken()
		p.parseCreateDatabase(create)
	case token.VIEW:
		p.nextToken()
		p.parseCreateView(create)
	default:
		p.errors = append(p.errors, fmt.Errorf("expected TABLE, DATABASE, or VIEW after CREATE"))
		return nil
	}

	return create
}

func (p *Parser) parseCreateTable(create *ast.CreateQuery) {
	// Handle IF NOT EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.NOT) {
			p.nextToken()
			if p.currentIs(token.EXISTS) {
				create.IfNotExists = true
				p.nextToken()
			}
		}
	}

	// Parse table name
	if p.currentIs(token.IDENT) {
		tableName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			create.Database = tableName
			if p.currentIs(token.IDENT) {
				create.Table = p.current.Value
				p.nextToken()
			}
		} else {
			create.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				create.OnCluster = p.current.Value
				p.nextToken()
			}
		}
	}

	// Parse column definitions
	if p.currentIs(token.LPAREN) {
		p.nextToken()
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			col := p.parseColumnDeclaration()
			if col != nil {
				create.Columns = append(create.Columns, col)
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
		p.expect(token.RPAREN)
	}

	// Parse ENGINE
	if p.currentIs(token.ENGINE) {
		p.nextToken()
		if p.currentIs(token.EQ) {
			p.nextToken()
		}
		create.Engine = p.parseEngineClause()
	}

	// Parse ORDER BY
	if p.currentIs(token.ORDER) {
		p.nextToken()
		if p.expect(token.BY) {
			if p.currentIs(token.LPAREN) {
				p.nextToken()
				create.OrderBy = p.parseExpressionList()
				p.expect(token.RPAREN)
			} else {
				create.OrderBy = []ast.Expression{p.parseExpression(LOWEST)}
			}
		}
	}

	// Parse PARTITION BY
	if p.currentIs(token.PARTITION) {
		p.nextToken()
		if p.expect(token.BY) {
			create.PartitionBy = p.parseExpression(LOWEST)
		}
	}

	// Parse PRIMARY KEY
	if p.currentIs(token.PRIMARY) {
		p.nextToken()
		if p.expect(token.KEY) {
			if p.currentIs(token.LPAREN) {
				p.nextToken()
				create.PrimaryKey = p.parseExpressionList()
				p.expect(token.RPAREN)
			} else {
				create.PrimaryKey = []ast.Expression{p.parseExpression(LOWEST)}
			}
		}
	}

	// Parse SAMPLE BY
	if p.currentIs(token.SAMPLE) {
		p.nextToken()
		if p.expect(token.BY) {
			create.SampleBy = p.parseExpression(LOWEST)
		}
	}

	// Parse TTL
	if p.currentIs(token.TTL) {
		p.nextToken()
		create.TTL = &ast.TTLClause{
			Position:   p.current.Pos,
			Expression: p.parseExpression(LOWEST),
		}
	}

	// Parse SETTINGS
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		create.Settings = p.parseSettingsList()
	}

	// Parse AS SELECT
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
			create.AsSelect = p.parseSelectWithUnion()
		}
	}
}

func (p *Parser) parseCreateDatabase(create *ast.CreateQuery) {
	// Handle IF NOT EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.NOT) {
			p.nextToken()
			if p.currentIs(token.EXISTS) {
				create.IfNotExists = true
				p.nextToken()
			}
		}
	}

	// Parse database name
	if p.currentIs(token.IDENT) {
		create.Database = p.current.Value
		p.nextToken()
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				create.OnCluster = p.current.Value
				p.nextToken()
			}
		}
	}

	// Parse ENGINE
	if p.currentIs(token.ENGINE) {
		p.nextToken()
		if p.currentIs(token.EQ) {
			p.nextToken()
		}
		create.Engine = p.parseEngineClause()
	}
}

func (p *Parser) parseCreateView(create *ast.CreateQuery) {
	// Handle IF NOT EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.NOT) {
			p.nextToken()
			if p.currentIs(token.EXISTS) {
				create.IfNotExists = true
				p.nextToken()
			}
		}
	}

	// Parse view name
	if p.currentIs(token.IDENT) {
		viewName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			create.Database = viewName
			if p.currentIs(token.IDENT) {
				create.View = p.current.Value
				p.nextToken()
			}
		} else {
			create.View = viewName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				create.OnCluster = p.current.Value
				p.nextToken()
			}
		}
	}

	// Parse AS SELECT
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
			create.AsSelect = p.parseSelectWithUnion()
		}
	}
}

func (p *Parser) parseColumnDeclaration() *ast.ColumnDeclaration {
	col := &ast.ColumnDeclaration{
		Position: p.current.Pos,
	}

	// Parse column name
	if p.currentIs(token.IDENT) {
		col.Name = p.current.Value
		p.nextToken()
	} else {
		return nil
	}

	// Parse data type
	col.Type = p.parseDataType()

	// Parse DEFAULT/MATERIALIZED/ALIAS
	switch p.current.Token {
	case token.DEFAULT:
		col.DefaultKind = "DEFAULT"
		p.nextToken()
		col.Default = p.parseExpression(LOWEST)
	case token.MATERIALIZED:
		col.DefaultKind = "MATERIALIZED"
		p.nextToken()
		col.Default = p.parseExpression(LOWEST)
	case token.ALIAS:
		col.DefaultKind = "ALIAS"
		p.nextToken()
		col.Default = p.parseExpression(LOWEST)
	}

	// Parse CODEC
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "CODEC" {
		p.nextToken()
		col.Codec = p.parseCodecExpr()
	}

	// Parse TTL
	if p.currentIs(token.TTL) {
		p.nextToken()
		col.TTL = p.parseExpression(LOWEST)
	}

	return col
}

func (p *Parser) parseDataType() *ast.DataType {
	if !p.currentIs(token.IDENT) {
		return nil
	}

	dt := &ast.DataType{
		Position: p.current.Pos,
		Name:     p.current.Value,
	}
	p.nextToken()

	// Parse type parameters
	if p.currentIs(token.LPAREN) {
		p.nextToken()
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			// Could be another data type or an expression
			if p.currentIs(token.IDENT) && p.isDataTypeName(p.current.Value) {
				dt.Parameters = append(dt.Parameters, p.parseDataType())
			} else {
				dt.Parameters = append(dt.Parameters, p.parseExpression(LOWEST))
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
		p.expect(token.RPAREN)
	}

	return dt
}

func (p *Parser) isDataTypeName(name string) bool {
	upper := strings.ToUpper(name)
	types := []string{
		"INT8", "INT16", "INT32", "INT64", "INT128", "INT256",
		"UINT8", "UINT16", "UINT32", "UINT64", "UINT128", "UINT256",
		"FLOAT32", "FLOAT64",
		"DECIMAL", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256",
		"STRING", "FIXEDSTRING",
		"UUID", "DATE", "DATE32", "DATETIME", "DATETIME64",
		"ENUM", "ENUM8", "ENUM16",
		"ARRAY", "TUPLE", "MAP", "NESTED",
		"NULLABLE", "LOWCARDINALITY",
		"BOOL", "BOOLEAN",
		"IPV4", "IPV6",
		"NOTHING", "INTERVAL",
	}
	for _, t := range types {
		if upper == t {
			return true
		}
	}
	return false
}

func (p *Parser) parseCodecExpr() *ast.CodecExpr {
	codec := &ast.CodecExpr{
		Position: p.current.Pos,
	}

	if !p.expect(token.LPAREN) {
		return nil
	}

	for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
		if p.currentIs(token.IDENT) {
			name := p.current.Value
			pos := p.current.Pos
			p.nextToken()

			fn := &ast.FunctionCall{
				Position: pos,
				Name:     name,
			}

			if p.currentIs(token.LPAREN) {
				p.nextToken()
				if !p.currentIs(token.RPAREN) {
					fn.Arguments = p.parseExpressionList()
				}
				p.expect(token.RPAREN)
			}

			codec.Codecs = append(codec.Codecs, fn)
		}

		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	p.expect(token.RPAREN)
	return codec
}

func (p *Parser) parseEngineClause() *ast.EngineClause {
	engine := &ast.EngineClause{
		Position: p.current.Pos,
	}

	if p.currentIs(token.IDENT) {
		engine.Name = p.current.Value
		p.nextToken()
	}

	if p.currentIs(token.LPAREN) {
		p.nextToken()
		if !p.currentIs(token.RPAREN) {
			engine.Parameters = p.parseExpressionList()
		}
		p.expect(token.RPAREN)
	}

	return engine
}

func (p *Parser) parseDrop() *ast.DropQuery {
	drop := &ast.DropQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DROP

	// Handle TEMPORARY
	if p.currentIs(token.TEMPORARY) {
		drop.Temporary = true
		p.nextToken()
	}

	// What are we dropping?
	switch p.current.Token {
	case token.TABLE:
		p.nextToken()
	case token.DATABASE:
		drop.DropDatabase = true
		p.nextToken()
	case token.VIEW:
		p.nextToken()
	default:
		p.nextToken() // skip unknown token
	}

	// Handle IF EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.EXISTS) {
			drop.IfExists = true
			p.nextToken()
		}
	}

	// Parse name
	if p.currentIs(token.IDENT) {
		name := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			drop.Database = name
			if p.currentIs(token.IDENT) {
				if drop.DropDatabase {
					drop.Database = p.current.Value
				} else {
					drop.Table = p.current.Value
				}
				p.nextToken()
			}
		} else {
			if drop.DropDatabase {
				drop.Database = name
			} else {
				drop.Table = name
			}
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				drop.OnCluster = p.current.Value
				p.nextToken()
			}
		}
	}

	return drop
}

func (p *Parser) parseAlter() *ast.AlterQuery {
	alter := &ast.AlterQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip ALTER

	if !p.expect(token.TABLE) {
		return nil
	}

	// Parse table name
	if p.currentIs(token.IDENT) {
		tableName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			alter.Database = tableName
			if p.currentIs(token.IDENT) {
				alter.Table = p.current.Value
				p.nextToken()
			}
		} else {
			alter.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				alter.OnCluster = p.current.Value
				p.nextToken()
			}
		}
	}

	// Parse commands
	for {
		cmd := p.parseAlterCommand()
		if cmd == nil {
			break
		}
		alter.Commands = append(alter.Commands, cmd)

		if !p.currentIs(token.COMMA) {
			break
		}
		p.nextToken()
	}

	return alter
}

func (p *Parser) parseAlterCommand() *ast.AlterCommand {
	cmd := &ast.AlterCommand{
		Position: p.current.Pos,
	}

	switch p.current.Token {
	case token.ADD:
		p.nextToken()
		if p.currentIs(token.COLUMN) {
			cmd.Type = ast.AlterAddColumn
			p.nextToken()
			cmd.Column = p.parseColumnDeclaration()
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "AFTER" {
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.AfterColumn = p.current.Value
					p.nextToken()
				}
			}
		} else if p.currentIs(token.INDEX) {
			cmd.Type = ast.AlterAddIndex
			p.nextToken()
			// Parse index definition
		} else if p.currentIs(token.CONSTRAINT) {
			cmd.Type = ast.AlterAddConstraint
			p.nextToken()
			// Parse constraint
		}
	case token.DROP:
		p.nextToken()
		if p.currentIs(token.COLUMN) {
			cmd.Type = ast.AlterDropColumn
			p.nextToken()
			if p.currentIs(token.IF) {
				p.nextToken()
				p.expect(token.EXISTS)
			}
			if p.currentIs(token.IDENT) {
				cmd.ColumnName = p.current.Value
				p.nextToken()
			}
		} else if p.currentIs(token.INDEX) {
			cmd.Type = ast.AlterDropIndex
			p.nextToken()
			if p.currentIs(token.IDENT) {
				cmd.Index = p.current.Value
				p.nextToken()
			}
		} else if p.currentIs(token.CONSTRAINT) {
			cmd.Type = ast.AlterDropConstraint
			p.nextToken()
		} else if p.currentIs(token.PARTITION) {
			cmd.Type = ast.AlterDropPartition
			p.nextToken()
			cmd.Partition = p.parseExpression(LOWEST)
		}
	case token.MODIFY:
		p.nextToken()
		if p.currentIs(token.COLUMN) {
			cmd.Type = ast.AlterModifyColumn
			p.nextToken()
			cmd.Column = p.parseColumnDeclaration()
		} else if p.currentIs(token.TTL) {
			cmd.Type = ast.AlterModifyTTL
			p.nextToken()
			cmd.TTL = &ast.TTLClause{
				Position:   p.current.Pos,
				Expression: p.parseExpression(LOWEST),
			}
		} else if p.currentIs(token.SETTINGS) {
			cmd.Type = ast.AlterModifySetting
			p.nextToken()
			cmd.Settings = p.parseSettingsList()
		}
	case token.RENAME:
		p.nextToken()
		if p.currentIs(token.COLUMN) {
			cmd.Type = ast.AlterRenameColumn
			p.nextToken()
			if p.currentIs(token.IDENT) {
				cmd.ColumnName = p.current.Value
				p.nextToken()
			}
			if p.currentIs(token.TO) {
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.NewName = p.current.Value
					p.nextToken()
				}
			}
		}
	case token.DETACH:
		p.nextToken()
		if p.currentIs(token.PARTITION) {
			cmd.Type = ast.AlterDetachPartition
			p.nextToken()
			cmd.Partition = p.parseExpression(LOWEST)
		}
	case token.ATTACH:
		p.nextToken()
		if p.currentIs(token.PARTITION) {
			cmd.Type = ast.AlterAttachPartition
			p.nextToken()
			cmd.Partition = p.parseExpression(LOWEST)
		}
	default:
		return nil
	}

	return cmd
}

func (p *Parser) parseTruncate() *ast.TruncateQuery {
	trunc := &ast.TruncateQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip TRUNCATE

	if p.currentIs(token.TABLE) {
		p.nextToken()
	}

	// Handle IF EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.EXISTS) {
			trunc.IfExists = true
			p.nextToken()
		}
	}

	// Parse table name
	if p.currentIs(token.IDENT) {
		tableName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			trunc.Database = tableName
			if p.currentIs(token.IDENT) {
				trunc.Table = p.current.Value
				p.nextToken()
			}
		} else {
			trunc.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				trunc.OnCluster = p.current.Value
				p.nextToken()
			}
		}
	}

	return trunc
}

func (p *Parser) parseUse() *ast.UseQuery {
	use := &ast.UseQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip USE

	if p.currentIs(token.IDENT) {
		use.Database = p.current.Value
		p.nextToken()
	}

	return use
}

func (p *Parser) parseDescribe() *ast.DescribeQuery {
	desc := &ast.DescribeQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DESCRIBE

	if p.currentIs(token.TABLE) {
		p.nextToken()
	}

	// Parse table name
	if p.currentIs(token.IDENT) {
		tableName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			desc.Database = tableName
			if p.currentIs(token.IDENT) {
				desc.Table = p.current.Value
				p.nextToken()
			}
		} else {
			desc.Table = tableName
		}
	}

	return desc
}

func (p *Parser) parseShow() *ast.ShowQuery {
	show := &ast.ShowQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip SHOW

	switch p.current.Token {
	case token.TABLES:
		show.ShowType = ast.ShowTables
		p.nextToken()
	case token.DATABASES:
		show.ShowType = ast.ShowDatabases
		p.nextToken()
	case token.CREATE:
		show.ShowType = ast.ShowCreate
		p.nextToken()
		if p.currentIs(token.TABLE) {
			p.nextToken()
		}
	default:
		// Handle SHOW PROCESSLIST etc.
		if p.currentIs(token.IDENT) {
			if strings.ToUpper(p.current.Value) == "PROCESSLIST" {
				show.ShowType = ast.ShowProcesses
			}
			p.nextToken()
		}
	}

	// Parse FROM clause
	if p.currentIs(token.FROM) {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			show.From = p.current.Value
			p.nextToken()
		}
	}

	// Parse LIKE clause
	if p.currentIs(token.LIKE) {
		p.nextToken()
		if p.currentIs(token.STRING) {
			show.Like = p.current.Value
			p.nextToken()
		}
	}

	// Parse WHERE clause
	if p.currentIs(token.WHERE) {
		p.nextToken()
		show.Where = p.parseExpression(LOWEST)
	}

	// Parse LIMIT clause
	if p.currentIs(token.LIMIT) {
		p.nextToken()
		show.Limit = p.parseExpression(LOWEST)
	}

	return show
}

func (p *Parser) parseExplain() *ast.ExplainQuery {
	explain := &ast.ExplainQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip EXPLAIN

	// Parse explain type
	if p.currentIs(token.IDENT) {
		switch strings.ToUpper(p.current.Value) {
		case "AST":
			explain.ExplainType = ast.ExplainAST
			p.nextToken()
		case "SYNTAX":
			explain.ExplainType = ast.ExplainSyntax
			p.nextToken()
		case "PLAN":
			explain.ExplainType = ast.ExplainPlan
			p.nextToken()
		case "PIPELINE":
			explain.ExplainType = ast.ExplainPipeline
			p.nextToken()
		case "ESTIMATE":
			explain.ExplainType = ast.ExplainEstimate
			p.nextToken()
		default:
			explain.ExplainType = ast.ExplainPlan
		}
	}

	// Parse the statement being explained
	explain.Statement = p.parseStatement()

	return explain
}

func (p *Parser) parseSet() *ast.SetQuery {
	set := &ast.SetQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip SET

	set.Settings = p.parseSettingsList()

	return set
}

func (p *Parser) parseOptimize() *ast.OptimizeQuery {
	opt := &ast.OptimizeQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip OPTIMIZE

	if !p.expect(token.TABLE) {
		return nil
	}

	// Parse table name
	if p.currentIs(token.IDENT) {
		tableName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			opt.Database = tableName
			if p.currentIs(token.IDENT) {
				opt.Table = p.current.Value
				p.nextToken()
			}
		} else {
			opt.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				opt.OnCluster = p.current.Value
				p.nextToken()
			}
		}
	}

	// Handle PARTITION
	if p.currentIs(token.PARTITION) {
		p.nextToken()
		opt.Partition = p.parseExpression(LOWEST)
	}

	// Handle FINAL
	if p.currentIs(token.FINAL) {
		opt.Final = true
		p.nextToken()
	}

	// Handle DEDUPLICATE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "DEDUPLICATE" {
		opt.Dedupe = true
		p.nextToken()
	}

	return opt
}

func (p *Parser) parseSystem() *ast.SystemQuery {
	sys := &ast.SystemQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip SYSTEM

	// Read the command
	var parts []string
	for p.currentIs(token.IDENT) {
		parts = append(parts, p.current.Value)
		p.nextToken()
	}
	sys.Command = strings.Join(parts, " ")

	return sys
}
