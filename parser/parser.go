// Package parser implements a parser for ClickHouse SQL.
package parser

import (
	"context"
	"fmt"
	"io"
	"strconv"
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
	case token.DESCRIBE, token.DESC:
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
	case token.RENAME:
		return p.parseRename()
	case token.EXCHANGE:
		return p.parseExchange()
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
		var mode string
		if p.currentIs(token.ALL) {
			query.UnionAll = true
			mode = "ALL"
			p.nextToken()
		} else if p.currentIs(token.DISTINCT) {
			mode = "DISTINCT"
			p.nextToken()
		}
		query.UnionModes = append(query.UnionModes, mode)
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

	// Parse ARRAY JOIN clause
	if p.currentIs(token.ARRAY) || (p.currentIs(token.LEFT) && p.peekIs(token.ARRAY)) {
		sel.ArrayJoin = p.parseArrayJoin()
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

	// Parse WINDOW clause for named windows
	if p.currentIs(token.WINDOW) {
		p.nextToken()
		sel.Window = p.parseWindowDefinitions()
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

		// LIMIT BY clause (ClickHouse specific: LIMIT n BY expr1, expr2, ...)
		if p.currentIs(token.BY) {
			p.nextToken()
			// Parse LIMIT BY expressions - skip them for now
			for !p.isEndOfExpression() {
				p.parseExpression(LOWEST)
				if p.currentIs(token.COMMA) {
					p.nextToken()
				} else {
					break
				}
			}
		}

		// WITH TIES modifier
		if p.currentIs(token.WITH) && p.peekIs(token.TIES) {
			p.nextToken() // skip WITH
			p.nextToken() // skip TIES
		}
	}

	// Parse OFFSET clause
	if p.currentIs(token.OFFSET) {
		p.nextToken()
		sel.Offset = p.parseExpression(LOWEST)
	}

	// Parse FETCH FIRST ... ROW ONLY (SQL standard syntax)
	if p.currentIs(token.FETCH) {
		p.nextToken()
		// Skip FIRST or NEXT
		if p.currentIs(token.FIRST) || (p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "NEXT") {
			p.nextToken()
		}
		// Parse the limit count
		if !p.currentIs(token.IDENT) || strings.ToUpper(p.current.Value) != "ROW" {
			sel.Limit = p.parseExpression(LOWEST)
		}
		// Skip ROW/ROWS
		if p.currentIs(token.IDENT) && (strings.ToUpper(p.current.Value) == "ROW" || strings.ToUpper(p.current.Value) == "ROWS") {
			p.nextToken()
		}
		// Skip ONLY
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ONLY" {
			p.nextToken()
		}
		// Skip WITH TIES
		if p.currentIs(token.WITH) {
			p.nextToken()
			if p.currentIs(token.TIES) {
				p.nextToken()
			}
		}
	}

	// Parse SETTINGS clause
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		sel.Settings = p.parseSettingsList()
	}

	// Parse INTO OUTFILE clause
	if p.currentIs(token.INTO) {
		p.nextToken()
		if p.currentIs(token.OUTFILE) {
			p.nextToken()
			if p.currentIs(token.STRING) {
				sel.IntoOutfile = &ast.IntoOutfileClause{
					Position: p.current.Pos,
					Filename: p.current.Value,
				}
				p.nextToken()
			}
		}
	}

	// Parse FORMAT clause (format names can be keywords like Null, JSON, etc.)
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.currentIs(token.NULL) || p.current.Token.IsKeyword() {
			sel.Format = &ast.Identifier{
				Position: p.current.Pos,
				Parts:    []string{p.current.Value},
			}
			p.nextToken()
		}
	}

	// Parse SETTINGS clause (can come after FORMAT)
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		sel.Settings = p.parseSettingsList()
	}

	return sel
}

func (p *Parser) parseWithClause() []ast.Expression {
	var elements []ast.Expression

	for {
		elem := &ast.WithElement{
			Position: p.current.Pos,
		}

		// Check if it's the "name AS (subquery)" syntax (standard SQL CTE)
		// or "expr AS name" syntax (ClickHouse scalar)
		if p.currentIs(token.IDENT) && p.peekIs(token.AS) {
			// This could be "name AS (subquery)" or "ident AS alias" for scalar
			name := p.current.Value
			p.nextToken() // skip identifier
			p.nextToken() // skip AS

			if p.currentIs(token.LPAREN) {
				// Standard CTE: name AS (subquery)
				p.nextToken()
				if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
					subquery := p.parseSelectWithUnion()
					if !p.expect(token.RPAREN) {
						return nil
					}
					elem.Name = name
					elem.Query = &ast.Subquery{Query: subquery}
				} else {
					// It's an expression in parentheses, parse it and use name as alias
					expr := p.parseExpression(LOWEST)
					p.expect(token.RPAREN)
					elem.Name = name
					elem.Query = expr
				}
			} else {
				// Scalar expression where the first identifier is used directly
				// This is likely "name AS name" which means the CTE name is name with scalar value name
				elem.Name = name
				elem.Query = &ast.Identifier{Position: elem.Position, Parts: []string{name}}
			}
		} else if p.currentIs(token.LPAREN) {
			// Subquery: (SELECT ...) AS name
			p.nextToken()
			subquery := p.parseSelectWithUnion()
			if !p.expect(token.RPAREN) {
				return nil
			}
			elem.Query = &ast.Subquery{Query: subquery}

			if !p.expect(token.AS) {
				return nil
			}

			if p.currentIs(token.IDENT) {
				elem.Name = p.current.Value
				p.nextToken()
			}
		} else {
			// Scalar WITH: expr AS name (ClickHouse style)
			// Examples: WITH 1 AS x, WITH 'hello' AS s, WITH func() AS f
			elem.Query = p.parseExpression(ALIAS_PREC) // Use ALIAS_PREC to stop before AS

			if !p.expect(token.AS) {
				return nil
			}

			if p.currentIs(token.IDENT) {
				elem.Name = p.current.Value
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
	// LEFT ARRAY JOIN is handled by parseArrayJoin, not as a regular join
	if p.currentIs(token.LEFT) && p.peekIs(token.ARRAY) {
		return false
	}
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
	} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() || p.currentIs(token.NUMBER) {
		// Table identifier or function (keywords can be table names like "system")
		// Table names can also start with numbers in ClickHouse
		pos := p.current.Pos
		ident := p.parseIdentifierName()

		if p.currentIs(token.LPAREN) {
			// Table function
			expr.Table = p.parseFunctionCall(ident, pos)
		} else if p.currentIs(token.DOT) {
			// database.table
			p.nextToken()
			tableName := p.parseIdentifierName()
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

	// Handle alias (keywords like LEFT, RIGHT can be used as aliases after AS)
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
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

func (p *Parser) isEndOfExpression() bool {
	switch p.current.Token {
	case token.EOF, token.RPAREN, token.RBRACKET, token.SEMICOLON,
		token.UNION, token.EXCEPT, token.ORDER, token.LIMIT,
		token.OFFSET, token.SETTINGS, token.FORMAT, token.INTO,
		token.WITH:
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
			} else if p.currentIs(token.LAST) {
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

		// Handle WITH FILL
		if p.currentIs(token.WITH) && p.peekIs(token.FILL) {
			p.nextToken() // skip WITH
			p.nextToken() // skip FILL
			elem.WithFill = true

			// Handle FROM
			if p.currentIs(token.FROM) {
				p.nextToken()
				elem.FillFrom = p.parseExpression(LOWEST)
			}

			// Handle TO
			if p.currentIs(token.TO) {
				p.nextToken()
				elem.FillTo = p.parseExpression(LOWEST)
			}

			// Handle STEP
			if p.currentIs(token.STEP) {
				p.nextToken()
				elem.FillStep = p.parseExpression(LOWEST)
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

		// Settings can have optional value (bool settings can be just name)
		if p.currentIs(token.EQ) {
			p.nextToken()
			setting.Value = p.parseExpression(LOWEST)
		} else {
			// Boolean setting without value - defaults to true
			setting.Value = &ast.Literal{
				Position: setting.Position,
				Type:     ast.LiteralBoolean,
				Value:    true,
			}
		}
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

	// Handle INSERT INTO FUNCTION
	if p.currentIs(token.FUNCTION) {
		p.nextToken()
		// Parse the function call
		funcName := p.parseIdentifierName()
		if funcName != "" && p.currentIs(token.LPAREN) {
			ins.Function = p.parseFunctionCall(funcName, p.current.Pos)
		}
	} else {
		// Parse table name (can start with a number in ClickHouse)
		tableName := p.parseIdentifierName()
		if tableName != "" {
			if p.currentIs(token.DOT) {
				p.nextToken()
				ins.Database = tableName
				ins.Table = p.parseIdentifierName()
			} else {
				ins.Table = tableName
			}
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

	// Parse SETTINGS before VALUES
	if p.currentIs(token.SETTINGS) {
		ins.HasSettings = true
		p.nextToken()
		// Just parse and skip the settings
		p.parseSettingsList()
	}

	// Parse VALUES or SELECT
	if p.currentIs(token.VALUES) {
		p.nextToken()
		// Skip VALUES data - consume until end of statement
		for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.FORMAT) && !p.currentIs(token.SETTINGS) {
			p.nextToken()
		}
	} else if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
		ins.Select = p.parseSelectWithUnion()
	}

	// Parse FORMAT (format names can be keywords like Null, JSON, etc.)
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.currentIs(token.NULL) || p.current.Token.IsKeyword() {
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

	// Parse table name (can start with a number in ClickHouse)
	tableName := p.parseIdentifierName()
	if tableName != "" {
		if p.currentIs(token.DOT) {
			p.nextToken()
			create.Database = tableName
			create.Table = p.parseIdentifierName()
		} else {
			create.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			create.OnCluster = p.parseIdentifierName()
		}
	}

	// Parse column definitions and indexes
	if p.currentIs(token.LPAREN) {
		p.nextToken()
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			// Handle INDEX definition
			if p.currentIs(token.INDEX) {
				p.nextToken()
				// Skip index definition: INDEX name expr TYPE type GRANULARITY n
				p.parseIdentifierName() // index name
				// Skip expression and other index parts
				for !p.currentIs(token.COMMA) && !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
					p.nextToken()
				}
			} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "CONSTRAINT" {
				// Skip CONSTRAINT definitions
				p.nextToken()
				p.parseIdentifierName() // constraint name
				for !p.currentIs(token.COMMA) && !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
					p.nextToken()
				}
			} else {
				col := p.parseColumnDeclaration()
				if col != nil {
					create.Columns = append(create.Columns, col)
				}
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

	// Parse table options in flexible order (PARTITION BY, ORDER BY, PRIMARY KEY, etc.)
	for {
		switch {
		case p.currentIs(token.PARTITION):
			p.nextToken()
			if p.expect(token.BY) {
				create.PartitionBy = p.parseExpression(LOWEST)
			}
		case p.currentIs(token.ORDER):
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
		case p.currentIs(token.PRIMARY):
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
		case p.currentIs(token.SAMPLE):
			p.nextToken()
			if p.expect(token.BY) {
				create.SampleBy = p.parseExpression(LOWEST)
			}
		case p.currentIs(token.TTL):
			p.nextToken()
			create.TTL = &ast.TTLClause{
				Position:   p.current.Pos,
				Expression: p.parseExpression(LOWEST),
			}
		case p.currentIs(token.SETTINGS):
			p.nextToken()
			create.Settings = p.parseSettingsList()
		default:
			goto done_table_options
		}
	}
done_table_options:

	// Parse AS SELECT or AS table_function()
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
			create.AsSelect = p.parseSelectWithUnion()
		} else if p.currentIs(token.IDENT) {
			// AS table_function(...) like "AS s3Cluster(...)"
			// Skip the function call for now
			p.parseIdentifierName()
			if p.currentIs(token.LPAREN) {
				depth := 1
				p.nextToken()
				for depth > 0 && !p.currentIs(token.EOF) {
					if p.currentIs(token.LPAREN) {
						depth++
					} else if p.currentIs(token.RPAREN) {
						depth--
					}
					p.nextToken()
				}
			}
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

	// Parse database name (can start with a number in ClickHouse)
	create.Database = p.parseIdentifierName()

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			create.OnCluster = p.parseIdentifierName()
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

	// Parse view name (can start with a number in ClickHouse)
	viewName := p.parseIdentifierName()
	if viewName != "" {
		if p.currentIs(token.DOT) {
			p.nextToken()
			create.Database = viewName
			create.View = p.parseIdentifierName()
		} else {
			create.View = viewName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			create.OnCluster = p.parseIdentifierName()
		}
	}

	// Handle TO (target table for materialized views)
	if p.currentIs(token.TO) {
		p.nextToken()
		create.To = p.parseIdentifierName()
	}

	// Parse ENGINE (for materialized views)
	if p.currentIs(token.ENGINE) {
		p.nextToken()
		if p.currentIs(token.EQ) {
			p.nextToken()
		}
		create.Engine = p.parseEngineClause()
	}

	// Parse POPULATE (for materialized views)
	if p.currentIs(token.POPULATE) {
		create.Populate = true
		p.nextToken()
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

	// Parse column name (can be identifier or keyword like KEY)
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		col.Name = p.current.Value
		p.nextToken()
	} else {
		return nil
	}

	// Parse data type
	col.Type = p.parseDataType()

	// Parse DEFAULT/MATERIALIZED/ALIAS/EPHEMERAL
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

	// Handle EPHEMERAL (can be EPHEMERAL or EPHEMERAL default_value)
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "EPHEMERAL" {
		col.DefaultKind = "EPHEMERAL"
		p.nextToken()
		// Optional default value
		if !p.currentIs(token.COMMA) && !p.currentIs(token.RPAREN) && !p.currentIs(token.IDENT) {
			col.Default = p.parseExpression(LOWEST)
		}
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

	// Parse COMMENT
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "COMMENT" {
		p.nextToken()
		if p.currentIs(token.STRING) {
			col.Comment = p.current.Value
			p.nextToken()
		}
	}

	return col
}

func (p *Parser) parseDataType() *ast.DataType {
	// Type names can be identifiers or keywords (Array, Nested, Key, etc.)
	if !p.currentIs(token.IDENT) && !p.current.Token.IsKeyword() {
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

		// Special handling for Nested type - it contains column declarations, not just types
		if strings.ToUpper(dt.Name) == "NESTED" {
			for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
				// Parse as column name + type
				if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					colName := p.current.Value
					p.nextToken()
					// Parse the type for this column
					colType := p.parseDataType()
					if colType != nil {
						// Wrap in a special format or just store as data type
						colType.Name = colName + " " + colType.Name
						dt.Parameters = append(dt.Parameters, colType)
					}
				}
				if p.currentIs(token.COMMA) {
					p.nextToken()
				} else {
					break
				}
			}
		} else {
			for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
				// Could be another data type or an expression
				// Type names can be identifiers or keywords (Array, Nested, etc.)
				if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.isDataTypeName(p.current.Value) {
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
		}
		p.expect(token.RPAREN)
	}

	return dt
}

func (p *Parser) isDataTypeName(name string) bool {
	upper := strings.ToUpper(name)
	types := []string{
		"INT", "INT8", "INT16", "INT32", "INT64", "INT128", "INT256",
		"UINT8", "UINT16", "UINT32", "UINT64", "UINT128", "UINT256",
		"FLOAT32", "FLOAT64", "FLOAT",
		"DECIMAL", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256",
		"STRING", "FIXEDSTRING",
		"UUID", "DATE", "DATE32", "DATETIME", "DATETIME64",
		"ENUM", "ENUM8", "ENUM16",
		"ARRAY", "TUPLE", "MAP", "NESTED",
		"NULLABLE", "LOWCARDINALITY",
		"BOOL", "BOOLEAN",
		"IPV4", "IPV6",
		"NOTHING", "INTERVAL",
		"JSON", "OBJECT", "VARIANT",
		"AGGREGATEFUNCTION", "SIMPLEAGGREGATEFUNCTION",
		"POINT", "RING", "POLYGON", "MULTIPOLYGON",
		"TIME64", "TIME",
		"DYNAMIC",
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

	// Engine name can be identifier or keyword (Null, Join, Memory, etc.)
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		engine.Name = p.current.Value
		p.nextToken()
	}

	if p.currentIs(token.LPAREN) {
		engine.HasParentheses = true
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
	dropUser := false
	switch p.current.Token {
	case token.TABLE:
		p.nextToken()
	case token.DATABASE:
		drop.DropDatabase = true
		p.nextToken()
	case token.VIEW:
		p.nextToken()
	case token.USER:
		dropUser = true
		p.nextToken()
	case token.FUNCTION:
		p.nextToken()
	case token.INDEX:
		p.nextToken()
	default:
		// Handle multi-word DROP types: ROW POLICY, NAMED COLLECTION, SETTINGS PROFILE
		if p.currentIs(token.IDENT) {
			upper := strings.ToUpper(p.current.Value)
			switch upper {
			case "ROW", "NAMED", "POLICY", "SETTINGS", "QUOTA", "ROLE":
				// Skip the DROP type tokens
				for p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					if p.currentIs(token.IF) {
						break // Hit IF EXISTS
					}
					p.nextToken()
				}
			default:
				p.nextToken() // skip unknown token
			}
		} else {
			p.nextToken() // skip unknown token
		}
	}

	// Handle IF EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.EXISTS) {
			drop.IfExists = true
			p.nextToken()
		}
	}

	// Parse name (can start with a number in ClickHouse)
	name := p.parseIdentifierName()
	if name != "" {
		if p.currentIs(token.DOT) {
			p.nextToken()
			drop.Database = name
			tableName := p.parseIdentifierName()
			if tableName != "" {
				if drop.DropDatabase {
					drop.Database = tableName
				} else {
					drop.Table = tableName
				}
			}
		} else {
			if dropUser {
				drop.User = name
			} else if drop.DropDatabase {
				drop.Database = name
			} else {
				drop.Table = name
			}
		}
	}

	// Handle multiple tables (DROP TABLE IF EXISTS t1, t2, t3)
	// For now, just skip additional table names
	for p.currentIs(token.COMMA) {
		p.nextToken()
		// Skip the table name (may be qualified like db.table)
		p.parseIdentifierName()
		if p.currentIs(token.DOT) {
			p.nextToken()
			p.parseIdentifierName()
		}
	}

	// Handle ON table or ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
				drop.OnCluster = p.current.Value
				p.nextToken()
			}
		} else {
			// ON table_name (for DROP ROW POLICY, etc.)
			// Skip the table reference
			p.parseIdentifierName()
			if p.currentIs(token.DOT) {
				p.nextToken()
				p.parseIdentifierName()
			}
		}
	}

	// Handle second ON CLUSTER (can appear after ON table)
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

	// Handle SYNC
	if p.currentIs(token.SYNC) {
		drop.Sync = true
		p.nextToken()
	}

	// Handle NO DELAY
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "NO" {
		p.nextToken()
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "DELAY" {
			p.nextToken()
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

	// Parse table name (can start with a number in ClickHouse)
	tableName := p.parseIdentifierName()
	if tableName != "" {
		if p.currentIs(token.DOT) {
			p.nextToken()
			alter.Database = tableName
			alter.Table = p.parseIdentifierName()
		} else {
			alter.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			alter.OnCluster = p.parseIdentifierName()
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
			// Handle IF NOT EXISTS
			if p.currentIs(token.IF) {
				p.nextToken()
				if p.currentIs(token.NOT) {
					p.nextToken()
					if p.currentIs(token.EXISTS) {
						cmd.IfNotExists = true
						p.nextToken()
					}
				}
			}
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
			// Parse index name
			if p.currentIs(token.IDENT) {
				cmd.Index = p.current.Value
				p.nextToken()
			}
			// Parse expression in parentheses
			if p.currentIs(token.LPAREN) {
				p.nextToken()
				cmd.IndexExpr = p.parseExpression(LOWEST)
				p.expect(token.RPAREN)
			}
			// Parse TYPE
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "TYPE" {
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.IndexType = p.current.Value
					p.nextToken()
				}
			}
			// Parse GRANULARITY
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "GRANULARITY" {
				p.nextToken()
				if p.currentIs(token.NUMBER) {
					granularity, _ := strconv.Atoi(p.current.Value)
					cmd.Granularity = granularity
					p.nextToken()
				}
			}
		} else if p.currentIs(token.CONSTRAINT) {
			cmd.Type = ast.AlterAddConstraint
			p.nextToken()
			// Parse constraint name
			if p.currentIs(token.IDENT) {
				cmd.ConstraintName = p.current.Value
				p.nextToken()
			}
			// Parse CHECK
			if p.currentIs(token.CHECK) {
				p.nextToken()
				cmd.Constraint = &ast.Constraint{
					Position:   p.current.Pos,
					Name:       cmd.ConstraintName,
					Expression: p.parseExpression(LOWEST),
				}
			}
		}
	case token.DROP:
		p.nextToken()
		if p.currentIs(token.COLUMN) {
			cmd.Type = ast.AlterDropColumn
			p.nextToken()
			if p.currentIs(token.IF) {
				p.nextToken()
				p.expect(token.EXISTS)
				cmd.IfExists = true
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
			if p.currentIs(token.IDENT) {
				cmd.ConstraintName = p.current.Value
				p.nextToken()
			}
		} else if p.currentIs(token.PARTITION) {
			cmd.Type = ast.AlterDropPartition
			p.nextToken()
			cmd.Partition = p.parseExpression(LOWEST)
		}
	case token.IDENT:
		// Handle CLEAR, MATERIALIZE
		upper := strings.ToUpper(p.current.Value)
		if upper == "CLEAR" {
			p.nextToken()
			if p.currentIs(token.INDEX) {
				cmd.Type = ast.AlterClearIndex
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.Index = p.current.Value
					p.nextToken()
				}
			} else if p.currentIs(token.COLUMN) {
				cmd.Type = ast.AlterClearColumn
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.ColumnName = p.current.Value
					p.nextToken()
				}
			}
		} else if upper == "MATERIALIZE" {
			p.nextToken()
			if p.currentIs(token.INDEX) {
				cmd.Type = ast.AlterMaterializeIndex
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.Index = p.current.Value
					p.nextToken()
				}
			}
		} else {
			return nil
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
	case token.FREEZE:
		p.nextToken()
		if p.currentIs(token.PARTITION) {
			cmd.Type = ast.AlterFreezePartition
			p.nextToken()
			cmd.Partition = p.parseExpression(LOWEST)
		} else {
			cmd.Type = ast.AlterFreeze
		}
	case token.REPLACE:
		p.nextToken()
		if p.currentIs(token.PARTITION) {
			cmd.Type = ast.AlterReplacePartition
			p.nextToken()
			cmd.Partition = p.parseExpression(LOWEST)
			if p.currentIs(token.FROM) {
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.FromTable = p.current.Value
					p.nextToken()
				}
			}
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

	// Parse table name (can start with a number in ClickHouse)
	tableName := p.parseIdentifierName()
	if tableName != "" {
		if p.currentIs(token.DOT) {
			p.nextToken()
			trunc.Database = tableName
			trunc.Table = p.parseIdentifierName()
		} else {
			trunc.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			trunc.OnCluster = p.parseIdentifierName()
		}
	}

	return trunc
}

func (p *Parser) parseUse() *ast.UseQuery {
	use := &ast.UseQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip USE

	// Database name can be an identifier or a keyword like DEFAULT (can also start with number)
	use.Database = p.parseIdentifierName()

	return use
}

func (p *Parser) parseDescribe() *ast.DescribeQuery {
	desc := &ast.DescribeQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DESCRIBE or DESC

	if p.currentIs(token.TABLE) {
		p.nextToken()
	}

	// Parse table name (can be identifier or keyword used as table name like "system")
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		tableName := p.current.Value
		p.nextToken()

		if p.currentIs(token.DOT) {
			p.nextToken()
			desc.Database = tableName
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
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
	case token.COLUMNS:
		show.ShowType = ast.ShowColumns
		p.nextToken()
	case token.CREATE:
		p.nextToken()
		if p.currentIs(token.DATABASE) {
			show.ShowType = ast.ShowCreateDB
			p.nextToken()
		} else {
			show.ShowType = ast.ShowCreate
			if p.currentIs(token.TABLE) {
				p.nextToken()
			}
		}
	default:
		// Handle SHOW PROCESSLIST, SHOW DICTIONARIES, SHOW FUNCTIONS, etc.
		if p.currentIs(token.IDENT) {
			upper := strings.ToUpper(p.current.Value)
			switch upper {
			case "PROCESSLIST":
				show.ShowType = ast.ShowProcesses
			case "DICTIONARIES":
				show.ShowType = ast.ShowDictionaries
			case "FUNCTIONS":
				show.ShowType = ast.ShowFunctions
			}
			p.nextToken()
		}
	}

	// Parse FROM clause (or table/database name for SHOW CREATE TABLE/DATABASE)
	if p.currentIs(token.FROM) || ((show.ShowType == ast.ShowCreate || show.ShowType == ast.ShowCreateDB) && (p.currentIs(token.IDENT) || p.current.Token.IsKeyword())) {
		if p.currentIs(token.FROM) {
			p.nextToken()
		}
		// Parse table name which can be database.table or just table
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			name := p.current.Value
			p.nextToken()
			if p.currentIs(token.DOT) {
				p.nextToken()
				show.Database = name
				if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					show.From = p.current.Value
					p.nextToken()
				}
			} else {
				show.From = name
			}
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

	// Parse table name (can start with a number in ClickHouse)
	tableName := p.parseIdentifierName()
	if tableName != "" {
		if p.currentIs(token.DOT) {
			p.nextToken()
			opt.Database = tableName
			opt.Table = p.parseIdentifierName()
		} else {
			opt.Table = tableName
		}
	}

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			opt.OnCluster = p.parseIdentifierName()
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

	// Read the command - can include identifiers and keywords (like TTL, SYNC, etc.)
	var parts []string
	for p.currentIs(token.IDENT) || p.isSystemCommandKeyword() {
		parts = append(parts, p.current.Value)
		p.nextToken()
	}
	sys.Command = strings.Join(parts, " ")

	// Parse optional table name for commands like SYNC REPLICA table
	// Table names can be keywords like "system" or dotted like "system.one"
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		tableName := p.current.Value
		p.nextToken()
		if p.currentIs(token.DOT) {
			p.nextToken()
			sys.Database = tableName
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				sys.Table = p.current.Value
				p.nextToken()
			}
		} else {
			sys.Table = tableName
		}
	}

	return sys
}

// isSystemCommandKeyword returns true if current token is a keyword that can be part of SYSTEM command
func (p *Parser) isSystemCommandKeyword() bool {
	switch p.current.Token {
	case token.TTL, token.SYNC, token.DROP:
		return true
	}
	return false
}

func (p *Parser) parseRename() *ast.RenameQuery {
	rename := &ast.RenameQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip RENAME

	if !p.expect(token.TABLE) {
		return nil
	}

	// Parse from table name (can start with a number in ClickHouse)
	rename.From = p.parseIdentifierName()

	if !p.expect(token.TO) {
		return nil
	}

	// Parse to table name (can start with a number in ClickHouse)
	rename.To = p.parseIdentifierName()

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			rename.OnCluster = p.parseIdentifierName()
		}
	}

	return rename
}

func (p *Parser) parseExchange() *ast.ExchangeQuery {
	exchange := &ast.ExchangeQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip EXCHANGE

	if !p.expect(token.TABLES) {
		return nil
	}

	// Parse first table name (can start with a number in ClickHouse)
	exchange.Table1 = p.parseIdentifierName()

	if !p.expect(token.AND) {
		return nil
	}

	// Parse second table name (can start with a number in ClickHouse)
	exchange.Table2 = p.parseIdentifierName()

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			exchange.OnCluster = p.parseIdentifierName()
		}
	}

	return exchange
}

func (p *Parser) parseArrayJoin() *ast.ArrayJoinClause {
	aj := &ast.ArrayJoinClause{
		Position: p.current.Pos,
	}

	// Check for LEFT ARRAY JOIN
	if p.currentIs(token.LEFT) {
		aj.Left = true
		p.nextToken()
	}

	if !p.expect(token.ARRAY) {
		return nil
	}

	if !p.expect(token.JOIN) {
		return nil
	}

	// Parse array expressions
	aj.Columns = p.parseExpressionList()

	return aj
}

func (p *Parser) parseWindowDefinitions() []*ast.WindowDefinition {
	var defs []*ast.WindowDefinition

	for {
		def := &ast.WindowDefinition{
			Position: p.current.Pos,
		}

		// Parse window name
		if p.currentIs(token.IDENT) {
			def.Name = p.current.Value
			p.nextToken()
		}

		if !p.expect(token.AS) {
			break
		}

		if !p.expect(token.LPAREN) {
			break
		}

		// Parse window specification
		spec := &ast.WindowSpec{
			Position: p.current.Pos,
		}

		// Parse PARTITION BY
		if p.currentIs(token.PARTITION) {
			p.nextToken()
			if p.expect(token.BY) {
				spec.PartitionBy = p.parseExpressionList()
			}
		}

		// Parse ORDER BY
		if p.currentIs(token.ORDER) {
			p.nextToken()
			if p.expect(token.BY) {
				spec.OrderBy = p.parseOrderByList()
			}
		}

		p.expect(token.RPAREN)
		def.Spec = spec
		defs = append(defs, def)

		if !p.currentIs(token.COMMA) {
			break
		}
		p.nextToken()
	}

	return defs
}

// parseIdentifierName parses an identifier name that may start with a number.
// In ClickHouse, table and database names can start with digits (e.g., 03657_test).
// When such names are lexed, they produce NUMBER + IDENT tokens that need to be combined.
func (p *Parser) parseIdentifierName() string {
	var name string

	// Handle identifier or keyword used as name
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		name = p.current.Value
		p.nextToken()
		return name
	}

	// Handle name starting with number (e.g., 03657_test)
	if p.currentIs(token.NUMBER) {
		name = p.current.Value
		p.nextToken()
		// Check if followed by identifier (underscore connects them)
		if p.currentIs(token.IDENT) {
			name += p.current.Value
			p.nextToken()
		}
		return name
	}

	// Handle string (e.g., for cluster names)
	if p.currentIs(token.STRING) {
		name = p.current.Value
		p.nextToken()
		return name
	}

	return ""
}
