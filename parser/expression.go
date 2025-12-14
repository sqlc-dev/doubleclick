package parser

import (
	"math"
	"strconv"
	"strings"

	"github.com/kyleconroy/doubleclick/ast"
	"github.com/kyleconroy/doubleclick/token"
)

// Operator precedence levels
const (
	LOWEST      = iota
	ALIAS_PREC  // AS
	OR_PREC     // OR
	AND_PREC    // AND
	NOT_PREC    // NOT
	COMPARE     // =, !=, <, >, <=, >=, LIKE, IN, BETWEEN, IS
	CONCAT_PREC // ||
	ADD_PREC    // +, -
	MUL_PREC    // *, /, %
	UNARY       // -x, NOT x
	CALL        // function(), array[]
	HIGHEST
)

func (p *Parser) precedence(tok token.Token) int {
	switch tok {
	case token.AS:
		return ALIAS_PREC
	case token.OR:
		return OR_PREC
	case token.AND:
		return AND_PREC
	case token.NOT:
		return NOT_PREC
	case token.EQ, token.NEQ, token.LT, token.GT, token.LTE, token.GTE,
		token.LIKE, token.ILIKE, token.REGEXP, token.IN, token.BETWEEN, token.IS,
		token.NULL_SAFE_EQ, token.GLOBAL:
		return COMPARE
	case token.QUESTION:
		return COMPARE // Ternary operator
	case token.CONCAT:
		return CONCAT_PREC
	case token.PLUS, token.MINUS:
		return ADD_PREC
	case token.ASTERISK, token.SLASH, token.PERCENT, token.DIV, token.MOD:
		return MUL_PREC
	case token.LPAREN, token.LBRACKET:
		return CALL
	case token.EXCEPT, token.REPLACE:
		return CALL // For asterisk modifiers
	case token.COLONCOLON:
		return CALL // Cast operator
	case token.DOT:
		return HIGHEST // Dot access
	case token.ARROW:
		return ALIAS_PREC // Lambda arrow (low precedence)
	case token.NUMBER:
		// Handle .1 as tuple access (number starting with dot)
		return LOWEST
	default:
		return LOWEST
	}
}

// precedenceForCurrent returns the precedence for the current token,
// with special handling for tuple access (number starting with dot)
func (p *Parser) precedenceForCurrent() int {
	if p.currentIs(token.NUMBER) && strings.HasPrefix(p.current.Value, ".") {
		return HIGHEST // Tuple access like t.1
	}
	return p.precedence(p.current.Token)
}

func (p *Parser) parseExpressionList() []ast.Expression {
	var exprs []ast.Expression

	if p.currentIs(token.RPAREN) || p.currentIs(token.EOF) {
		return exprs
	}

	expr := p.parseExpression(LOWEST)
	if expr != nil {
		// Handle implicit alias (identifier without AS)
		expr = p.parseImplicitAlias(expr)
		exprs = append(exprs, expr)
	}

	for p.currentIs(token.COMMA) {
		p.nextToken()
		expr := p.parseExpression(LOWEST)
		if expr != nil {
			// Handle implicit alias (identifier without AS)
			expr = p.parseImplicitAlias(expr)
			exprs = append(exprs, expr)
		}
	}

	return exprs
}

// parseGroupingSets parses GROUPING SETS ((a), (b), (a, b))
func (p *Parser) parseGroupingSets() []ast.Expression {
	var exprs []ast.Expression

	if !p.expect(token.LPAREN) {
		return exprs
	}

	for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
		// Each element in GROUPING SETS is a tuple or a single expression
		if p.currentIs(token.LPAREN) {
			// Parse as tuple
			tuple := p.parseGroupedOrTuple()
			exprs = append(exprs, tuple)
		} else {
			// Single expression
			expr := p.parseExpression(LOWEST)
			if expr != nil {
				exprs = append(exprs, expr)
			}
		}

		// Skip comma if present
		if p.currentIs(token.COMMA) {
			p.nextToken()
		}
	}

	p.expect(token.RPAREN)
	return exprs
}

// parseFunctionArgumentList parses arguments for function calls, stopping at SETTINGS
func (p *Parser) parseFunctionArgumentList() []ast.Expression {
	var exprs []ast.Expression

	if p.currentIs(token.RPAREN) || p.currentIs(token.EOF) || p.currentIs(token.SETTINGS) {
		return exprs
	}

	expr := p.parseExpression(LOWEST)
	if expr != nil {
		exprs = append(exprs, expr)
	}

	for p.currentIs(token.COMMA) {
		p.nextToken()
		// Stop if we hit SETTINGS
		if p.currentIs(token.SETTINGS) {
			break
		}
		expr := p.parseExpression(LOWEST)
		if expr != nil {
			exprs = append(exprs, expr)
		}
	}

	return exprs
}

// parseImplicitAlias handles implicit column aliases like "SELECT 'a' c0" (meaning 'a' AS c0)
func (p *Parser) parseImplicitAlias(expr ast.Expression) ast.Expression {
	// If next token is a plain identifier (not a keyword), treat as implicit alias
	// Keywords like FROM, WHERE etc. are tokenized as their own token types, not IDENT
	if p.currentIs(token.IDENT) {
		alias := p.current.Value
		p.nextToken()

		// Set alias on the expression if it supports it
		switch e := expr.(type) {
		case *ast.Identifier:
			e.Alias = alias
			return e
		case *ast.FunctionCall:
			e.Alias = alias
			return e
		case *ast.Subquery:
			e.Alias = alias
			return e
		default:
			return &ast.AliasedExpr{
				Position: expr.Pos(),
				Expr:     expr,
				Alias:    alias,
			}
		}
	}
	return expr
}

func (p *Parser) parseExpression(precedence int) ast.Expression {
	left := p.parsePrefixExpression()
	if left == nil {
		return nil
	}

	for !p.currentIs(token.EOF) && precedence < p.precedenceForCurrent() {
		left = p.parseInfixExpression(left)
		if left == nil {
			return nil
		}
	}

	return left
}

func (p *Parser) parsePrefixExpression() ast.Expression {
	switch p.current.Token {
	case token.IDENT:
		return p.parseIdentifierOrFunction()
	case token.NUMBER:
		return p.parseNumber()
	case token.STRING:
		return p.parseString()
	case token.TRUE, token.FALSE:
		return p.parseBoolean()
	case token.NULL:
		return p.parseNull()
	case token.NAN, token.INF:
		return p.parseSpecialNumber()
	case token.MINUS:
		return p.parseUnaryMinus()
	case token.NOT:
		return p.parseNot()
	case token.LPAREN:
		return p.parseGroupedOrTuple()
	case token.LBRACKET:
		return p.parseArrayLiteral()
	case token.ASTERISK:
		return p.parseAsterisk()
	case token.CASE:
		return p.parseCase()
	case token.CAST:
		return p.parseCast()
	case token.EXTRACT:
		return p.parseExtract()
	case token.INTERVAL:
		// INTERVAL can be a literal (INTERVAL 1 DAY) or identifier reference
		// Check if next token can start an interval value
		if p.peekIs(token.NUMBER) || p.peekIs(token.LPAREN) || p.peekIs(token.MINUS) || p.peekIs(token.STRING) {
			return p.parseInterval()
		}
		// Otherwise treat as identifier
		return p.parseKeywordAsIdentifier()
	case token.EXISTS:
		return p.parseExists()
	case token.PARAM:
		return p.parseParameter()
	case token.QUESTION:
		return p.parsePositionalParameter()
	case token.SUBSTRING:
		return p.parseSubstring()
	case token.TRIM:
		return p.parseTrim()
	case token.COLUMNS:
		return p.parseColumnsMatcher()
	case token.ARRAY:
		// array(1,2,3) constructor
		return p.parseArrayConstructor()
	case token.IF:
		// IF function
		return p.parseIfFunction()
	case token.FORMAT:
		// format() function (not FORMAT clause)
		if p.peekIs(token.LPAREN) {
			return p.parseKeywordAsFunction()
		}
		// format as identifier (e.g., format='Parquet' in function args)
		return p.parseKeywordAsIdentifier()
	default:
		// Handle other keywords that can be used as function names or identifiers
		if p.current.Token.IsKeyword() {
			if p.peekIs(token.LPAREN) {
				return p.parseKeywordAsFunction()
			}
			// Keywords like ALL, DEFAULT, etc. can be used as identifiers
			return p.parseKeywordAsIdentifier()
		}
		return nil
	}
}

func (p *Parser) parseInfixExpression(left ast.Expression) ast.Expression {
	switch p.current.Token {
	case token.PLUS, token.MINUS, token.ASTERISK, token.SLASH, token.PERCENT,
		token.EQ, token.NEQ, token.LT, token.GT, token.LTE, token.GTE,
		token.AND, token.OR, token.CONCAT, token.DIV, token.MOD:
		return p.parseBinaryExpression(left)
	case token.NULL_SAFE_EQ:
		return p.parseBinaryExpression(left)
	case token.QUESTION:
		return p.parseTernary(left)
	case token.LIKE, token.ILIKE:
		return p.parseLikeExpression(left, false)
	case token.REGEXP:
		return p.parseRegexpExpression(left, false)
	case token.NOT:
		// NOT IN, NOT LIKE, NOT BETWEEN, NOT REGEXP, IS NOT
		p.nextToken()
		switch p.current.Token {
		case token.IN:
			return p.parseInExpression(left, true)
		case token.LIKE:
			return p.parseLikeExpression(left, true)
		case token.ILIKE:
			return p.parseLikeExpression(left, true)
		case token.REGEXP:
			return p.parseRegexpExpression(left, true)
		case token.BETWEEN:
			return p.parseBetweenExpression(left, true)
		default:
			// Put back NOT and treat as binary
			return left
		}
	case token.IN:
		return p.parseInExpression(left, false)
	case token.GLOBAL:
		// GLOBAL IN or GLOBAL NOT IN
		p.nextToken()
		not := false
		if p.currentIs(token.NOT) {
			not = true
			p.nextToken()
		}
		if p.currentIs(token.IN) {
			expr := p.parseInExpression(left, not)
			if inExpr, ok := expr.(*ast.InExpr); ok {
				inExpr.Global = true
			}
			return expr
		}
		return left
	case token.BETWEEN:
		return p.parseBetweenExpression(left, false)
	case token.IS:
		return p.parseIsExpression(left)
	case token.LPAREN:
		// Function call on identifier
		if ident, ok := left.(*ast.Identifier); ok {
			return p.parseFunctionCall(ident.Name(), ident.Position)
		}
		// Parametric function call like quantile(0.9)(number)
		if fn, ok := left.(*ast.FunctionCall); ok {
			return p.parseParametricFunctionCall(fn)
		}
		return left
	case token.LBRACKET:
		return p.parseArrayAccess(left)
	case token.DOT:
		return p.parseDotAccess(left)
	case token.AS:
		return p.parseAlias(left)
	case token.COLONCOLON:
		return p.parseCastOperator(left)
	case token.ARROW:
		return p.parseLambda(left)
	case token.EXCEPT:
		// Handle * EXCEPT (col1, col2)
		if asterisk, ok := left.(*ast.Asterisk); ok {
			return p.parseAsteriskExcept(asterisk)
		}
		return left
	case token.REPLACE:
		// Handle * REPLACE (expr AS col)
		if asterisk, ok := left.(*ast.Asterisk); ok {
			return p.parseAsteriskReplace(asterisk)
		}
		return left
	case token.NUMBER:
		// Handle tuple access like t.1 where .1 is lexed as a number
		if strings.HasPrefix(p.current.Value, ".") {
			return p.parseTupleAccessFromNumber(left)
		}
		return left
	default:
		return left
	}
}

func (p *Parser) parseIdentifierOrFunction() ast.Expression {
	pos := p.current.Pos
	name := p.current.Value
	p.nextToken()

	// Check for function call
	if p.currentIs(token.LPAREN) {
		return p.parseFunctionCall(name, pos)
	}

	// Check for qualified identifier (a.b.c)
	parts := []string{name}
	for p.currentIs(token.DOT) {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			parts = append(parts, p.current.Value)
			p.nextToken()
		} else if p.currentIs(token.ASTERISK) {
			// table.*
			p.nextToken()
			return &ast.Asterisk{
				Position: pos,
				Table:    strings.Join(parts, "."),
			}
		} else {
			break
		}
	}

	// Check for function call after qualified name
	if p.currentIs(token.LPAREN) {
		return p.parseFunctionCall(strings.Join(parts, "."), pos)
	}

	return &ast.Identifier{
		Position: pos,
		Parts:    parts,
	}
}

func (p *Parser) parseFunctionCall(name string, pos token.Position) *ast.FunctionCall {
	fn := &ast.FunctionCall{
		Position: pos,
		Name:     name,
	}

	p.nextToken() // skip (

	// Handle DISTINCT
	if p.currentIs(token.DISTINCT) {
		fn.Distinct = true
		p.nextToken()
	}

	// Handle view() and similar functions that take a subquery as argument
	// view(SELECT ...) should parse SELECT as a subquery, not expression
	if strings.ToLower(name) == "view" && (p.currentIs(token.SELECT) || p.currentIs(token.WITH)) {
		subquery := p.parseSelectWithUnion()
		fn.Arguments = []ast.Expression{&ast.Subquery{Position: pos, Query: subquery}}
	} else if !p.currentIs(token.RPAREN) && !p.currentIs(token.SETTINGS) {
		// Parse arguments
		fn.Arguments = p.parseFunctionArgumentList()
	}

	// Handle SETTINGS inside function call (table functions)
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		fn.Settings = p.parseSettingsList()
	}

	p.expect(token.RPAREN)

	// Handle IGNORE NULLS / RESPECT NULLS (window function modifiers)
	if p.currentIs(token.IDENT) {
		upper := strings.ToUpper(p.current.Value)
		if upper == "IGNORE" || upper == "RESPECT" {
			p.nextToken()
			if p.currentIs(token.NULLS) {
				p.nextToken()
			}
		}
	}

	// Handle OVER clause for window functions
	if p.currentIs(token.OVER) {
		p.nextToken()
		fn.Over = p.parseWindowSpec()
	}

	// Note: AS alias is handled by the expression parser's infix handling (parseAlias)
	// to respect precedence levels when called from contexts like WITH clauses

	return fn
}

func (p *Parser) parseWindowSpec() *ast.WindowSpec {
	spec := &ast.WindowSpec{
		Position: p.current.Pos,
	}

	if p.currentIs(token.IDENT) {
		// Window name reference
		spec.Name = p.current.Value
		p.nextToken()
		return spec
	}

	if !p.expect(token.LPAREN) {
		return spec
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

	// Parse frame specification
	if p.currentIs(token.IDENT) {
		frameType := strings.ToUpper(p.current.Value)
		if frameType == "ROWS" || frameType == "RANGE" || frameType == "GROUPS" {
			spec.Frame = p.parseWindowFrame()
		}
	}

	p.expect(token.RPAREN)
	return spec
}

func (p *Parser) parseWindowFrame() *ast.WindowFrame {
	frame := &ast.WindowFrame{
		Position: p.current.Pos,
	}

	switch strings.ToUpper(p.current.Value) {
	case "ROWS":
		frame.Type = ast.FrameRows
	case "RANGE":
		frame.Type = ast.FrameRange
	case "GROUPS":
		frame.Type = ast.FrameGroups
	}
	p.nextToken()

	if p.currentIs(token.BETWEEN) {
		p.nextToken()
		frame.StartBound = p.parseFrameBound()
		if p.currentIs(token.AND) {
			p.nextToken()
			frame.EndBound = p.parseFrameBound()
		}
	} else {
		frame.StartBound = p.parseFrameBound()
	}

	return frame
}

func (p *Parser) parseFrameBound() *ast.FrameBound {
	bound := &ast.FrameBound{
		Position: p.current.Pos,
	}

	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "CURRENT" {
		p.nextToken()
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROW" {
			p.nextToken()
		}
		bound.Type = ast.BoundCurrentRow
		return bound
	}

	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "UNBOUNDED" {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			switch strings.ToUpper(p.current.Value) {
			case "PRECEDING":
				bound.Type = ast.BoundUnboundedPre
			case "FOLLOWING":
				bound.Type = ast.BoundUnboundedFol
			}
			p.nextToken()
		}
		return bound
	}

	// n PRECEDING or n FOLLOWING
	bound.Offset = p.parseExpression(LOWEST)
	if p.currentIs(token.IDENT) {
		switch strings.ToUpper(p.current.Value) {
		case "PRECEDING":
			bound.Type = ast.BoundPreceding
		case "FOLLOWING":
			bound.Type = ast.BoundFollowing
		}
		p.nextToken()
	}

	return bound
}

func (p *Parser) parseNumber() ast.Expression {
	lit := &ast.Literal{
		Position: p.current.Pos,
	}

	value := p.current.Value
	p.nextToken()

	// Check if it's a float
	if strings.Contains(value, ".") || strings.ContainsAny(value, "eE") {
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			lit.Type = ast.LiteralString
			lit.Value = value
		} else {
			lit.Type = ast.LiteralFloat
			lit.Value = f
		}
	} else {
		// Try signed int64 first
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			// Try unsigned uint64 for large positive numbers
			u, uerr := strconv.ParseUint(value, 10, 64)
			if uerr != nil {
				lit.Type = ast.LiteralString
				lit.Value = value
			} else {
				lit.Type = ast.LiteralInteger
				lit.Value = u // Store as uint64
			}
		} else {
			lit.Type = ast.LiteralInteger
			lit.Value = i
		}
	}

	return lit
}

func (p *Parser) parseString() ast.Expression {
	lit := &ast.Literal{
		Position: p.current.Pos,
		Type:     ast.LiteralString,
		Value:    p.current.Value,
	}
	p.nextToken()
	return lit
}

func (p *Parser) parseBoolean() ast.Expression {
	lit := &ast.Literal{
		Position: p.current.Pos,
		Type:     ast.LiteralBoolean,
		Value:    p.current.Token == token.TRUE,
	}
	p.nextToken()
	return lit
}

func (p *Parser) parseNull() ast.Expression {
	lit := &ast.Literal{
		Position: p.current.Pos,
		Type:     ast.LiteralNull,
		Value:    nil,
	}
	p.nextToken()
	return lit
}

func (p *Parser) parseSpecialNumber() ast.Expression {
	lit := &ast.Literal{
		Position: p.current.Pos,
		Type:     ast.LiteralFloat,
	}
	switch p.current.Token {
	case token.NAN:
		lit.Value = math.NaN()
	case token.INF:
		lit.Value = math.Inf(1)
	}
	p.nextToken()
	return lit
}

func (p *Parser) parseUnaryMinus() ast.Expression {
	expr := &ast.UnaryExpr{
		Position: p.current.Pos,
		Op:       "-",
	}
	p.nextToken()
	expr.Operand = p.parseExpression(UNARY)
	return expr
}

func (p *Parser) parseNot() ast.Expression {
	expr := &ast.UnaryExpr{
		Position: p.current.Pos,
		Op:       "NOT",
	}
	p.nextToken()
	expr.Operand = p.parseExpression(NOT_PREC)
	return expr
}

func (p *Parser) parseGroupedOrTuple() ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip (

	// Handle empty tuple ()
	if p.currentIs(token.RPAREN) {
		p.nextToken()
		return &ast.Literal{
			Position: pos,
			Type:     ast.LiteralTuple,
			Value:    []ast.Expression{},
		}
	}

	// Check for subquery (SELECT, WITH, or EXPLAIN)
	if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
		subquery := p.parseSelectWithUnion()
		p.expect(token.RPAREN)
		return &ast.Subquery{
			Position: pos,
			Query:    subquery,
		}
	}
	// EXPLAIN as subquery
	if p.currentIs(token.EXPLAIN) {
		explain := p.parseExplain()
		p.expect(token.RPAREN)
		return &ast.Subquery{
			Position: pos,
			Query:    explain,
		}
	}

	// Parse first expression
	first := p.parseExpression(LOWEST)

	// Check if it's a tuple
	if p.currentIs(token.COMMA) {
		elements := []ast.Expression{first}
		for p.currentIs(token.COMMA) {
			p.nextToken()
			elements = append(elements, p.parseExpression(LOWEST))
		}
		p.expect(token.RPAREN)
		return &ast.Literal{
			Position: pos,
			Type:     ast.LiteralTuple,
			Value:    elements,
		}
	}

	p.expect(token.RPAREN)
	return first
}

func (p *Parser) parseArrayLiteral() ast.Expression {
	lit := &ast.Literal{
		Position: p.current.Pos,
		Type:     ast.LiteralArray,
	}
	p.nextToken() // skip [

	var elements []ast.Expression
	if !p.currentIs(token.RBRACKET) {
		elements = p.parseExpressionList()
	}
	lit.Value = elements

	p.expect(token.RBRACKET)
	return lit
}

func (p *Parser) parseAsterisk() ast.Expression {
	asterisk := &ast.Asterisk{
		Position: p.current.Pos,
	}
	p.nextToken()
	return asterisk
}

func (p *Parser) parseCase() ast.Expression {
	expr := &ast.CaseExpr{
		Position: p.current.Pos,
	}
	p.nextToken() // skip CASE

	// Check for CASE operand (simple CASE)
	if !p.currentIs(token.WHEN) {
		expr.Operand = p.parseExpression(LOWEST)
	}

	// Parse WHEN clauses
	for p.currentIs(token.WHEN) {
		when := &ast.WhenClause{
			Position: p.current.Pos,
		}
		p.nextToken() // skip WHEN

		when.Condition = p.parseExpression(LOWEST)

		if !p.expect(token.THEN) {
			break
		}

		when.Result = p.parseExpression(LOWEST)
		expr.Whens = append(expr.Whens, when)
	}

	// Parse ELSE clause
	if p.currentIs(token.ELSE) {
		p.nextToken()
		expr.Else = p.parseExpression(LOWEST)
	}

	p.expect(token.END)

	// Handle alias
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			expr.Alias = p.current.Value
			p.nextToken()
		}
	}

	return expr
}

func (p *Parser) parseCast() ast.Expression {
	expr := &ast.CastExpr{
		Position: p.current.Pos,
	}
	p.nextToken() // skip CAST

	if !p.expect(token.LPAREN) {
		return nil
	}

	// Use ALIAS_PREC to avoid consuming AS as an alias operator
	expr.Expr = p.parseExpression(ALIAS_PREC)

	// Handle both CAST(x AS Type) and CAST(x, 'Type') syntax
	if p.currentIs(token.AS) {
		p.nextToken()
		expr.Type = p.parseDataType()
	} else if p.currentIs(token.COMMA) {
		p.nextToken()
		// Type is given as a string literal
		if p.currentIs(token.STRING) {
			expr.Type = &ast.DataType{
				Position: p.current.Pos,
				Name:     p.current.Value,
			}
			p.nextToken()
		}
	}

	p.expect(token.RPAREN)

	return expr
}

func (p *Parser) parseExtract() ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip EXTRACT

	if !p.expect(token.LPAREN) {
		return nil
	}

	// Check if it's EXTRACT(field FROM expr) or extract(str, pattern) form
	if p.currentIs(token.IDENT) {
		field := strings.ToUpper(p.current.Value)
		p.nextToken()

		// Check for FROM keyword - if present, it's the EXTRACT(field FROM expr) form
		if p.currentIs(token.FROM) {
			p.nextToken()
			from := p.parseExpression(LOWEST)
			p.expect(token.RPAREN)
			return &ast.ExtractExpr{
				Position: pos,
				Field:    field,
				From:     from,
			}
		}

		// Not FROM, so backtrack and parse as regular function call
		// This is the extract(str, pattern) regex form
		// We need to re-parse as a function call
		args := []ast.Expression{
			&ast.Identifier{Position: pos, Parts: []string{strings.ToLower(field)}},
		}
		if p.currentIs(token.COMMA) {
			p.nextToken()
			for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
				args = append(args, p.parseExpression(LOWEST))
				if p.currentIs(token.COMMA) {
					p.nextToken()
				} else {
					break
				}
			}
		}
		p.expect(token.RPAREN)
		return &ast.FunctionCall{
			Position:  pos,
			Name:      "extract",
			Arguments: args,
		}
	}

	// If first token is a string, it's the regex form extract(str, pattern)
	var args []ast.Expression
	for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
		args = append(args, p.parseExpression(LOWEST))
		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}
	p.expect(token.RPAREN)

	return &ast.FunctionCall{
		Position:  pos,
		Name:      "extract",
		Arguments: args,
	}
}

func (p *Parser) parseInterval() ast.Expression {
	expr := &ast.IntervalExpr{
		Position: p.current.Pos,
	}
	p.nextToken() // skip INTERVAL

	expr.Value = p.parseExpression(LOWEST)

	// Parse unit
	if p.currentIs(token.IDENT) {
		expr.Unit = strings.ToUpper(p.current.Value)
		p.nextToken()
	}

	return expr
}

func (p *Parser) parseExists() ast.Expression {
	expr := &ast.ExistsExpr{
		Position: p.current.Pos,
	}
	p.nextToken() // skip EXISTS

	if !p.expect(token.LPAREN) {
		return nil
	}

	expr.Query = p.parseSelectWithUnion()

	p.expect(token.RPAREN)

	return expr
}

func (p *Parser) parseParameter() ast.Expression {
	param := &ast.Parameter{
		Position: p.current.Pos,
	}

	value := p.current.Value
	p.nextToken()

	// Parse {name:Type} format
	parts := strings.SplitN(value, ":", 2)
	param.Name = parts[0]
	if len(parts) > 1 {
		param.Type = &ast.DataType{Name: parts[1]}
	}

	return param
}

func (p *Parser) parsePositionalParameter() ast.Expression {
	param := &ast.Parameter{
		Position: p.current.Pos,
	}
	p.nextToken()
	return param
}

func (p *Parser) parseSubstring() ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip SUBSTRING

	if !p.expect(token.LPAREN) {
		return nil
	}

	args := []ast.Expression{p.parseExpression(LOWEST)}

	// Handle FROM
	if p.currentIs(token.FROM) {
		p.nextToken()
		args = append(args, p.parseExpression(LOWEST))
	} else if p.currentIs(token.COMMA) {
		p.nextToken()
		args = append(args, p.parseExpression(LOWEST))
	}

	// Handle FOR
	if p.currentIs(token.FOR) {
		p.nextToken()
		args = append(args, p.parseExpression(LOWEST))
	} else if p.currentIs(token.COMMA) {
		p.nextToken()
		args = append(args, p.parseExpression(LOWEST))
	}

	p.expect(token.RPAREN)

	return &ast.FunctionCall{
		Position:  pos,
		Name:      "substring",
		Arguments: args,
	}
}

func (p *Parser) parseTrim() ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip TRIM

	if !p.expect(token.LPAREN) {
		return nil
	}

	var trimType string
	var trimChars ast.Expression

	// Check for LEADING, TRAILING, BOTH
	if p.currentIs(token.LEADING) {
		trimType = "LEADING"
		p.nextToken()
	} else if p.currentIs(token.TRAILING) {
		trimType = "TRAILING"
		p.nextToken()
	} else if p.currentIs(token.BOTH) {
		trimType = "BOTH"
		p.nextToken()
	}

	// Parse characters to trim (if specified)
	if !p.currentIs(token.FROM) && !p.currentIs(token.RPAREN) {
		trimChars = p.parseExpression(LOWEST)
	}

	// FROM clause
	var expr ast.Expression
	if p.currentIs(token.FROM) {
		p.nextToken()
		expr = p.parseExpression(LOWEST)
	} else {
		expr = trimChars
		trimChars = nil
	}

	p.expect(token.RPAREN)

	// Build appropriate function call
	fnName := "trim"
	switch trimType {
	case "LEADING":
		fnName = "trimLeft"
	case "TRAILING":
		fnName = "trimRight"
	}

	args := []ast.Expression{expr}
	if trimChars != nil {
		args = append(args, trimChars)
	}

	return &ast.FunctionCall{
		Position:  pos,
		Name:      fnName,
		Arguments: args,
	}
}

func (p *Parser) parseBinaryExpression(left ast.Expression) ast.Expression {
	expr := &ast.BinaryExpr{
		Position: p.current.Pos,
		Left:     left,
		Op:       p.current.Value,
	}

	if p.current.Token.IsKeyword() {
		expr.Op = strings.ToUpper(p.current.Value)
	}

	prec := p.precedence(p.current.Token)
	p.nextToken()

	expr.Right = p.parseExpression(prec)
	return expr
}

func (p *Parser) parseLikeExpression(left ast.Expression, not bool) ast.Expression {
	expr := &ast.LikeExpr{
		Position: p.current.Pos,
		Expr:     left,
		Not:      not,
	}

	if p.currentIs(token.ILIKE) {
		expr.CaseInsensitive = true
	}

	p.nextToken() // skip LIKE/ILIKE

	expr.Pattern = p.parseExpression(COMPARE)
	return expr
}

func (p *Parser) parseRegexpExpression(left ast.Expression, not bool) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip REGEXP

	pattern := p.parseExpression(COMPARE)

	// REGEXP translates to match(expr, pattern) function
	fnCall := &ast.FunctionCall{
		Position:  pos,
		Name:      "match",
		Arguments: []ast.Expression{left, pattern},
	}

	if not {
		// NOT REGEXP uses NOT match(...)
		return &ast.UnaryExpr{
			Position: pos,
			Op:       "NOT",
			Operand:  fnCall,
		}
	}
	return fnCall
}

func (p *Parser) parseInExpression(left ast.Expression, not bool) ast.Expression {
	expr := &ast.InExpr{
		Position: p.current.Pos,
		Expr:     left,
		Not:      not,
	}

	// Handle GLOBAL IN
	if p.currentIs(token.GLOBAL) {
		expr.Global = true
		p.nextToken()
	}

	p.nextToken() // skip IN

	// Handle different IN list formats:
	// 1. (subquery or list) - standard format
	// 2. [array literal] - array format
	// 3. identifier - table or alias reference
	// 4. tuple(...) - explicit tuple function

	if p.currentIs(token.LPAREN) {
		p.nextToken() // skip (
		// Check for subquery
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
			expr.Query = p.parseSelectWithUnion()
		} else {
			expr.List = p.parseExpressionList()
		}
		p.expect(token.RPAREN)
	} else if p.currentIs(token.LBRACKET) {
		// Array literal: IN [1, 2, 3]
		arr := p.parseArrayLiteral()
		expr.List = []ast.Expression{arr}
	} else {
		// Could be identifier, tuple function, or other expression
		// Parse as expression
		innerExpr := p.parseExpression(CALL)
		if innerExpr != nil {
			expr.List = []ast.Expression{innerExpr}
		}
	}

	return expr
}

func (p *Parser) parseBetweenExpression(left ast.Expression, not bool) ast.Expression {
	expr := &ast.BetweenExpr{
		Position: p.current.Pos,
		Expr:     left,
		Not:      not,
	}

	p.nextToken() // skip BETWEEN

	expr.Low = p.parseExpression(COMPARE)

	if !p.expect(token.AND) {
		return nil
	}

	expr.High = p.parseExpression(COMPARE)
	return expr
}

func (p *Parser) parseIsExpression(left ast.Expression) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip IS

	not := false
	if p.currentIs(token.NOT) {
		not = true
		p.nextToken()
	}

	if p.currentIs(token.NULL) {
		p.nextToken()
		return &ast.IsNullExpr{
			Position: pos,
			Expr:     left,
			Not:      not,
		}
	}

	// IS TRUE, IS FALSE
	if p.currentIs(token.TRUE) || p.currentIs(token.FALSE) {
		value := p.currentIs(token.TRUE)
		if not {
			value = !value
		}
		p.nextToken()
		return &ast.BinaryExpr{
			Position: pos,
			Left:     left,
			Op:       "=",
			Right: &ast.Literal{
				Position: pos,
				Type:     ast.LiteralBoolean,
				Value:    value,
			},
		}
	}

	return left
}

func (p *Parser) parseArrayAccess(left ast.Expression) ast.Expression {
	expr := &ast.ArrayAccess{
		Position: p.current.Pos,
		Array:    left,
	}

	p.nextToken() // skip [
	expr.Index = p.parseExpression(LOWEST)
	p.expect(token.RBRACKET)

	return expr
}

// parseTupleAccessFromNumber handles tuple access like t.1 where .1 was lexed as a single NUMBER token
func (p *Parser) parseTupleAccessFromNumber(left ast.Expression) ast.Expression {
	// The current value is like ".1" - extract the index part
	indexStr := strings.TrimPrefix(p.current.Value, ".")
	pos := p.current.Pos
	p.nextToken()

	idx, err := strconv.ParseInt(indexStr, 10, 64)
	if err != nil {
		return left
	}

	return &ast.TupleAccess{
		Position: pos,
		Tuple:    left,
		Index: &ast.Literal{
			Position: pos,
			Type:     ast.LiteralInteger,
			Value:    idx,
		},
	}
}

func (p *Parser) parseDotAccess(left ast.Expression) ast.Expression {
	p.nextToken() // skip .

	// Check for tuple access with number
	if p.currentIs(token.NUMBER) {
		expr := &ast.TupleAccess{
			Position: p.current.Pos,
			Tuple:    left,
			Index:    p.parseNumber(),
		}
		return expr
	}

	// Regular identifier access
	if p.currentIs(token.IDENT) {
		if ident, ok := left.(*ast.Identifier); ok {
			ident.Parts = append(ident.Parts, p.current.Value)
			p.nextToken()

			// Check for function call
			if p.currentIs(token.LPAREN) {
				return p.parseFunctionCall(ident.Name(), ident.Position)
			}

			// Check for table.*
			if p.currentIs(token.ASTERISK) {
				tableName := ident.Name()
				p.nextToken()
				return &ast.Asterisk{
					Position: ident.Position,
					Table:    tableName,
				}
			}

			return ident
		}
	}

	return left
}

func (p *Parser) parseAlias(left ast.Expression) ast.Expression {
	p.nextToken() // skip AS

	// Alias can be an identifier or a keyword (ClickHouse allows keywords as aliases)
	alias := ""
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		alias = p.current.Value
		p.nextToken()
	}

	// Set alias on the expression if it supports it
	switch e := left.(type) {
	case *ast.Identifier:
		e.Alias = alias
		return e
	case *ast.FunctionCall:
		e.Alias = alias
		return e
	case *ast.Subquery:
		e.Alias = alias
		return e
	default:
		return &ast.AliasedExpr{
			Position: left.Pos(),
			Expr:     left,
			Alias:    alias,
		}
	}
}

func (p *Parser) parseCastOperator(left ast.Expression) ast.Expression {
	expr := &ast.CastExpr{
		Position:       p.current.Pos,
		Expr:           left,
		OperatorSyntax: true,
	}

	p.nextToken() // skip ::

	expr.Type = p.parseDataType()
	return expr
}

func (p *Parser) parseLambda(left ast.Expression) ast.Expression {
	lambda := &ast.Lambda{
		Position: p.current.Pos,
	}

	// Extract parameter names from left expression
	switch e := left.(type) {
	case *ast.Identifier:
		lambda.Parameters = e.Parts
	case *ast.Literal:
		if e.Type == ast.LiteralTuple {
			if exprs, ok := e.Value.([]ast.Expression); ok {
				for _, expr := range exprs {
					if ident, ok := expr.(*ast.Identifier); ok {
						lambda.Parameters = append(lambda.Parameters, ident.Name())
					}
				}
			}
		}
	}

	p.nextToken() // skip ->

	lambda.Body = p.parseExpression(LOWEST)
	return lambda
}

func (p *Parser) parseTernary(condition ast.Expression) ast.Expression {
	ternary := &ast.TernaryExpr{
		Position:  p.current.Pos,
		Condition: condition,
	}

	p.nextToken() // skip ?

	ternary.Then = p.parseExpression(LOWEST)

	if !p.expect(token.COLON) {
		return nil
	}

	ternary.Else = p.parseExpression(LOWEST)

	return ternary
}

func (p *Parser) parseParametricFunctionCall(fn *ast.FunctionCall) *ast.FunctionCall {
	// The first FunctionCall's arguments become the parameters
	// and we parse the second set of arguments
	result := &ast.FunctionCall{
		Position:   fn.Position,
		Name:       fn.Name,
		Parameters: fn.Arguments, // Parameters are the first ()'s content
	}

	p.nextToken() // skip (

	// Parse the actual arguments
	if !p.currentIs(token.RPAREN) {
		result.Arguments = p.parseExpressionList()
	}

	p.expect(token.RPAREN)

	// Handle IGNORE NULLS / RESPECT NULLS (aggregate function modifiers)
	if p.currentIs(token.IDENT) {
		upper := strings.ToUpper(p.current.Value)
		if upper == "IGNORE" || upper == "RESPECT" {
			p.nextToken()
			if p.currentIs(token.NULLS) {
				p.nextToken()
			}
		}
	}

	// Handle OVER clause for window functions
	if p.currentIs(token.OVER) {
		p.nextToken()
		result.Over = p.parseWindowSpec()
	}

	return result
}

func (p *Parser) parseColumnsMatcher() ast.Expression {
	matcher := &ast.ColumnsMatcher{
		Position: p.current.Pos,
	}

	p.nextToken() // skip COLUMNS

	if !p.expect(token.LPAREN) {
		return nil
	}

	// Parse the pattern (string)
	if p.currentIs(token.STRING) {
		matcher.Pattern = p.current.Value
		p.nextToken()
	}

	p.expect(token.RPAREN)

	// Handle EXCEPT
	if p.currentIs(token.EXCEPT) {
		p.nextToken()
		if p.expect(token.LPAREN) {
			for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
				if p.currentIs(token.IDENT) {
					matcher.Except = append(matcher.Except, p.current.Value)
					p.nextToken()
				}
				if p.currentIs(token.COMMA) {
					p.nextToken()
				}
			}
			p.expect(token.RPAREN)
		}
	}

	return matcher
}

func (p *Parser) parseArrayConstructor() ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip ARRAY

	if !p.expect(token.LPAREN) {
		return nil
	}

	var args []ast.Expression
	if !p.currentIs(token.RPAREN) {
		args = p.parseExpressionList()
	}

	p.expect(token.RPAREN)

	return &ast.FunctionCall{
		Position:  pos,
		Name:      "array",
		Arguments: args,
	}
}

func (p *Parser) parseIfFunction() ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip IF

	if !p.expect(token.LPAREN) {
		return nil
	}

	var args []ast.Expression
	if !p.currentIs(token.RPAREN) {
		args = p.parseExpressionList()
	}

	p.expect(token.RPAREN)

	return &ast.FunctionCall{
		Position:  pos,
		Name:      "if",
		Arguments: args,
	}
}

func (p *Parser) parseKeywordAsFunction() ast.Expression {
	pos := p.current.Pos
	name := strings.ToLower(p.current.Value)
	p.nextToken() // skip keyword

	if !p.expect(token.LPAREN) {
		return nil
	}

	var args []ast.Expression
	// Handle view() and similar functions that take a subquery as argument
	if name == "view" && (p.currentIs(token.SELECT) || p.currentIs(token.WITH)) {
		subquery := p.parseSelectWithUnion()
		args = []ast.Expression{&ast.Subquery{Position: pos, Query: subquery}}
	} else if !p.currentIs(token.RPAREN) {
		args = p.parseExpressionList()
	}

	p.expect(token.RPAREN)

	return &ast.FunctionCall{
		Position:  pos,
		Name:      name,
		Arguments: args,
	}
}

func (p *Parser) parseKeywordAsIdentifier() ast.Expression {
	pos := p.current.Pos
	name := p.current.Value
	p.nextToken()

	return &ast.Identifier{
		Position: pos,
		Parts:    []string{name},
	}
}

func (p *Parser) parseAsteriskExcept(asterisk *ast.Asterisk) ast.Expression {
	p.nextToken() // skip EXCEPT

	if !p.expect(token.LPAREN) {
		return asterisk
	}

	for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
		if p.currentIs(token.IDENT) {
			asterisk.Except = append(asterisk.Except, p.current.Value)
			p.nextToken()
		}
		if p.currentIs(token.COMMA) {
			p.nextToken()
		}
	}

	p.expect(token.RPAREN)

	return asterisk
}

func (p *Parser) parseAsteriskReplace(asterisk *ast.Asterisk) ast.Expression {
	p.nextToken() // skip REPLACE

	if !p.expect(token.LPAREN) {
		return asterisk
	}

	for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
		replace := &ast.ReplaceExpr{
			Position: p.current.Pos,
		}

		replace.Expr = p.parseExpression(LOWEST)

		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) {
				replace.Name = p.current.Value
				p.nextToken()
			}
		}

		asterisk.Replace = append(asterisk.Replace, replace)

		if p.currentIs(token.COMMA) {
			p.nextToken()
		}
	}

	p.expect(token.RPAREN)

	return asterisk
}
