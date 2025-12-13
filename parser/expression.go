package parser

import (
	"fmt"
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
		token.LIKE, token.ILIKE, token.IN, token.BETWEEN, token.IS:
		return COMPARE
	case token.CONCAT:
		return CONCAT_PREC
	case token.PLUS, token.MINUS:
		return ADD_PREC
	case token.ASTERISK, token.SLASH, token.PERCENT:
		return MUL_PREC
	case token.LPAREN, token.LBRACKET:
		return CALL
	default:
		return LOWEST
	}
}

func (p *Parser) parseExpressionList() []ast.Expression {
	var exprs []ast.Expression

	if p.currentIs(token.RPAREN) || p.currentIs(token.EOF) {
		return exprs
	}

	exprs = append(exprs, p.parseExpression(LOWEST))

	for p.currentIs(token.COMMA) {
		p.nextToken()
		exprs = append(exprs, p.parseExpression(LOWEST))
	}

	return exprs
}

func (p *Parser) parseExpression(precedence int) ast.Expression {
	left := p.parsePrefixExpression()
	if left == nil {
		return nil
	}

	for !p.currentIs(token.EOF) && precedence < p.precedence(p.current.Token) {
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
		return p.parseInterval()
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
	default:
		return nil
	}
}

func (p *Parser) parseInfixExpression(left ast.Expression) ast.Expression {
	switch p.current.Token {
	case token.PLUS, token.MINUS, token.ASTERISK, token.SLASH, token.PERCENT,
		token.EQ, token.NEQ, token.LT, token.GT, token.LTE, token.GTE,
		token.AND, token.OR, token.CONCAT:
		return p.parseBinaryExpression(left)
	case token.LIKE, token.ILIKE:
		return p.parseLikeExpression(left, false)
	case token.NOT:
		// NOT IN, NOT LIKE, NOT BETWEEN, IS NOT
		p.nextToken()
		switch p.current.Token {
		case token.IN:
			return p.parseInExpression(left, true)
		case token.LIKE:
			return p.parseLikeExpression(left, true)
		case token.ILIKE:
			return p.parseLikeExpression(left, true)
		case token.BETWEEN:
			return p.parseBetweenExpression(left, true)
		default:
			// Put back NOT and treat as binary
			return left
		}
	case token.IN:
		return p.parseInExpression(left, false)
	case token.BETWEEN:
		return p.parseBetweenExpression(left, false)
	case token.IS:
		return p.parseIsExpression(left)
	case token.LPAREN:
		// Function call on identifier
		if ident, ok := left.(*ast.Identifier); ok {
			return p.parseFunctionCall(ident.Name(), ident.Position)
		}
		// Parametric function call like quantile(0.9)(number) - not yet supported
		// Return nil to signal error and prevent infinite loop
		p.errors = append(p.errors, fmt.Errorf("parametric function calls like func(params)(args) are not yet supported at line %d, column %d",
			p.current.Pos.Line, p.current.Pos.Column))
		return nil
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

	// Parse arguments
	if !p.currentIs(token.RPAREN) {
		fn.Arguments = p.parseExpressionList()
	}

	p.expect(token.RPAREN)

	// Handle OVER clause for window functions
	if p.currentIs(token.OVER) {
		p.nextToken()
		fn.Over = p.parseWindowSpec()
	}

	// Handle alias
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.IDENT) {
			fn.Alias = p.current.Value
			p.nextToken()
		}
	}

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
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			lit.Type = ast.LiteralString
			lit.Value = value
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

	// Check for subquery
	if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
		subquery := p.parseSelectWithUnion()
		p.expect(token.RPAREN)
		return &ast.Subquery{
			Position: pos,
			Query:    subquery,
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

	expr.Expr = p.parseExpression(LOWEST)

	if !p.expect(token.AS) {
		return nil
	}

	expr.Type = p.parseDataType()

	p.expect(token.RPAREN)

	return expr
}

func (p *Parser) parseExtract() ast.Expression {
	expr := &ast.ExtractExpr{
		Position: p.current.Pos,
	}
	p.nextToken() // skip EXTRACT

	if !p.expect(token.LPAREN) {
		return nil
	}

	// Parse field (YEAR, MONTH, etc.)
	if p.currentIs(token.IDENT) {
		expr.Field = strings.ToUpper(p.current.Value)
		p.nextToken()
	}

	if !p.expect(token.FROM) {
		return nil
	}

	expr.From = p.parseExpression(LOWEST)

	p.expect(token.RPAREN)

	return expr
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

	if !p.expect(token.LPAREN) {
		return nil
	}

	// Check for subquery
	if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
		expr.Query = p.parseSelectWithUnion()
	} else {
		expr.List = p.parseExpressionList()
	}

	p.expect(token.RPAREN)
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

	alias := ""
	if p.currentIs(token.IDENT) {
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
		Position: p.current.Pos,
		Expr:     left,
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
