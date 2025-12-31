package parser

import (
	"math"
	"strconv"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
	"github.com/sqlc-dev/doubleclick/token"
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
	case token.EXCEPT, token.REPLACE, token.APPLY:
		return CALL // For asterisk modifiers
	case token.COLONCOLON:
		return CALL // Cast operator
	case token.DOT:
		return HIGHEST // Dot access
	case token.ARROW:
		return OR_PREC // Lambda arrow (just above ALIAS_PREC to allow parsing before AS)
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

	// Post-process: merge consecutive identifiers followed by a lambda into a multi-param lambda
	// Pattern: [Ident("acc"), Lambda(["x"], body)] -> [Lambda(["acc", "x"], body)]
	exprs = mergeMultiParamLambdas(exprs)

	return exprs
}

// mergeMultiParamLambdas looks for pattern [Ident, Ident, ..., Lambda] at the START
// of the expression list and merges them into a single multi-param lambda.
// This handles ClickHouse's syntax: acc,x -> body (multi-param lambda without parentheses)
// This ONLY applies at position 0 - identifiers in the middle are regular arguments.
func mergeMultiParamLambdas(exprs []ast.Expression) []ast.Expression {
	if len(exprs) < 2 {
		return exprs
	}

	// Only check at position 0 - the pattern must start at the beginning
	if ident, ok := exprs[0].(*ast.Identifier); ok && len(ident.Parts) == 1 {
		// Count consecutive simple identifiers at the start
		j := 0
		var params []string
		for j < len(exprs) {
			if id, ok := exprs[j].(*ast.Identifier); ok && len(id.Parts) == 1 {
				params = append(params, id.Name())
				j++
			} else {
				break
			}
		}
		// Check if the next expression is a lambda and we have at least one identifier
		if j < len(exprs) && len(params) >= 1 {
			if lambda, ok := exprs[j].(*ast.Lambda); ok {
				// Don't merge if lambda was explicitly parenthesized
				// e.g., f(a, (x -> y)) should NOT merge 'a' into the lambda
				if lambda.Parenthesized {
					return exprs
				}
				// Merge the identifiers into the lambda's parameters
				newParams := make([]string, 0, len(params)+len(lambda.Parameters))
				newParams = append(newParams, params...)
				newParams = append(newParams, lambda.Parameters...)
				lambda.Parameters = newParams
				// Return lambda followed by remaining expressions
				result := make([]ast.Expression, 0, len(exprs)-j)
				result = append(result, lambda)
				result = append(result, exprs[j+1:]...)
				return result
			}
		}
	}

	// No merge needed
	return exprs
}

// parseImplicitAlias handles implicit column aliases like "SELECT 'a' c0" (meaning 'a' AS c0)
func (p *Parser) parseImplicitAlias(expr ast.Expression) ast.Expression {
	// Check if current token can be an implicit alias
	// Can be IDENT or certain keywords that are used as aliases (KEY, VALUE, TYPE, etc.)
	canBeAlias := p.currentIs(token.IDENT)
	if !canBeAlias {
		// Some keywords can be used as implicit aliases in ClickHouse
		switch p.current.Token {
		case token.KEY, token.INDEX, token.VIEW, token.DATABASE, token.TABLE:
			canBeAlias = true
		}
	}

	if canBeAlias {
		upper := strings.ToUpper(p.current.Value)
		// Don't consume SQL set operation keywords that aren't tokens
		if upper == "INTERSECT" {
			return expr
		}
		// Don't consume window frame keywords as implicit aliases
		switch upper {
		case "ROWS", "RANGE", "GROUPS", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT":
			return expr
		}
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
		case *ast.CastExpr:
			// Only set alias on CastExpr if using :: operator syntax
			// Function-style CAST() aliases go to AliasedExpr
			if e.OperatorSyntax {
				e.Alias = alias
				return e
			}
			return &ast.AliasedExpr{
				Position: expr.Pos(),
				Expr:     expr,
				Alias:    alias,
			}
		case *ast.CaseExpr:
			e.Alias = alias
			return e
		case *ast.ExtractExpr:
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
		// Track position to detect infinite loops (when infix parsing doesn't consume tokens)
		startPos := p.current.Pos
		left = p.parseInfixExpression(left)
		if left == nil {
			return nil
		}
		// If we didn't advance, break to avoid infinite loop
		if p.current.Pos == startPos {
			break
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
	case token.PLUS:
		return p.parseUnaryPlus()
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
		if p.peekIs(token.NUMBER) || p.peekIs(token.LPAREN) || p.peekIs(token.MINUS) || p.peekIs(token.STRING) || p.peekIs(token.IDENT) {
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
		// array(1,2,3) constructor or array as identifier (column name)
		if p.peekIs(token.LPAREN) {
			return p.parseArrayConstructor()
		}
		// array used as identifier (column/variable name)
		return p.parseKeywordAsIdentifier()
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
		// Handle * EXCEPT (col1, col2) or COLUMNS(...) EXCEPT (col1, col2)
		if asterisk, ok := left.(*ast.Asterisk); ok {
			return p.parseAsteriskExcept(asterisk)
		}
		if matcher, ok := left.(*ast.ColumnsMatcher); ok {
			return p.parseColumnsExcept(matcher)
		}
		return left
	case token.REPLACE:
		// Handle * REPLACE (expr AS col) or COLUMNS(...) REPLACE (expr AS col)
		if asterisk, ok := left.(*ast.Asterisk); ok {
			return p.parseAsteriskReplace(asterisk)
		}
		if matcher, ok := left.(*ast.ColumnsMatcher); ok {
			return p.parseColumnsReplace(matcher)
		}
		return left
	case token.APPLY:
		// Handle * APPLY (func) or COLUMNS(...) APPLY(func)
		if asterisk, ok := left.(*ast.Asterisk); ok {
			return p.parseAsteriskApply(asterisk)
		}
		if matcher, ok := left.(*ast.ColumnsMatcher); ok {
			return p.parseColumnsApply(matcher)
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

	// Check for typed literals: DATE '...', TIMESTAMP '...', TIME '...'
	// These are converted to toDate(), toDateTime(), toTime() function calls
	upperName := strings.ToUpper(name)
	if p.currentIs(token.STRING) && (upperName == "DATE" || upperName == "TIMESTAMP" || upperName == "TIME") {
		fnName := "toDate"
		if upperName == "TIMESTAMP" {
			fnName = "toDateTime"
		} else if upperName == "TIME" {
			fnName = "toTime"
		}
		strLit := &ast.Literal{
			Position: p.current.Pos,
			Type:     "String",
			Value:    p.current.Value,
		}
		p.nextToken()
		return &ast.FunctionCall{
			Position:  pos,
			Name:      fnName,
			Arguments: []ast.Expression{strLit},
		}
	}

	// Check for MySQL-style @@variable syntax (system variables)
	// Convert to globalVariable('varname') function call with alias @@varname
	if strings.HasPrefix(name, "@@") {
		varName := name[2:] // Strip @@
		// Handle @@session.var or @@global.var
		if p.currentIs(token.DOT) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				varName = varName + "." + p.current.Value
				name = name + "." + p.current.Value
				p.nextToken()
			}
		}
		return &ast.FunctionCall{
			Position: pos,
			Name:     "globalVariable",
			Alias:    name,
			Arguments: []ast.Expression{
				&ast.Literal{
					Position: pos,
					Type:     "String",
					Value:    varName,
				},
			},
		}
	}

	// Check for function call
	if p.currentIs(token.LPAREN) {
		return p.parseFunctionCall(name, pos)
	}

	// Check for qualified identifier (a.b.c)
	parts := []string{name}
	for p.currentIs(token.DOT) {
		p.nextToken()
		if p.currentIs(token.CARET) {
			// JSON path notation: x.^c0 (traverse into JSON field)
			p.nextToken() // skip ^
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				parts = append(parts, "^"+p.current.Value)
				p.nextToken()
			} else {
				break
			}
		} else if p.currentIs(token.COLON) {
			// JSON subcolumn type accessor: json.field.:`TypeName` or json.field.:TypeName
			p.nextToken() // skip :
			typePart := ":"
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() || p.currentIs(token.STRING) {
				typePart += "`" + p.current.Value + "`"
				p.nextToken()
			}
			parts = append(parts, typePart)
		} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			// Keywords can be used as column/field names (e.g., l_t.key, t.index)
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
		// Special case: qualified COLUMNS matcher (e.g., test_table.COLUMNS(id))
		if len(parts) >= 2 && strings.ToUpper(parts[len(parts)-1]) == "COLUMNS" {
			qualifier := strings.Join(parts[:len(parts)-1], ".")
			return p.parseQualifiedColumnsMatcher(qualifier, pos)
		}
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
	// view(SELECT ...) should parse SELECT as a subquery
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
	// Can appear multiple times (e.g., RESPECT NULLS IGNORE NULLS)
	for p.currentIs(token.IDENT) {
		upper := strings.ToUpper(p.current.Value)
		if upper == "IGNORE" || upper == "RESPECT" {
			p.nextToken()
			if p.currentIs(token.NULLS) {
				p.nextToken()
			}
		} else {
			break
		}
	}

	// Handle FILTER clause for aggregate functions: func() FILTER(WHERE condition)
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "FILTER" {
		p.nextToken() // skip FILTER
		if p.currentIs(token.LPAREN) {
			p.nextToken() // skip (
			if p.currentIs(token.WHERE) {
				p.nextToken() // skip WHERE
				// Parse the filter condition - just consume it for now
				// The filter is essentially a where clause for the aggregate
				p.parseExpression(LOWEST)
			}
			p.expect(token.RPAREN)
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
		// Window name reference (OVER w0)
		spec.Name = p.current.Value
		p.nextToken()
		return spec
	}

	if !p.expect(token.LPAREN) {
		return spec
	}

	// Check for named window reference inside parentheses: OVER (w0)
	// This happens when the identifier is not a known clause keyword
	if p.currentIs(token.IDENT) {
		upper := strings.ToUpper(p.current.Value)
		// If it's not a window clause keyword, it's a named window reference
		if upper != "PARTITION" && upper != "ORDER" && upper != "ROWS" && upper != "RANGE" && upper != "GROUPS" {
			spec.Name = p.current.Value
			p.nextToken()
			p.expect(token.RPAREN)
			return spec
		}
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

	// Check if this is a hex, binary, or octal number
	isHex := strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X")
	isBin := strings.HasPrefix(value, "0b") || strings.HasPrefix(value, "0B")
	isOctal := strings.HasPrefix(value, "0o") || strings.HasPrefix(value, "0O")

	// Check for hex float (e.g., 0x1.2p3)
	isHexFloat := isHex && (strings.ContainsAny(value, "pP") || strings.Contains(value, "."))

	// Check if it's a decimal float (but not a hex/binary/octal integer)
	// Note: hex numbers can contain 'e' as a hex digit, so we need to exclude them
	isDecimalFloat := !isHex && !isBin && !isOctal && (strings.Contains(value, ".") || strings.ContainsAny(value, "eE"))

	if isDecimalFloat {
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			lit.Type = ast.LiteralString
			lit.Value = value
		} else {
			lit.Type = ast.LiteralFloat
			lit.Value = f
			lit.Source = value // Preserve original source text (e.g., "0.0" vs "0")
		}
	} else if isHexFloat {
		// Parse hex float (Go doesn't support this directly, approximate)
		// For now, store as string - ClickHouse will interpret it
		lit.Type = ast.LiteralString
		lit.Value = value
	} else {
		// Determine the base for parsing
		// - 0x/0X: hex (base 16)
		// - 0b/0B: binary (base 2)
		// - 0o/0O: octal (base 8, explicit notation)
		// - Otherwise: decimal (base 10) - ClickHouse does NOT use leading zero for octal
		base := 10
		if isHex {
			base = 0 // Let strconv detect hex
		} else if isBin {
			base = 0 // Let strconv detect binary
		} else if isOctal {
			base = 0 // Let strconv detect octal with 0o prefix
		}
		// Note: We explicitly use base 10 for numbers like "077" because
		// ClickHouse does NOT interpret leading zeros as octal

		// Try signed int64 first
		i, err := strconv.ParseInt(value, base, 64)
		if err != nil {
			// Try unsigned uint64 for large positive numbers
			u, uerr := strconv.ParseUint(value, base, 64)
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
	pos := p.current.Pos
	p.nextToken() // skip minus

	// For negative number literals followed by ::, keep them together as a signed literal
	// This matches ClickHouse's behavior where -0::Int16 becomes CAST('-0', 'Int16')
	if p.currentIs(token.NUMBER) && p.peekIs(token.COLONCOLON) {
		// Parse the number and create a "signed" literal
		// We'll store the negative sign in the raw value
		numVal := "-" + p.current.Value
		lit := &ast.Literal{
			Position: pos,
			Type:     ast.LiteralInteger,
			Negative: true, // Mark as explicitly negative for proper formatting
		}
		// Check if it's a float
		if strings.Contains(numVal, ".") || strings.ContainsAny(numVal, "eE") {
			f, _ := strconv.ParseFloat(numVal, 64)
			lit.Type = ast.LiteralFloat
			lit.Value = f
			lit.Source = numVal // Preserve original source text
		} else {
			i, _ := strconv.ParseInt(numVal, 10, 64)
			lit.Value = i
		}
		p.nextToken() // move past number
		// Apply postfix operators like :: using the expression parsing loop
		left := ast.Expression(lit)
		for !p.currentIs(token.EOF) && LOWEST < p.precedenceForCurrent() {
			startPos := p.current.Pos
			left = p.parseInfixExpression(left)
			if left == nil {
				return nil
			}
			if p.current.Pos == startPos {
				break
			}
		}
		return left
	}

	// Standard unary minus handling
	expr := &ast.UnaryExpr{
		Position: pos,
		Op:       "-",
	}
	expr.Operand = p.parseExpression(UNARY)
	return expr
}

func (p *Parser) parseUnaryPlus() ast.Expression {
	expr := &ast.UnaryExpr{
		Position: p.current.Pos,
		Op:       "+",
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
	// When NOT is followed by a parenthesized expression, use UNARY precedence
	// so that binary operators after the group don't continue as part of the NOT operand.
	// e.g., NOT (0) + 1 should parse as (NOT(0)) + 1, not NOT((0) + 1)
	// But NOT 0 + 1 should parse as NOT(0 + 1)
	if p.currentIs(token.LPAREN) {
		expr.Operand = p.parseExpression(UNARY)
	} else {
		expr.Operand = p.parseExpression(NOT_PREC)
	}
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
			// Handle trailing comma: (1,) should create tuple with single element
			if p.currentIs(token.RPAREN) {
				break
			}
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

	// Mark binary expressions as parenthesized so we can preserve explicit
	// grouping in EXPLAIN output (e.g., "(a OR b) OR c" vs "a OR b OR c")
	if binExpr, ok := first.(*ast.BinaryExpr); ok {
		binExpr.Parenthesized = true
	}

	// Mark lambda expressions as parenthesized so we don't merge them
	// with preceding identifiers in multi-param lambda detection
	// e.g., f(a, (x -> y)) should NOT merge 'a' into the lambda
	if lambda, ok := first.(*ast.Lambda); ok {
		lambda.Parenthesized = true
	}

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
			expr.QuotedAlias = p.current.Quoted
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

	// Handle both CAST(x AS Type) and CAST(x, 'Type') or CAST(x, expr) syntax
	// Also handle CAST(x AS alias AS Type) and CAST(x alias AS Type) where alias is for the expression
	// And CAST(x AS alias, 'Type') and CAST(x alias, 'Type') for comma-style with aliased expression
	if p.currentIs(token.AS) {
		p.nextToken() // skip AS

		// Check what comes after the identifier
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			if p.peekIs(token.AS) {
				// "AS alias AS Type" pattern
				alias := p.current.Value
				p.nextToken() // skip alias
				p.nextToken() // skip AS
				expr.Expr = p.wrapWithAlias(expr.Expr, alias)
				expr.Type = p.parseDataType()
				expr.UsedASSyntax = true
			} else if p.peekIs(token.COMMA) {
				// "AS alias, 'Type'" pattern - comma-style with aliased expression
				alias := p.current.Value
				p.nextToken() // skip alias
				p.nextToken() // skip comma
				expr.Expr = p.wrapWithAlias(expr.Expr, alias)
				// Parse type (which may also have an alias)
				if p.currentIs(token.STRING) {
					typeStr := p.current.Value
					typePos := p.current.Pos
					p.nextToken()
					// Check for alias on the type string
					if p.currentIs(token.AS) {
						p.nextToken()
						if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
							typeAlias := p.current.Value
							p.nextToken()
							expr.TypeExpr = &ast.AliasedExpr{
								Position: typePos,
								Expr:     &ast.Literal{Position: typePos, Type: ast.LiteralString, Value: typeStr},
								Alias:    typeAlias,
							}
						} else {
							expr.Type = &ast.DataType{Position: typePos, Name: typeStr}
						}
					} else if p.currentIs(token.IDENT) && !p.peekIs(token.LPAREN) && !p.peekIs(token.COMMA) {
						// Implicit alias: cast('1234' AS lhs, 'UInt32' rhs)
						typeAlias := p.current.Value
						p.nextToken()
						expr.TypeExpr = &ast.AliasedExpr{
							Position: typePos,
							Expr:     &ast.Literal{Position: typePos, Type: ast.LiteralString, Value: typeStr},
							Alias:    typeAlias,
						}
					} else {
						expr.Type = &ast.DataType{Position: typePos, Name: typeStr}
					}
				} else {
					expr.TypeExpr = p.parseExpression(LOWEST)
				}
			} else {
				// Just "AS Type"
				expr.Type = p.parseDataType()
				expr.UsedASSyntax = true
			}
		} else {
			// Just "AS Type"
			expr.Type = p.parseDataType()
			expr.UsedASSyntax = true
		}
	} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.peekIs(token.AS) {
		// Handle "expr alias AS Type" pattern (alias without AS keyword)
		alias := p.current.Value
		p.nextToken() // skip alias
		p.nextToken() // skip AS
		expr.Expr = p.wrapWithAlias(expr.Expr, alias)
		expr.Type = p.parseDataType()
		expr.UsedASSyntax = true
	} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.peekIs(token.COMMA) {
		// Handle "expr alias, 'Type'" pattern (alias without AS keyword, comma-style)
		alias := p.current.Value
		p.nextToken() // skip alias
		p.nextToken() // skip comma
		expr.Expr = p.wrapWithAlias(expr.Expr, alias)
		// Parse type (which may also have an alias)
		if p.currentIs(token.STRING) {
			typeStr := p.current.Value
			typePos := p.current.Pos
			p.nextToken()
			// Check for alias on the type string
			if p.currentIs(token.AS) {
				p.nextToken()
				if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					typeAlias := p.current.Value
					p.nextToken()
					expr.TypeExpr = &ast.AliasedExpr{
						Position: typePos,
						Expr:     &ast.Literal{Position: typePos, Type: ast.LiteralString, Value: typeStr},
						Alias:    typeAlias,
					}
				} else {
					expr.Type = &ast.DataType{Position: typePos, Name: typeStr}
				}
			} else if p.currentIs(token.IDENT) && !p.peekIs(token.LPAREN) && !p.peekIs(token.COMMA) {
				// Implicit alias: cast('1234' lhs, 'UInt32' rhs)
				typeAlias := p.current.Value
				p.nextToken()
				expr.TypeExpr = &ast.AliasedExpr{
					Position: typePos,
					Expr:     &ast.Literal{Position: typePos, Type: ast.LiteralString, Value: typeStr},
					Alias:    typeAlias,
				}
			} else {
				expr.Type = &ast.DataType{Position: typePos, Name: typeStr}
			}
		} else {
			expr.TypeExpr = p.parseExpression(LOWEST)
		}
	} else if p.currentIs(token.COMMA) {
		p.nextToken()
		// Type can be given as a string literal or an expression (e.g., if(cond, 'Type1', 'Type2'))
		// It can also have an alias like: cast('1234', 'UInt32' AS rhs)
		if p.currentIs(token.STRING) {
			typeStr := p.current.Value
			typePos := p.current.Pos
			p.nextToken()
			// Check for alias on the type string
			if p.currentIs(token.AS) {
				p.nextToken()
				if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					alias := p.current.Value
					p.nextToken()
					// Store as aliased literal in TypeExpr
					expr.TypeExpr = &ast.AliasedExpr{
						Position: typePos,
						Expr: &ast.Literal{
							Position: typePos,
							Type:     ast.LiteralString,
							Value:    typeStr,
						},
						Alias: alias,
					}
				} else {
					expr.Type = &ast.DataType{Position: typePos, Name: typeStr}
				}
			} else if p.currentIs(token.IDENT) && !p.peekIs(token.LPAREN) && !p.peekIs(token.COMMA) {
				// Implicit alias (no AS keyword): cast('1234', 'UInt32' rhs)
				alias := p.current.Value
				p.nextToken()
				expr.TypeExpr = &ast.AliasedExpr{
					Position: typePos,
					Expr: &ast.Literal{
						Position: typePos,
						Type:     ast.LiteralString,
						Value:    typeStr,
					},
					Alias: alias,
				}
			} else {
				expr.Type = &ast.DataType{Position: typePos, Name: typeStr}
			}
		} else {
			// Parse as expression for dynamic type casting
			expr.TypeExpr = p.parseExpression(LOWEST)
		}
	}

	p.expect(token.RPAREN)

	return expr
}

// wrapWithAlias wraps an expression with an alias, handling different expression types appropriately
// If the expression already has an alias (e.g., AliasedExpr), the new alias replaces/overrides it
func (p *Parser) wrapWithAlias(expr ast.Expression, alias string) ast.Expression {
	switch e := expr.(type) {
	case *ast.Identifier:
		e.Alias = alias
		return e
	case *ast.FunctionCall:
		e.Alias = alias
		return e
	case *ast.AliasedExpr:
		// Replace the alias instead of double-wrapping
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

func (p *Parser) parseExtract() ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip EXTRACT

	if !p.expect(token.LPAREN) {
		return nil
	}

	// Check if it's EXTRACT(field FROM expr) form
	// The field must be a known date/time field identifier followed by FROM
	if p.currentIs(token.IDENT) && !p.peekIs(token.LPAREN) {
		field := strings.ToUpper(p.current.Value)
		// Check if it's a known date/time field
		dateTimeFields := map[string]bool{
			"YEAR": true, "YYYY": true, "QUARTER": true, "MONTH": true, "WEEK": true,
			"DAY": true, "DAYOFWEEK": true, "DAYOFYEAR": true,
			"HOUR": true, "MINUTE": true, "SECOND": true,
			"TIMEZONE_HOUR": true, "TIMEZONE_MINUTE": true,
		}
		if dateTimeFields[field] {
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
			// Not FROM, so create args starting with the field as identifier
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
	}

	// Parse as regular function call - extract(str, pattern) regex form
	// or extract(expr, pattern) where expr can be any expression
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

	// Use ALIAS_PREC to prevent consuming the unit as an alias
	expr.Value = p.parseExpression(ALIAS_PREC)

	// Handle INTERVAL '2' AS n minute - where AS n is alias on the value
	if p.currentIs(token.AS) {
		p.nextToken() // skip AS
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			alias := p.current.Value
			p.nextToken()
			expr.Value = p.wrapWithAlias(expr.Value, alias)
		}
	}

	// Parse unit (interval units are identifiers like DAY, MONTH, etc.)
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

	// Parse first argument (source string) - may have alias before FROM
	// Use ALIAS_PREC to not consume AS
	firstArg := p.parseExpression(ALIAS_PREC)

	// Check for alias on first argument (AS alias or just alias before FROM)
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			alias := p.current.Value
			p.nextToken()
			firstArg = p.wrapWithAlias(firstArg, alias)
		}
	} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && (p.peekIs(token.FROM) || p.peekIs(token.COMMA)) {
		// Implicit alias before FROM or COMMA
		alias := p.current.Value
		p.nextToken()
		firstArg = p.wrapWithAlias(firstArg, alias)
	}

	args := []ast.Expression{firstArg}

	// Handle FROM or COMMA for second argument
	if p.currentIs(token.FROM) {
		p.nextToken()
		// Parse start position - may have alias before FOR or )
		startArg := p.parseExpression(ALIAS_PREC)
		// Check for alias
		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				alias := p.current.Value
				p.nextToken()
				startArg = p.wrapWithAlias(startArg, alias)
			}
		} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && (p.peekIs(token.FOR) || p.peekIs(token.RPAREN)) {
			alias := p.current.Value
			p.nextToken()
			startArg = p.wrapWithAlias(startArg, alias)
		}
		args = append(args, startArg)
	} else if p.currentIs(token.COMMA) {
		p.nextToken()
		// Parse second argument with possible alias
		startArg := p.parseExpression(ALIAS_PREC)
		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				alias := p.current.Value
				p.nextToken()
				startArg = p.wrapWithAlias(startArg, alias)
			}
		} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && (p.peekIs(token.COMMA) || p.peekIs(token.RPAREN)) {
			alias := p.current.Value
			p.nextToken()
			startArg = p.wrapWithAlias(startArg, alias)
		}
		args = append(args, startArg)
	}

	// Handle FOR or COMMA for third argument
	if p.currentIs(token.FOR) {
		p.nextToken()
		// Parse length - may have alias before )
		lenArg := p.parseExpression(ALIAS_PREC)
		// Check for alias
		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				alias := p.current.Value
				p.nextToken()
				lenArg = p.wrapWithAlias(lenArg, alias)
			}
		} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.peekIs(token.RPAREN) {
			alias := p.current.Value
			p.nextToken()
			lenArg = p.wrapWithAlias(lenArg, alias)
		}
		args = append(args, lenArg)
	} else if p.currentIs(token.COMMA) {
		p.nextToken()
		// Parse third argument with possible alias
		lenArg := p.parseExpression(ALIAS_PREC)
		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				alias := p.current.Value
				p.nextToken()
				lenArg = p.wrapWithAlias(lenArg, alias)
			}
		} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.peekIs(token.RPAREN) {
			alias := p.current.Value
			p.nextToken()
			lenArg = p.wrapWithAlias(lenArg, alias)
		}
		args = append(args, lenArg)
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
	// Use ALIAS_PREC to not consume AS as alias
	if !p.currentIs(token.FROM) && !p.currentIs(token.RPAREN) {
		trimChars = p.parseExpression(ALIAS_PREC)
		// Check for alias on trimChars
		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				alias := p.current.Value
				p.nextToken()
				trimChars = p.wrapWithAlias(trimChars, alias)
			}
		} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.peekIs(token.FROM) {
			alias := p.current.Value
			p.nextToken()
			trimChars = p.wrapWithAlias(trimChars, alias)
		}
	}

	// FROM clause
	var expr ast.Expression
	if p.currentIs(token.FROM) {
		p.nextToken()
		// Parse expression with possible alias
		expr = p.parseExpression(ALIAS_PREC)
		// Check for alias
		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				alias := p.current.Value
				p.nextToken()
				expr = p.wrapWithAlias(expr, alias)
			}
		} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.peekIs(token.RPAREN) {
			alias := p.current.Value
			p.nextToken()
			expr = p.wrapWithAlias(expr, alias)
		}
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
	case "BOTH":
		fnName = "trimBoth"
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

	// Check for ANY/ALL subquery comparison modifier: expr >= ANY(subquery)
	if p.currentIs(token.ANY) || p.currentIs(token.ALL) {
		modifier := strings.ToLower(p.current.Value)
		p.nextToken()
		if p.currentIs(token.LPAREN) {
			p.nextToken()
			// Parse the subquery
			if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
				subquery := p.parseSelectWithUnion()
				p.expect(token.RPAREN)
				// Create function name that encodes both modifier and operator
				// e.g., anyEquals, allLess, anyGreaterOrEquals, etc.
				opName := operatorToName(expr.Op)
				fnName := modifier + opName
				return &ast.FunctionCall{
					Position: expr.Position,
					Name:     fnName,
					Arguments: []ast.Expression{
						left,
						&ast.Subquery{Position: expr.Position, Query: subquery},
					},
				}
			}
			// Not a subquery, parse as expression list
			args := p.parseExpressionList()
			p.expect(token.RPAREN)
			return &ast.BinaryExpr{
				Position: expr.Position,
				Left:     left,
				Op:       expr.Op,
				Right: &ast.FunctionCall{
					Position:  expr.Position,
					Name:      strings.ToLower(modifier),
					Arguments: args,
				},
			}
		}
	}

	expr.Right = p.parseExpression(prec)
	return expr
}

// operatorToName converts a comparison operator to a capitalized name for use
// in ANY/ALL function names (e.g., "==" -> "Equals", "<" -> "Less")
func operatorToName(op string) string {
	switch op {
	case "=", "==":
		return "Equals"
	case "!=", "<>":
		return "NotEquals"
	case "<":
		return "Less"
	case "<=":
		return "LessOrEquals"
	case ">":
		return "Greater"
	case ">=":
		return "GreaterOrEquals"
	default:
		return "Equals" // fallback
	}
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

	// IS [NOT] DISTINCT FROM expr
	if p.currentIs(token.DISTINCT) {
		p.nextToken() // skip DISTINCT
		if p.currentIs(token.FROM) {
			p.nextToken() // skip FROM
			right := p.parseExpression(COMPARE)
			// IS NOT DISTINCT FROM is same as =, IS DISTINCT FROM is same as !=
			op := "="
			if not {
				op = "!="
			}
			return &ast.BinaryExpr{
				Position: pos,
				Left:     left,
				Op:       op,
				Right:    right,
			}
		}
	}

	return left
}

func (p *Parser) parseArrayAccess(left ast.Expression) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip [

	// Check for empty brackets [] - this is JSON array path notation
	// json.arr[].field becomes Identifier json.arr.:`Array(JSON)`.field
	if p.currentIs(token.RBRACKET) {
		p.nextToken() // skip ]

		if ident, ok := left.(*ast.Identifier); ok {
			// Append the JSON array type notation to the identifier
			ident.Parts = append(ident.Parts, ":`Array(JSON)`")

			// Continue parsing any dot accesses that follow
			for p.currentIs(token.DOT) {
				p.nextToken() // skip .

				if p.currentIs(token.CARET) {
					// Handle JSON path parent access: x.^c0
					p.nextToken() // skip ^
					if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
						ident.Parts = append(ident.Parts, "^"+p.current.Value)
						p.nextToken()
					} else {
						break
					}
				} else if p.currentIs(token.COLON) {
					// JSON subcolumn type accessor: json.field.:`TypeName`
					p.nextToken() // skip :
					typePart := ":"
					if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() || p.currentIs(token.STRING) {
						typePart += "`" + p.current.Value + "`"
						p.nextToken()
					}
					ident.Parts = append(ident.Parts, typePart)
				} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					ident.Parts = append(ident.Parts, p.current.Value)
					p.nextToken()

					// Check for nested empty array access (e.g., arr[].nested[].field)
					if p.currentIs(token.LBRACKET) {
						return p.parseArrayAccess(ident)
					}
				} else {
					break
				}
			}
			return ident
		}

		// Not an identifier, fall through to create ArrayAccess with nil index
		return &ast.ArrayAccess{
			Position: pos,
			Array:    left,
			Index:    nil,
		}
	}

	// Regular array access with index expression
	expr := &ast.ArrayAccess{
		Position: pos,
		Array:    left,
	}
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

	// Check for JSON path parent access with ^ (e.g., x.^c0)
	if p.currentIs(token.CARET) {
		p.nextToken() // skip ^
		if p.currentIs(token.IDENT) {
			pathPart := "^" + p.current.Value
			p.nextToken()
			if ident, ok := left.(*ast.Identifier); ok {
				ident.Parts = append(ident.Parts, pathPart)
				return ident
			}
			// Create new identifier with JSON path
			return &ast.Identifier{
				Position: left.Pos(),
				Parts:    []string{pathPart},
			}
		}
	}

	// Check for tuple access with number
	if p.currentIs(token.NUMBER) {
		expr := &ast.TupleAccess{
			Position: p.current.Pos,
			Tuple:    left,
			Index:    p.parseNumber(),
		}
		return expr
	}

	// Handle JSON caret notation: x.^c0 (traverse into JSON field)
	if p.currentIs(token.CARET) {
		p.nextToken() // skip ^
		if ident, ok := left.(*ast.Identifier); ok {
			// Add ^fieldname as a single part with caret prefix
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				ident.Parts = append(ident.Parts, "^"+p.current.Value)
				p.nextToken()
				return ident
			}
		}
		return left
	}

	// Regular identifier access (keywords can also be column/field names after DOT)
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
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
	case *ast.CastExpr:
		// For :: operator syntax, set alias directly on CastExpr
		// For function-style CAST(), wrap in AliasedExpr
		if e.OperatorSyntax {
			e.Alias = alias
			return e
		}
		return &ast.AliasedExpr{
			Position: left.Pos(),
			Expr:     left,
			Alias:    alias,
		}
	case *ast.CaseExpr:
		e.Alias = alias
		return e
	case *ast.ExtractExpr:
		e.Alias = alias
		return e
	case *ast.LikeExpr:
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

	// Use ALIAS_PREC to prevent consuming AS keyword that might belong to containing context
	// e.g., WITH x -> toString(x) AS lambda_1 SELECT...
	lambda.Body = p.parseExpression(ALIAS_PREC)
	return lambda
}

func (p *Parser) parseTernary(condition ast.Expression) ast.Expression {
	ternary := &ast.TernaryExpr{
		Position:  p.current.Pos,
		Condition: condition,
	}

	p.nextToken() // skip ?

	// Use ALIAS_PREC to prevent consuming AS keyword, but still allow nested ternaries
	ternary.Then = p.parseExpression(ALIAS_PREC)

	if !p.expect(token.COLON) {
		return nil
	}

	// Use ALIAS_PREC to prevent consuming AS keyword that might belong to containing context
	// e.g., WITH cond ? a : b AS x SELECT...
	ternary.Else = p.parseExpression(ALIAS_PREC)

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
	// Can appear multiple times (e.g., RESPECT NULLS IGNORE NULLS)
	for p.currentIs(token.IDENT) {
		upper := strings.ToUpper(p.current.Value)
		if upper == "IGNORE" || upper == "RESPECT" {
			p.nextToken()
			if p.currentIs(token.NULLS) {
				p.nextToken()
			}
		} else {
			break
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

	// Parse the arguments - either a string pattern or a list of identifiers
	if p.currentIs(token.STRING) {
		// String pattern: COLUMNS('pattern')
		matcher.Pattern = p.current.Value
		p.nextToken()
	} else {
		// Column list: COLUMNS(col1, col2, ...)
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			col := p.parseExpression(LOWEST)
			if col != nil {
				matcher.Columns = append(matcher.Columns, col)
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
	}

	p.expect(token.RPAREN)

	// EXCEPT, REPLACE, and APPLY are now handled via infix parsing
	// to preserve transformer ordering

	return matcher
}

// parseQualifiedColumnsMatcher parses qualified COLUMNS matchers like test_table.COLUMNS(id)
// The qualifier is passed in and we're already positioned at LPAREN
func (p *Parser) parseQualifiedColumnsMatcher(qualifier string, pos token.Position) ast.Expression {
	matcher := &ast.ColumnsMatcher{
		Position:  pos,
		Qualifier: qualifier,
	}

	p.nextToken() // skip LPAREN

	// Parse the arguments - either a string pattern or a list of identifiers
	if p.currentIs(token.STRING) {
		// String pattern: COLUMNS('pattern')
		matcher.Pattern = p.current.Value
		p.nextToken()
	} else {
		// Column list: COLUMNS(col1, col2, ...)
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			col := p.parseExpression(LOWEST)
			if col != nil {
				matcher.Columns = append(matcher.Columns, col)
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
	}

	p.expect(token.RPAREN)

	// EXCEPT, REPLACE, and APPLY are now handled via infix parsing
	// to preserve transformer ordering

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

	fn := &ast.FunctionCall{
		Position: pos,
		Name:     name,
	}

	// Handle DISTINCT
	if p.currentIs(token.DISTINCT) {
		fn.Distinct = true
		p.nextToken()
	}

	// Handle view() and similar functions that take a subquery as argument
	if name == "view" && (p.currentIs(token.SELECT) || p.currentIs(token.WITH)) {
		subquery := p.parseSelectWithUnion()
		fn.Arguments = []ast.Expression{&ast.Subquery{Position: pos, Query: subquery}}
	} else if !p.currentIs(token.RPAREN) {
		fn.Arguments = p.parseExpressionList()
	}

	p.expect(token.RPAREN)

	// Handle IGNORE NULLS / RESPECT NULLS (window function modifiers)
	for p.currentIs(token.IDENT) {
		upper := strings.ToUpper(p.current.Value)
		if upper == "IGNORE" || upper == "RESPECT" {
			p.nextToken()
			if p.currentIs(token.NULLS) {
				p.nextToken()
			}
		} else {
			break
		}
	}

	// Handle FILTER clause for aggregate functions: func() FILTER(WHERE condition)
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "FILTER" {
		p.nextToken() // skip FILTER
		if p.currentIs(token.LPAREN) {
			p.nextToken() // skip (
			if p.currentIs(token.WHERE) {
				p.nextToken() // skip WHERE
				p.parseExpression(LOWEST)
			}
			p.expect(token.RPAREN)
		}
	}

	// Handle OVER clause for window functions
	if p.currentIs(token.OVER) {
		p.nextToken()
		fn.Over = p.parseWindowSpec()
	}

	return fn
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
	pos := p.current.Pos
	p.nextToken() // skip EXCEPT

	// EXCEPT can have optional parentheses: * EXCEPT (col1, col2) or * EXCEPT col
	hasParens := p.currentIs(token.LPAREN)
	if hasParens {
		p.nextToken() // skip (
	}

	var exceptCols []string
	// Parse column names (can be IDENT or keywords)
	for {
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			exceptCols = append(exceptCols, p.current.Value)
			asterisk.Except = append(asterisk.Except, p.current.Value)
			p.nextToken()
		}

		if hasParens && p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	if len(exceptCols) > 0 {
		asterisk.Transformers = append(asterisk.Transformers, &ast.ColumnTransformer{
			Position: pos,
			Type:     "except",
			Except:   exceptCols,
		})
	}

	if hasParens {
		p.expect(token.RPAREN)
	}

	return asterisk
}

func (p *Parser) parseAsteriskReplace(asterisk *ast.Asterisk) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip REPLACE

	// REPLACE can have optional parentheses: REPLACE (expr AS col) or REPLACE expr AS col
	hasParens := p.currentIs(token.LPAREN)
	if hasParens {
		p.nextToken()
	}

	var replaces []*ast.ReplaceExpr
	for {
		// Stop conditions based on context
		if hasParens && p.currentIs(token.RPAREN) {
			break
		}
		if !hasParens && (p.currentIs(token.FROM) || p.currentIs(token.WHERE) || p.currentIs(token.EOF) ||
			p.currentIs(token.GROUP) || p.currentIs(token.ORDER) || p.currentIs(token.HAVING) ||
			p.currentIs(token.LIMIT) || p.currentIs(token.SETTINGS) || p.currentIs(token.FORMAT) ||
			p.currentIs(token.UNION) || p.currentIs(token.EXCEPT) || p.currentIs(token.COMMA)) {
			break
		}

		replace := &ast.ReplaceExpr{
			Position: p.current.Pos,
		}

		replace.Expr = p.parseExpression(ALIAS_PREC)

		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) {
				replace.Name = p.current.Value
				p.nextToken()
			}
		}

		asterisk.Replace = append(asterisk.Replace, replace)
		replaces = append(replaces, replace)

		if p.currentIs(token.COMMA) {
			p.nextToken()
			// If no parens and we see comma, might be end of select column
			if !hasParens {
				break
			}
		} else if !hasParens {
			break
		}
	}

	if len(replaces) > 0 {
		asterisk.Transformers = append(asterisk.Transformers, &ast.ColumnTransformer{
			Position: pos,
			Type:     "replace",
			Replaces: replaces,
		})
	}

	if hasParens {
		p.expect(token.RPAREN)
	}

	return asterisk
}

func (p *Parser) parseAsteriskApply(asterisk *ast.Asterisk) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip APPLY

	// APPLY can have optional parentheses: * APPLY(func) or * APPLY func
	hasParens := p.currentIs(token.LPAREN)
	if hasParens {
		p.nextToken() // skip (
	}

	// Check for lambda expression: x -> expr
	if p.currentIs(token.IDENT) && p.peekIs(token.ARROW) {
		// Parse lambda expression
		lambda := p.parseExpression(LOWEST)
		asterisk.Transformers = append(asterisk.Transformers, &ast.ColumnTransformer{
			Position:     pos,
			Type:         "apply",
			ApplyLambda:  lambda,
		})
	} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		// Parse function name (can be IDENT or keyword like sum, avg, etc.)
		funcName := p.current.Value
		asterisk.Apply = append(asterisk.Apply, funcName)
		asterisk.Transformers = append(asterisk.Transformers, &ast.ColumnTransformer{
			Position: pos,
			Type:     "apply",
			Apply:    funcName,
		})
		p.nextToken()
	}

	if hasParens {
		p.expect(token.RPAREN)
	}

	return asterisk
}

func (p *Parser) parseColumnsApply(matcher *ast.ColumnsMatcher) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip APPLY

	// APPLY can have optional parentheses: COLUMNS(...) APPLY(func) or COLUMNS(...) APPLY func
	hasParens := p.currentIs(token.LPAREN)
	if hasParens {
		p.nextToken() // skip (
	}

	// Check for lambda expression: x -> expr
	if p.currentIs(token.IDENT) && p.peekIs(token.ARROW) {
		// Parse lambda expression
		lambda := p.parseExpression(LOWEST)
		matcher.Transformers = append(matcher.Transformers, &ast.ColumnTransformer{
			Position:    pos,
			Type:        "apply",
			ApplyLambda: lambda,
		})
	} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		// Parse function name (can be IDENT or keyword like sum, avg, etc.)
		funcName := p.current.Value
		matcher.Apply = append(matcher.Apply, funcName)
		matcher.Transformers = append(matcher.Transformers, &ast.ColumnTransformer{
			Position: pos,
			Type:     "apply",
			Apply:    funcName,
		})
		p.nextToken()
	}

	if hasParens {
		p.expect(token.RPAREN)
	}

	return matcher
}

func (p *Parser) parseColumnsExcept(matcher *ast.ColumnsMatcher) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip EXCEPT

	// EXCEPT can have optional parentheses: COLUMNS(...) EXCEPT (col1, col2) or COLUMNS(...) EXCEPT col
	hasParens := p.currentIs(token.LPAREN)
	if hasParens {
		p.nextToken() // skip (
	}

	var exceptCols []string
	// Parse column names (can be IDENT or keywords)
	for {
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			exceptCols = append(exceptCols, p.current.Value)
			matcher.Except = append(matcher.Except, p.current.Value)
			p.nextToken()
		}

		if hasParens && p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	if len(exceptCols) > 0 {
		matcher.Transformers = append(matcher.Transformers, &ast.ColumnTransformer{
			Position: pos,
			Type:     "except",
			Except:   exceptCols,
		})
	}

	if hasParens {
		p.expect(token.RPAREN)
	}

	return matcher
}

func (p *Parser) parseColumnsReplace(matcher *ast.ColumnsMatcher) ast.Expression {
	pos := p.current.Pos
	p.nextToken() // skip REPLACE

	// Check for STRICT modifier
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "STRICT" {
		p.nextToken()
	}

	// REPLACE can have optional parentheses: REPLACE (expr AS col) or REPLACE expr AS col
	hasParens := p.currentIs(token.LPAREN)
	if hasParens {
		p.nextToken()
	}

	var replaces []*ast.ReplaceExpr
	for {
		// Stop conditions based on context
		if hasParens && p.currentIs(token.RPAREN) {
			break
		}
		if !hasParens && (p.currentIs(token.FROM) || p.currentIs(token.WHERE) || p.currentIs(token.EOF) ||
			p.currentIs(token.GROUP) || p.currentIs(token.ORDER) || p.currentIs(token.HAVING) ||
			p.currentIs(token.LIMIT) || p.currentIs(token.SETTINGS) || p.currentIs(token.FORMAT) ||
			p.currentIs(token.UNION) || p.currentIs(token.EXCEPT) || p.currentIs(token.COMMA) ||
			p.currentIs(token.APPLY)) {
			break
		}

		replace := &ast.ReplaceExpr{
			Position: p.current.Pos,
		}

		replace.Expr = p.parseExpression(ALIAS_PREC)

		if p.currentIs(token.AS) {
			p.nextToken()
			if p.currentIs(token.IDENT) {
				replace.Name = p.current.Value
				p.nextToken()
			}
		}

		matcher.Replace = append(matcher.Replace, replace)
		replaces = append(replaces, replace)

		if p.currentIs(token.COMMA) {
			p.nextToken()
			// If no parens and we see comma, might be end of select column
			if !hasParens {
				break
			}
		} else if !hasParens {
			break
		}
	}

	if len(replaces) > 0 {
		matcher.Transformers = append(matcher.Transformers, &ast.ColumnTransformer{
			Position: pos,
			Type:     "replace",
			Replaces: replaces,
		})
	}

	if hasParens {
		p.expect(token.RPAREN)
	}

	return matcher
}
