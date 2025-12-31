// Package parser implements a parser for ClickHouse SQL.
package parser

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
	"github.com/sqlc-dev/doubleclick/lexer"
	"github.com/sqlc-dev/doubleclick/token"
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
		// Skip whitespace and comments
		if p.peek.Token == token.WHITESPACE || p.peek.Token == token.LINE_COMMENT {
			continue
		}
		break
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

		// Skip leading semicolons (empty statements)
		for p.currentIs(token.SEMICOLON) {
			p.nextToken()
		}
		if p.currentIs(token.EOF) {
			break
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
	case token.FROM:
		// FROM ... SELECT syntax (ClickHouse extension)
		return p.parseFromSelectSyntax()
	case token.LPAREN:
		// Parenthesized SELECT at statement level: (SELECT 1)
		return p.parseParenthesizedSelect()
	case token.INSERT:
		return p.parseInsert()
	case token.CREATE:
		return p.parseCreate()
	case token.DROP:
		// Check for DROP SETTINGS PROFILE
		if p.peekIs(token.SETTINGS) {
			return p.parseDropSettingsProfile()
		}
		// Check for DROP PROFILE
		if p.peek.Token == token.IDENT && strings.ToUpper(p.peek.Value) == "PROFILE" {
			return p.parseDropSettingsProfile()
		}
		// Check for DROP ROW POLICY or DROP POLICY
		if p.peek.Token == token.IDENT && (strings.ToUpper(p.peek.Value) == "ROW" || strings.ToUpper(p.peek.Value) == "POLICY") {
			return p.parseDropRowPolicy()
		}
		// Check for DROP ROLE
		if p.peek.Token == token.IDENT && strings.ToUpper(p.peek.Value) == "ROLE" {
			return p.parseDropRole()
		}
		// Check for DROP RESOURCE
		if p.peek.Token == token.IDENT && strings.ToUpper(p.peek.Value) == "RESOURCE" {
			return p.parseDropResource()
		}
		// Check for DROP WORKLOAD
		if p.peek.Token == token.IDENT && strings.ToUpper(p.peek.Value) == "WORKLOAD" {
			return p.parseDropWorkload()
		}
		return p.parseDrop()
	case token.ALTER:
		// Check for ALTER USER
		if p.peekIs(token.USER) {
			return p.parseAlterUser()
		}
		// Check for ALTER SETTINGS PROFILE
		if p.peekIs(token.SETTINGS) {
			return p.parseAlterSettingsProfile()
		}
		// Check for ALTER PROFILE
		if p.peek.Token == token.IDENT && strings.ToUpper(p.peek.Value) == "PROFILE" {
			return p.parseAlterSettingsProfile()
		}
		// Check for ALTER ROW POLICY or ALTER POLICY
		if p.peek.Token == token.IDENT && (strings.ToUpper(p.peek.Value) == "ROW" || strings.ToUpper(p.peek.Value) == "POLICY") {
			return p.parseAlterRowPolicy()
		}
		// Check for ALTER ROLE
		if p.peek.Token == token.IDENT && strings.ToUpper(p.peek.Value) == "ROLE" {
			return p.parseAlterRole()
		}
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
		// Check for SET TRANSACTION SNAPSHOT
		if p.peekIs(token.TRANSACTION) {
			return p.parseTransactionControl()
		}
		// Check for SET DEFAULT ROLE
		if p.peekIs(token.DEFAULT) {
			return p.parseSetRole()
		}
		return p.parseSet()
	case token.OPTIMIZE:
		return p.parseOptimize()
	case token.SYSTEM:
		return p.parseSystem()
	case token.RENAME:
		return p.parseRename()
	case token.EXCHANGE:
		return p.parseExchange()
	case token.EXISTS:
		// EXISTS table_name syntax (check if table exists)
		return p.parseExistsStatement()
	case token.DETACH:
		return p.parseDetach()
	case token.ATTACH:
		return p.parseAttach()
	case token.CHECK:
		return p.parseCheck()
	case token.GRANT:
		return p.parseGrant()
	case token.REVOKE:
		return p.parseRevoke()
	case token.BEGIN:
		return p.parseTransactionControl()
	case token.COMMIT:
		return p.parseTransactionControl()
	case token.ROLLBACK:
		return p.parseTransactionControl()
	default:
		p.errors = append(p.errors, fmt.Errorf("unexpected token %s at line %d, column %d",
			p.current.Token, p.current.Pos.Line, p.current.Pos.Column))
		p.nextToken()
		return nil
	}
}

// parseSelectWithUnion parses SELECT ... UNION/INTERSECT/EXCEPT ... queries
func (p *Parser) parseSelectWithUnion() *ast.SelectWithUnionQuery {
	query := &ast.SelectWithUnionQuery{
		Position: p.current.Pos,
	}

	// Parse first select item (could be parenthesized or direct SELECT)
	var firstItem ast.Statement
	var firstWasParenthesized bool

	if p.currentIs(token.LPAREN) {
		firstWasParenthesized = true
		p.nextToken() // skip (
		nested := p.parseSelectWithUnion()
		if nested == nil {
			return nil
		}
		p.expect(token.RPAREN)
		firstItem = nested
	} else {
		// Parse first SELECT
		sel := p.parseSelect()
		if sel == nil {
			return nil
		}
		firstItem = sel
	}

	// Check if this is INTERSECT/EXCEPT that needs SelectIntersectExceptQuery wrapper
	// Only INTERSECT ALL and EXCEPT ALL are treated like UNION ALL (flattened into ExpressionList)
	if p.isIntersectExceptWithWrapper() {
		// Collect all operands and operators first, then apply precedence
		// INTERSECT has higher precedence than EXCEPT

		// Start with first operand (statements list) and operators list
		stmts := []ast.Statement{firstItem}
		var ops []string

		// Parse all INTERSECT/EXCEPT clauses and collect them
		for p.isIntersectExceptWithWrapper() {
			// Record the operator type
			var op string
			if p.currentIs(token.EXCEPT) {
				op = "EXCEPT"
			} else {
				op = "INTERSECT"
			}
			p.nextToken() // skip INTERSECT/EXCEPT

			// Handle DISTINCT if present (ALL case is handled in the loop condition)
			if p.currentIs(token.DISTINCT) {
				op += " DISTINCT"
				p.nextToken()
			}
			ops = append(ops, op)

			// Parse the next select
			var nextStmt ast.Statement
			if p.currentIs(token.LPAREN) {
				p.nextToken() // skip (
				nested := p.parseSelectWithUnion()
				if nested == nil {
					break
				}
				p.expect(token.RPAREN)
				nextStmt = nested
			} else {
				sel := p.parseSelect()
				if sel == nil {
					break
				}
				nextStmt = sel
			}
			stmts = append(stmts, nextStmt)
		}

		// Now apply precedence: INTERSECT binds tighter than EXCEPT
		result := buildIntersectExceptTree(stmts, ops)

		query.Selects = append(query.Selects, result)
		return query
	}

	// Handle regular case (UNION, INTERSECT ALL, EXCEPT ALL, or single SELECT)
	if firstWasParenthesized {
		if nested, ok := firstItem.(*ast.SelectWithUnionQuery); ok {
			for _, s := range nested.Selects {
				query.Selects = append(query.Selects, s)
			}
		}
	} else {
		query.Selects = append(query.Selects, firstItem)
	}

	// Parse UNION/INTERSECT ALL/EXCEPT ALL clauses
	for p.currentIs(token.UNION) || p.currentIs(token.EXCEPT) || p.currentIs(token.INTERSECT) {
		var setOp string
		if p.currentIs(token.UNION) {
			setOp = "UNION"
		} else if p.currentIs(token.EXCEPT) {
			setOp = "EXCEPT"
		} else {
			setOp = "INTERSECT"
		}
		p.nextToken() // skip UNION/INTERSECT/EXCEPT

		var mode string
		if p.currentIs(token.ALL) {
			query.UnionAll = true
			mode = "ALL"
			p.nextToken()
		} else if p.currentIs(token.DISTINCT) {
			mode = "DISTINCT"
			p.nextToken()
		}
		query.UnionModes = append(query.UnionModes, setOp+" "+mode)

		// Handle parenthesized subqueries
		if p.currentIs(token.LPAREN) {
			p.nextToken() // skip (
			nested := p.parseSelectWithUnion()
			if nested == nil {
				break
			}
			p.expect(token.RPAREN)
			// Flatten nested selects into current query
			for _, s := range nested.Selects {
				query.Selects = append(query.Selects, s)
			}
		} else {
			sel := p.parseSelect()
			if sel == nil {
				break
			}
			query.Selects = append(query.Selects, sel)
		}
	}

	return query
}

// isIntersectExceptWithWrapper checks if the current token is INTERSECT or EXCEPT
// that should use a SelectIntersectExceptQuery wrapper.
// Only INTERSECT ALL and EXCEPT ALL are flattened (no wrapper).
// INTERSECT DISTINCT, INTERSECT, EXCEPT DISTINCT, and EXCEPT all use the wrapper.
func (p *Parser) isIntersectExceptWithWrapper() bool {
	if !p.currentIs(token.EXCEPT) && !p.currentIs(token.INTERSECT) {
		return false
	}
	// INTERSECT ALL and EXCEPT ALL are flattened (no wrapper)
	// All other cases (DISTINCT or no modifier) use the wrapper
	nextTok := p.peek.Token
	return nextTok != token.ALL
}

// isIntersectOp checks if the operator is an INTERSECT variant (not EXCEPT)
func isIntersectOp(op string) bool {
	return op == "INTERSECT" || op == "INTERSECT DISTINCT"
}

// buildIntersectExceptTree builds the AST tree respecting operator precedence.
// INTERSECT has higher precedence than EXCEPT, so:
// "a EXCEPT b INTERSECT c" becomes "a EXCEPT (b INTERSECT c)"
// "a INTERSECT b EXCEPT c" becomes "(a INTERSECT b) EXCEPT c"
//
// EXCEPT is left-associative and creates binary trees:
// "a EXCEPT b EXCEPT c" becomes "((a) EXCEPT b) EXCEPT c"
//
// stmts has n elements, ops has n-1 elements where ops[i] is the operator between stmts[i] and stmts[i+1]
func buildIntersectExceptTree(stmts []ast.Statement, ops []string) ast.Statement {
	if len(stmts) == 1 {
		return stmts[0]
	}

	// First pass: group consecutive INTERSECT operations (higher precedence)
	// Result will be a list of statements/groups connected by EXCEPT operators
	var groups []ast.Statement
	var exceptOps []string

	i := 0
	for i < len(stmts) {
		// Start a new group with stmts[i]
		groupStmts := []ast.Statement{stmts[i]}
		var groupOps []string

		// Collect consecutive INTERSECTs - look at the operator AFTER the current statement
		for i < len(ops) && isIntersectOp(ops[i]) {
			groupOps = append(groupOps, ops[i])
			i++
			groupStmts = append(groupStmts, stmts[i])
		}

		// Create the group
		var groupStmt ast.Statement
		if len(groupStmts) == 1 {
			// Single statement, no grouping needed
			groupStmt = groupStmts[0]
		} else {
			// Multiple statements connected by INTERSECT
			groupStmt = &ast.SelectIntersectExceptQuery{
				Selects:   groupStmts,
				Operators: groupOps,
			}
		}
		groups = append(groups, groupStmt)

		// Check if there's an EXCEPT connecting to the next group
		if i < len(ops) && !isIntersectOp(ops[i]) {
			exceptOps = append(exceptOps, ops[i])
			i++ // Move past the EXCEPT operator to start next group
		} else if i < len(stmts)-1 {
			// No more operators but still have stmts - shouldn't happen with valid input
			i++
		} else {
			// We've processed all statements
			break
		}
	}

	// Now all groups are connected by EXCEPT - build the final tree
	if len(groups) == 1 {
		return groups[0]
	}

	// Build left-associative binary tree for EXCEPT operations
	// "a EXCEPT b EXCEPT c" becomes "((a) EXCEPT b) EXCEPT c"
	result := groups[0]
	for j := 0; j < len(exceptOps); j++ {
		result = &ast.SelectIntersectExceptQuery{
			Selects:   []ast.Statement{result, groups[j+1]},
			Operators: []string{exceptOps[j]},
		}
	}
	return result
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

	// Handle DISTINCT or ALL
	if p.currentIs(token.DISTINCT) {
		sel.Distinct = true
		p.nextToken()
	} else if p.currentIs(token.ALL) {
		// ALL is the default, just skip it
		p.nextToken()
	}

	// Handle TOP
	if p.currentIs(token.TOP) {
		p.nextToken()
		// Use MUL_PREC to stop at * (which would be parsed as column selector, not multiplication)
		sel.Top = p.parseExpression(MUL_PREC)
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

		// Handle GROUPING SETS, ROLLUP(...), CUBE(...) as special expressions
		if p.currentIs(token.GROUPING) && p.peekIs(token.SETS) {
			// GROUPING SETS ((a), (b), (a, b))
			p.nextToken() // skip GROUPING
			p.nextToken() // skip SETS
			sel.GroupBy = p.parseGroupingSets()
			sel.GroupingSets = true
		} else if p.currentIs(token.ROLLUP) && p.peekIs(token.LPAREN) {
			// ROLLUP(a, b, c)
			p.nextToken() // skip ROLLUP
			p.nextToken() // skip (
			sel.GroupBy = p.parseExpressionList()
			p.expect(token.RPAREN)
			sel.WithRollup = true
		} else if p.currentIs(token.CUBE) && p.peekIs(token.LPAREN) {
			// CUBE(a, b, c)
			p.nextToken() // skip CUBE
			p.nextToken() // skip (
			sel.GroupBy = p.parseExpressionList()
			p.expect(token.RPAREN)
			sel.WithCube = true
		} else if p.currentIs(token.ALL) {
			// GROUP BY ALL - special ClickHouse syntax
			sel.GroupByAll = true
			sel.GroupBy = p.parseExpressionList() // Still parse it, but mark as GroupByAll
		} else {
			sel.GroupBy = p.parseExpressionList()
		}

		// WITH ROLLUP
		if p.currentIs(token.WITH) && p.peekIs(token.ROLLUP) {
			p.nextToken()
			p.nextToken()
			sel.WithRollup = true
		}

		// WITH CUBE
		if p.currentIs(token.WITH) && p.peekIs(token.CUBE) {
			p.nextToken()
			p.nextToken()
			sel.WithCube = true
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

	// Parse QUALIFY clause (window function filter)
	if p.currentIs(token.QUALIFY) {
		p.nextToken()
		sel.Qualify = p.parseExpression(LOWEST)
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
			// Parse LIMIT BY expressions
			for !p.isEndOfExpression() {
				expr := p.parseExpression(LOWEST)
				sel.LimitBy = append(sel.LimitBy, expr)
				if p.currentIs(token.COMMA) {
					p.nextToken()
				} else {
					break
				}
			}
			// After LIMIT BY, there can be another LIMIT for overall output
			if p.currentIs(token.LIMIT) {
				p.nextToken()
				// Save the LIMIT BY limit value (e.g., LIMIT 1 BY x -> LimitByLimit=1)
				sel.LimitByLimit = sel.Limit
				sel.Limit = p.parseExpression(LOWEST)
				sel.LimitByHasLimit = true
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
		// Skip optional ROWS keyword
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROWS" {
			p.nextToken()
		}
		// LIMIT n OFFSET m BY expr syntax - handle BY after OFFSET
		if p.currentIs(token.BY) && sel.Limit != nil && len(sel.LimitBy) == 0 {
			p.nextToken()
			// Parse LIMIT BY expressions
			for !p.isEndOfExpression() {
				expr := p.parseExpression(LOWEST)
				sel.LimitBy = append(sel.LimitBy, expr)
				if p.currentIs(token.COMMA) {
					p.nextToken()
				} else {
					break
				}
			}
		}
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

	// Parse WITH TOTALS (can appear after GROUP BY or at end of SELECT)
	if p.currentIs(token.WITH) && p.peekIs(token.TOTALS) {
		p.nextToken()
		p.nextToken()
		sel.WithTotals = true
		// HAVING can follow WITH TOTALS
		if p.currentIs(token.HAVING) {
			p.nextToken()
			sel.Having = p.parseExpression(LOWEST)
		}
	}

	// Parse QUALIFY clause (window function filtering)
	if p.currentIs(token.QUALIFY) {
		p.nextToken()
		sel.Qualify = p.parseExpression(LOWEST)
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
				// Parse optional TRUNCATE
				if p.currentIs(token.TRUNCATE) {
					sel.IntoOutfile.Truncate = true
					p.nextToken()
				}
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
		// Skip any inline data after FORMAT (e.g., FORMAT JSONEachRow {"x": 1}, {"y": 2})
		// This can happen in INSERT ... SELECT ... FORMAT ... statements
		for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.SETTINGS) {
			p.nextToken()
		}
	}

	// Parse SETTINGS clause (can come after FORMAT)
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		sel.Settings = p.parseSettingsList()
		sel.SettingsAfterFormat = true
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
			// Need to look ahead to determine: if IDENT AS LPAREN (SELECT...) -> CTE
			// If IDENT AS IDENT -> scalar WITH (first ident is expression, second is alias)
			name := p.current.Value
			pos := p.current.Pos
			p.nextToken() // skip identifier
			p.nextToken() // skip AS

			if p.currentIs(token.LPAREN) {
				// Could be CTE: name AS (subquery) OR could be name AS (expr)
				p.nextToken()
				if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
					// Standard CTE: name AS (SELECT...)
					subquery := p.parseSelectWithUnion()
					if !p.expect(token.RPAREN) {
						return nil
					}
					elem.Name = name
					elem.Query = &ast.Subquery{Query: subquery}
				} else {
					// It's an expression in parentheses, use name as alias
					// e.g., WITH x AS (1 + 2)
					expr := p.parseExpression(LOWEST)
					p.expect(token.RPAREN)
					elem.Name = name
					elem.Query = expr
				}
			} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				// Scalar: IDENT AS IDENT (e.g., WITH number AS k)
				// The first identifier is a column reference, second is the alias
				alias := p.current.Value
				p.nextToken()
				elem.Name = alias
				elem.Query = &ast.Identifier{Position: pos, Parts: []string{name}}
			} else {
				// Scalar expression where the first identifier is used directly
				// This is likely "name AS name" which means the CTE name is name with scalar value name
				elem.Name = name
				elem.Query = &ast.Identifier{Position: pos, Parts: []string{name}}
			}
		} else if p.currentIs(token.LPAREN) && (p.peekIs(token.SELECT) || p.peekIs(token.WITH)) {
			// Subquery: (SELECT ...) AS name or (WITH ... SELECT ...) AS name
			// In this syntax, the alias goes on the Subquery, not on WithElement
			p.nextToken()
			subquery := p.parseSelectWithUnion()
			if !p.expect(token.RPAREN) {
				return nil
			}
			sq := &ast.Subquery{Query: subquery}

			if !p.expect(token.AS) {
				return nil
			}

			// Alias can be IDENT or certain keywords (VALUES, KEY, etc.)
			// Set alias on the Subquery for "(subquery) AS name" syntax
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				sq.Alias = p.current.Value
				p.nextToken()
			}
			elem.Query = sq
		} else {
			// Scalar WITH: expr AS name (ClickHouse style)
			// Examples: WITH 1 AS x, WITH 'hello' AS s, WITH func() AS f
			// Also handles lambda: WITH x -> toString(x) AS lambda_1
			// Arrow has OR_PREC precedence, so it gets parsed with ALIAS_PREC
			// Note: AS name is optional in ClickHouse, e.g., WITH 1 SELECT 1 is valid
			elem.Query = p.parseExpression(ALIAS_PREC) // Use ALIAS_PREC to stop before AS

			// AS name is optional
			if p.currentIs(token.AS) {
				p.nextToken()
				if p.currentIs(token.IDENT) {
					elem.Name = p.current.Value
					p.nextToken()
				}
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
	switch p.current.Token {
	case token.JOIN, token.INNER, token.LEFT, token.RIGHT, token.FULL, token.CROSS,
		token.GLOBAL, token.ANY, token.ALL, token.ASOF, token.SEMI, token.ANTI, token.PASTE,
		token.ARRAY:
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
		// ClickHouse adds an empty TableJoin node for comma joins, but only
		// when the table is NOT a subquery (subqueries don't get TableJoin nodes)
		if elem.Table != nil {
			if _, isSubquery := elem.Table.Table.(*ast.Subquery); !isSubquery {
				elem.Join = &ast.TableJoin{
					Position: elem.Position,
				}
			}
		}
		return elem
	}

	// Handle ARRAY JOIN or LEFT ARRAY JOIN
	if p.currentIs(token.ARRAY) || (p.currentIs(token.LEFT) && p.peekIs(token.ARRAY)) {
		elem.ArrayJoin = p.parseArrayJoin()
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
	case token.PASTE:
		join.Type = ast.JoinPaste
		p.nextToken()
	default:
		join.Type = ast.JoinInner
	}

	// Parse strictness after type if not already parsed (e.g., RIGHT ANTI JOIN)
	if join.Strictness == "" {
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
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) || p.currentIs(token.LPAREN) {
			// SELECT, WITH, or nested (SELECT...) for UNION queries like ((SELECT 1) UNION ALL SELECT 2)
			subquery := p.parseSelectWithUnion()
			expr.Table = &ast.Subquery{Query: subquery}
		} else if p.currentIs(token.EXPLAIN) {
			// EXPLAIN as subquery in FROM clause
			explain := p.parseExplain()
			expr.Table = &ast.Subquery{Query: explain}
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

	// Handle alias (keywords like LEFT, RIGHT, FIRST can be used as aliases after AS,
	// or without AS if they're not clause keywords)
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			expr.Alias = p.current.Value
			p.nextToken()
		}
	} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && !p.isKeywordForClause() {
		expr.Alias = p.current.Value
		p.nextToken()
	}

	return expr
}

func (p *Parser) isKeywordForClause() bool {
	switch p.current.Token {
	case token.WHERE, token.GROUP, token.HAVING, token.QUALIFY, token.ORDER, token.LIMIT,
		token.OFFSET, token.UNION, token.EXCEPT, token.SETTINGS, token.FORMAT,
		token.PREWHERE, token.JOIN, token.LEFT, token.RIGHT, token.INNER,
		token.FULL, token.CROSS, token.PASTE, token.ON, token.USING, token.GLOBAL,
		token.ANY, token.ALL, token.SEMI, token.ANTI, token.ASOF, token.ARRAY,
		token.WINDOW, token.WITH, token.INTERSECT, token.SELECT:
		return true
	}
	// Handle TOTALS as a clause keyword when used in "WITH TOTALS"
	if p.current.Token == token.IDENT && strings.ToUpper(p.current.Value) == "TOTALS" {
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
		// Setting names can be identifiers or keywords (like 'limit')
		if !p.currentIs(token.IDENT) && !p.current.Token.IsKeyword() {
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
			// Use ALIAS_PREC to stop before AS (for AS SELECT in CREATE TABLE AS SELECT)
			setting.Value = p.parseExpression(ALIAS_PREC)
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
		// Check for (*) meaning all columns
		if p.currentIs(token.ASTERISK) {
			ins.AllColumns = true
			p.nextToken()
		} else {
			for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
				pos := p.current.Pos
				colName := p.parseIdentifierName()
				if colName != "" {
					ins.Columns = append(ins.Columns, &ast.Identifier{
						Position: pos,
						Parts:    []string{colName},
					})
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

	// Parse PARTITION BY (for INSERT INTO FUNCTION)
	if p.currentIs(token.PARTITION) {
		p.nextToken()
		if p.currentIs(token.BY) {
			p.nextToken()
			ins.PartitionBy = p.parseExpression(LOWEST)
		}
	}

	// Parse SETTINGS before VALUES
	if p.currentIs(token.SETTINGS) {
		ins.HasSettings = true
		p.nextToken()
		// Just parse and skip the settings
		p.parseSettingsList()
	}

	// Parse FROM INFILE clause (for INSERT ... FROM INFILE '...' COMPRESSION 'gz')
	if p.currentIs(token.FROM) {
		p.nextToken()
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "INFILE" {
			p.nextToken()
			// Store the file path
			if p.currentIs(token.STRING) {
				ins.Infile = p.current.Value
				p.nextToken()
			}
			// Handle COMPRESSION clause
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "COMPRESSION" {
				p.nextToken()
				if p.currentIs(token.STRING) {
					ins.Compression = p.current.Value
					p.nextToken()
				}
			}
		}
	}

	// Parse VALUES or SELECT
	if p.currentIs(token.VALUES) {
		p.nextToken()
		// Parse VALUES rows: (expr, expr, ...), (expr, expr, ...), ...
		for {
			if !p.currentIs(token.LPAREN) {
				break
			}
			p.nextToken() // skip (
			var row []ast.Expression
			for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
				expr := p.parseExpression(LOWEST)
				if expr != nil {
					row = append(row, expr)
				}
				if p.currentIs(token.COMMA) {
					p.nextToken()
				} else {
					break
				}
			}
			if p.currentIs(token.RPAREN) {
				p.nextToken() // skip )
			}
			ins.Values = append(ins.Values, row)
			// Check for more rows
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
	} else if p.currentIs(token.SELECT) || p.currentIs(token.WITH) {
		ins.Select = p.parseSelectWithUnion()
		// If the SELECT has settings, mark the INSERT as having settings too
		if ins.Select != nil {
			if sel, ok := ins.Select.(*ast.SelectWithUnionQuery); ok && sel != nil && len(sel.Selects) > 0 {
				lastSel := sel.Selects[len(sel.Selects)-1]
				if lastSel != nil {
					if selQuery, ok := lastSel.(*ast.SelectQuery); ok && selQuery != nil && len(selQuery.Settings) > 0 {
						ins.HasSettings = true
					}
				}
			}
		}
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
		// Skip any inline data after FORMAT (e.g., FORMAT JSONEachRow {"x": 1}, {"y": 2})
		// The data is raw and should not be parsed as SQL
		for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
			p.nextToken()
		}
	}

	return ins
}

func (p *Parser) parseCreate() ast.Statement {
	pos := p.current.Pos
	p.nextToken() // skip CREATE

	// Handle CREATE [UNIQUE] INDEX
	if p.currentIs(token.INDEX) {
		return p.parseCreateIndex(pos)
	}
	// Handle CREATE UNIQUE INDEX
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "UNIQUE" {
		p.nextToken() // skip UNIQUE
		if p.currentIs(token.INDEX) {
			return p.parseCreateIndex(pos)
		}
	}

	create := &ast.CreateQuery{
		Position: pos,
	}

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
	case token.FUNCTION:
		// CREATE FUNCTION name AS lambda_expr
		create.CreateFunction = true
		p.nextToken()
		p.parseCreateFunction(create)
	case token.USER:
		// CREATE USER name ...
		create.CreateUser = true
		p.nextToken()
		p.parseCreateUser(create)
	case token.SETTINGS:
		// CREATE SETTINGS PROFILE
		return p.parseCreateSettingsProfile(pos)
	case token.IDENT:
		// Handle CREATE DICTIONARY, CREATE RESOURCE, CREATE WORKLOAD, CREATE NAMED COLLECTION, etc.
		identUpper := strings.ToUpper(p.current.Value)
		switch identUpper {
		case "DICTIONARY":
			create.CreateDictionary = true
			p.nextToken()
			p.parseCreateDictionary(create)
		case "NAMED":
			// CREATE NAMED COLLECTION name AS key=value, ...
			p.nextToken() // skip NAMED
			// Skip "COLLECTION" if present
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "COLLECTION" {
				p.nextToken()
			}
			p.parseCreateGeneric(create)
		case "PROFILE":
			// CREATE PROFILE (without SETTINGS keyword)
			return p.parseCreateSettingsProfile(pos)
		case "ROW":
			// CREATE ROW POLICY
			return p.parseCreateRowPolicy(pos)
		case "POLICY":
			// CREATE POLICY (without ROW keyword)
			return p.parseCreateRowPolicy(pos)
		case "ROLE":
			// CREATE ROLE
			return p.parseCreateRole(pos)
		case "RESOURCE":
			// CREATE RESOURCE
			return p.parseCreateResource(pos)
		case "WORKLOAD":
			// CREATE WORKLOAD
			return p.parseCreateWorkload(pos)
		case "QUOTA":
			// CREATE QUOTA
			return p.parseCreateQuota(pos)
		default:
			p.errors = append(p.errors, fmt.Errorf("expected TABLE, DATABASE, VIEW, FUNCTION, USER after CREATE"))
			return nil
		}
	default:
		p.errors = append(p.errors, fmt.Errorf("expected TABLE, DATABASE, VIEW, FUNCTION, USER after CREATE"))
		return nil
	}

	return create
}

func (p *Parser) parseCreateIndex(pos token.Position) *ast.CreateIndexQuery {
	p.nextToken() // skip INDEX

	query := &ast.CreateIndexQuery{
		Position: pos,
	}

	// Parse index name
	query.IndexName = p.parseIdentifierName()

	// Skip IF NOT EXISTS if present
	if p.currentIs(token.IF) {
		p.nextToken() // IF
		if p.currentIs(token.NOT) {
			p.nextToken() // NOT
		}
		if p.currentIs(token.EXISTS) {
			p.nextToken() // EXISTS
		}
	}

	// Expect ON
	if p.currentIs(token.ON) {
		p.nextToken()
	}

	// Parse table name
	query.Table = p.parseIdentifierName()
	if p.currentIs(token.DOT) {
		p.nextToken()
		query.Table = p.parseIdentifierName()
	}

	// Parse column list in parentheses
	if p.currentIs(token.LPAREN) {
		p.nextToken() // skip (

		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			col := p.parseExpression(0)
			query.Columns = append(query.Columns, col)

			// Skip ASC/DESC modifiers
			if p.currentIs(token.ASC) || p.currentIs(token.DESC) {
				p.nextToken()
			}

			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}

		if p.currentIs(token.RPAREN) {
			p.nextToken() // skip )
		}
	}

	return query
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
				idx := p.parseIndexDefinition()
				if idx != nil {
					create.Indexes = append(create.Indexes, idx)
				}
			} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROJECTION" {
				// Parse PROJECTION definitions: PROJECTION name (SELECT ...)
				p.nextToken() // skip PROJECTION
				proj := p.parseProjection()
				if proj != nil {
					create.Projections = append(create.Projections, proj)
				}
			} else if p.currentIs(token.CONSTRAINT) {
				// Parse CONSTRAINT name CHECK (expression)
				p.nextToken() // skip CONSTRAINT
				constraintName := p.parseIdentifierName() // constraint name
				if p.currentIs(token.CHECK) {
					p.nextToken() // skip CHECK
					constraint := &ast.Constraint{
						Position:   p.current.Pos,
						Name:       constraintName,
						Expression: p.parseExpression(LOWEST),
					}
					create.Constraints = append(create.Constraints, constraint)
				} else {
					// Skip other constraint types we don't know about
					for !p.currentIs(token.COMMA) && !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
						p.nextToken()
					}
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
	p.parseTableOptions(create)

	// Parse AS SELECT or AS (subquery) or AS table_function() or AS database.table
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) || p.currentIs(token.LPAREN) {
			// AS SELECT... or AS (SELECT...) INTERSECT ...
			create.AsSelect = p.parseSelectWithUnion()
		} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			// AS table_function(...) or AS database.table
			name := p.parseIdentifierName()
			if p.currentIs(token.DOT) {
				// AS database.table - skip the table name
				p.nextToken()
				p.parseIdentifierName()
			} else if p.currentIs(token.LPAREN) {
				// AS function(...) - parse as a function call
				fn := &ast.FunctionCall{Name: name}
				p.nextToken() // skip (
				if !p.currentIs(token.RPAREN) {
					fn.Arguments = p.parseExpressionList()
				}
				if p.currentIs(token.RPAREN) {
					p.nextToken()
				}
				create.AsTableFunction = fn
			}
			_ = name // Use name for future AS table support
		}
	}

	// Parse ENGINE after AS (for CREATE TABLE x AS y ENGINE=z syntax)
	if create.Engine == nil && p.currentIs(token.ENGINE) {
		p.nextToken()
		if p.currentIs(token.EQ) {
			p.nextToken()
		}
		create.Engine = p.parseEngineClause()
	}

	// Parse table options after AS ... ENGINE (PARTITION BY, ORDER BY, etc.)
	p.parseTableOptions(create)
}

// parseTableOptions parses table options: PARTITION BY, ORDER BY, PRIMARY KEY, SAMPLE BY, TTL, SETTINGS
func (p *Parser) parseTableOptions(create *ast.CreateQuery) {
	for {
		switch {
		case p.currentIs(token.PARTITION):
			p.nextToken()
			if p.expect(token.BY) {
				// Use ALIAS_PREC to avoid consuming AS keyword (for AS SELECT)
				create.PartitionBy = p.parseExpression(ALIAS_PREC)
			}
		case p.currentIs(token.ORDER):
			p.nextToken()
			if p.expect(token.BY) {
				if p.currentIs(token.LPAREN) {
					pos := p.current.Pos
					p.nextToken()
					exprs := p.parseExpressionList()
					p.expect(token.RPAREN)
					// Store tuple literal for ORDER BY (expr1, expr2, ...) or ORDER BY ()
					if len(exprs) == 0 || len(exprs) > 1 {
						create.OrderBy = []ast.Expression{&ast.Literal{
							Position: pos,
							Type:     ast.LiteralTuple,
							Value:    exprs,
						}}
					} else {
						// Single expression in parentheses - just extract it
						create.OrderBy = exprs
					}
				} else {
					// Use ALIAS_PREC to avoid consuming AS keyword (for AS SELECT)
					create.OrderBy = []ast.Expression{p.parseExpression(ALIAS_PREC)}
				}
			}
		case p.currentIs(token.PRIMARY):
			p.nextToken()
			if p.expect(token.KEY) {
				if p.currentIs(token.LPAREN) {
					pos := p.current.Pos
					p.nextToken()
					exprs := p.parseExpressionList()
					p.expect(token.RPAREN)
					// Store tuple literal for PRIMARY KEY (expr1, expr2, ...) or PRIMARY KEY ()
					if len(exprs) == 0 || len(exprs) > 1 {
						create.PrimaryKey = []ast.Expression{&ast.Literal{
							Position: pos,
							Type:     ast.LiteralTuple,
							Value:    exprs,
						}}
					} else {
						// Single expression in parentheses - just extract it
						create.PrimaryKey = exprs
					}
				} else {
					// Use ALIAS_PREC to avoid consuming AS keyword (for AS SELECT)
					create.PrimaryKey = []ast.Expression{p.parseExpression(ALIAS_PREC)}
				}
			}
		case p.currentIs(token.SAMPLE):
			p.nextToken()
			if p.expect(token.BY) {
				// Use ALIAS_PREC to avoid consuming AS keyword (for AS SELECT)
				create.SampleBy = p.parseExpression(ALIAS_PREC)
			}
		case p.currentIs(token.TTL):
			p.nextToken()
			create.TTL = &ast.TTLClause{
				Position:   p.current.Pos,
				Expression: p.parseExpression(ALIAS_PREC), // Use ALIAS_PREC for AS SELECT
			}
			// Handle TTL GROUP BY x SET y = max(y) syntax
			if p.currentIs(token.GROUP) {
				p.nextToken()
				if p.currentIs(token.BY) {
					p.nextToken()
					// Parse GROUP BY expressions (can have multiple, comma separated)
					for {
						p.parseExpression(ALIAS_PREC)
						if p.currentIs(token.COMMA) {
							p.nextToken()
						} else {
							break
						}
					}
				}
			}
			// Handle SET clause in TTL (aggregation expressions for TTL GROUP BY)
			if p.currentIs(token.SET) {
				p.nextToken()
				// Parse SET expressions until we hit a keyword or end
				for !p.currentIs(token.SETTINGS) && !p.currentIs(token.AS) && !p.currentIs(token.WHERE) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.EOF) {
					p.parseExpression(ALIAS_PREC)
					if p.currentIs(token.COMMA) {
						p.nextToken()
					} else {
						break
					}
				}
			}
			// Handle WHERE clause in TTL (conditional deletion)
			if p.currentIs(token.WHERE) {
				p.nextToken()
				// Parse WHERE condition
				p.parseExpression(ALIAS_PREC)
			}
		case p.currentIs(token.SETTINGS):
			p.nextToken()
			create.Settings = p.parseSettingsList()
		default:
			return
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

	// Parse column definitions (e.g., CREATE VIEW v (x UInt64) AS SELECT ...)
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

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			create.OnCluster = p.parseIdentifierName()
		}
	}

	// Handle TO (target table for materialized views only)
	// TO clause is not valid for regular views - only for MATERIALIZED VIEW
	if p.currentIs(token.TO) {
		if !create.Materialized {
			p.errors = append(p.errors, fmt.Errorf("TO clause is only valid for MATERIALIZED VIEW, not VIEW"))
			return
		}
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

	// Parse table options (ORDER BY, PRIMARY KEY, etc.) for materialized views
	p.parseTableOptions(create)

	// Parse POPULATE (for materialized views)
	if p.currentIs(token.POPULATE) {
		create.Populate = true
		p.nextToken()
	}

	// Parse AS SELECT or AS (subquery) INTERSECT/UNION (subquery)
	if p.currentIs(token.AS) {
		p.nextToken()
		if p.currentIs(token.SELECT) || p.currentIs(token.WITH) || p.currentIs(token.LPAREN) {
			create.AsSelect = p.parseSelectWithUnion()
		}
	}
}

func (p *Parser) parseCreateFunction(create *ast.CreateQuery) {
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

	// Parse function name
	create.FunctionName = p.parseIdentifierName()

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.CLUSTER) {
			p.nextToken()
			create.OnCluster = p.parseIdentifierName()
		}
	}

	// Parse AS lambda_expression
	if p.currentIs(token.AS) {
		p.nextToken()
		create.FunctionBody = p.parseExpression(LOWEST)
	}
}

func (p *Parser) parseCreateUser(create *ast.CreateQuery) {
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

	// Parse user name
	create.UserName = p.parseIdentifierName()

	// Look for authentication data (NOT IDENTIFIED or IDENTIFIED)
	// Scan through tokens looking for these keywords
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		// Check for NOT IDENTIFIED
		if p.currentIs(token.NOT) {
			p.nextToken()
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "IDENTIFIED" {
				create.HasAuthenticationData = true
				p.nextToken()
			}
			continue
		}
		// Check for IDENTIFIED (without NOT)
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "IDENTIFIED" {
			create.HasAuthenticationData = true
			p.nextToken()
			// Parse authentication method and value
			// Forms: IDENTIFIED BY 'password'
			//        IDENTIFIED WITH method BY 'password'
			//        IDENTIFIED WITH method BY 'password', method BY 'password', ...
			for {
				// Skip WITH if present (auth method follows)
				if p.currentIs(token.WITH) {
					p.nextToken()
				}
				// Skip auth method name (plaintext_password, sha256_password, etc.)
				// Stop at BY (token), comma, or next section keywords
				for p.currentIs(token.IDENT) {
					ident := strings.ToUpper(p.current.Value)
					// Stop at HOST, SETTINGS, DEFAULT, GRANTEES - don't consume these
					if ident == "HOST" || ident == "SETTINGS" || ident == "DEFAULT" || ident == "GRANTEES" {
						break
					}
					p.nextToken()
					// Handle REALM/SERVER string values (for kerberos/ldap)
					if p.currentIs(token.STRING) && (ident == "REALM" || ident == "SERVER") {
						p.nextToken()
					}
				}
				// Check for BY 'value' (BY is a keyword token, not IDENT)
				if p.currentIs(token.BY) {
					p.nextToken()
					if p.currentIs(token.STRING) {
						create.AuthenticationValues = append(create.AuthenticationValues, p.current.Value)
						p.nextToken()
					}
				}
				// Check for comma (multiple auth methods)
				if p.currentIs(token.COMMA) {
					p.nextToken()
					continue
				}
				break
			}
			continue
		}
		p.nextToken()
	}
}

func (p *Parser) parseAlterUser() *ast.CreateQuery {
	create := &ast.CreateQuery{
		Position:   p.current.Pos,
		CreateUser: true,
		AlterUser:  true,
	}

	p.nextToken() // skip ALTER
	p.nextToken() // skip USER

	// Parse user name
	create.UserName = p.parseIdentifierName()

	// Scan for authentication data (NOT IDENTIFIED or IDENTIFIED)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		// Check for NOT IDENTIFIED
		if p.currentIs(token.NOT) {
			p.nextToken()
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "IDENTIFIED" {
				create.HasAuthenticationData = true
				p.nextToken()
			}
			continue
		}
		// Check for IDENTIFIED (without NOT)
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "IDENTIFIED" {
			create.HasAuthenticationData = true
			p.nextToken()
			// Parse authentication method and value
			for {
				// Skip WITH if present
				if p.currentIs(token.WITH) {
					p.nextToken()
				}
				// Skip auth method name
				for p.currentIs(token.IDENT) {
					ident := strings.ToUpper(p.current.Value)
					if ident == "HOST" || ident == "SETTINGS" || ident == "DEFAULT" || ident == "GRANTEES" {
						break
					}
					p.nextToken()
					if p.currentIs(token.STRING) && (ident == "REALM" || ident == "SERVER") {
						p.nextToken()
					}
				}
				// Check for BY 'value'
				if p.currentIs(token.BY) {
					p.nextToken()
					if p.currentIs(token.STRING) {
						create.AuthenticationValues = append(create.AuthenticationValues, p.current.Value)
						p.nextToken()
					}
				}
				// Check for comma (multiple auth methods)
				if p.currentIs(token.COMMA) {
					p.nextToken()
					continue
				}
				break
			}
			continue
		}
		p.nextToken()
	}

	return create
}

func (p *Parser) parseCreateGeneric(create *ast.CreateQuery) {
	// Parse name
	name := p.parseIdentifierName()
	if name != "" {
		create.Table = name // Reuse Table field for generic name
	}

	// Skip the rest of the statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}
}

func (p *Parser) parseCreateSettingsProfile(pos token.Position) *ast.CreateSettingsProfileQuery {
	query := &ast.CreateSettingsProfileQuery{
		Position: pos,
	}

	// Skip SETTINGS if present (CREATE SETTINGS PROFILE vs CREATE PROFILE)
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
	}

	// Skip PROFILE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROFILE" {
		p.nextToken()
	}

	// Handle IF NOT EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.NOT) {
			p.nextToken()
		}
		if p.currentIs(token.EXISTS) {
			p.nextToken()
		}
	}

	// Parse profile names (can be multiple: s1, s2, s3)
	for {
		name := p.parseIdentifierName()
		if name != "" {
			query.Names = append(query.Names, name)
		}
		if p.currentIs(token.COMMA) {
			p.nextToken()
			continue
		}
		break
	}

	// Skip the rest of the statement (SETTINGS, TO, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseDropSettingsProfile() *ast.DropSettingsProfileQuery {
	query := &ast.DropSettingsProfileQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DROP

	// Skip SETTINGS if present (DROP SETTINGS PROFILE vs DROP PROFILE)
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
	}

	// Skip PROFILE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROFILE" {
		p.nextToken()
	}

	// Handle IF EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.EXISTS) {
			query.IfExists = true
			p.nextToken()
		}
	}

	// Parse profile names (can be multiple: s1, s2, s3)
	for {
		name := p.parseIdentifierName()
		if name != "" {
			query.Names = append(query.Names, name)
		}
		if p.currentIs(token.COMMA) {
			p.nextToken()
			continue
		}
		break
	}

	// Skip the rest of the statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseAlterSettingsProfile() *ast.AlterSettingsProfileQuery {
	query := &ast.AlterSettingsProfileQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip ALTER

	// Skip SETTINGS if present (ALTER SETTINGS PROFILE vs ALTER PROFILE)
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
	}

	// Skip PROFILE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROFILE" {
		p.nextToken()
	}

	// Handle IF EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.EXISTS) {
			p.nextToken()
		}
	}

	// Parse profile names (can be multiple: s1, s2, s3)
	for {
		name := p.parseIdentifierName()
		if name != "" {
			query.Names = append(query.Names, name)
		}
		if p.currentIs(token.COMMA) {
			p.nextToken()
			continue
		}
		break
	}

	// Skip the rest of the statement (SETTINGS, RENAME TO, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseShowCreateSettingsProfile(pos token.Position) *ast.ShowCreateSettingsProfileQuery {
	query := &ast.ShowCreateSettingsProfileQuery{
		Position: pos,
	}

	// Skip SETTINGS if present (SHOW CREATE SETTINGS PROFILE vs SHOW CREATE PROFILE)
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
	}

	// Skip PROFILE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROFILE" {
		p.nextToken()
	}

	// Parse profile names (can be multiple: s1, s2, s3)
	for {
		name := p.parseIdentifierName()
		if name != "" {
			query.Names = append(query.Names, name)
		}
		if p.currentIs(token.COMMA) {
			p.nextToken()
			continue
		}
		break
	}

	// Skip tokens until FORMAT or end of statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.FORMAT) {
		p.nextToken()
	}

	// Parse FORMAT clause if present
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			query.Format = p.current.Value
			p.nextToken()
		}
	}

	return query
}

func (p *Parser) parseCreateRowPolicy(pos token.Position) *ast.CreateRowPolicyQuery {
	query := &ast.CreateRowPolicyQuery{
		Position: pos,
	}

	// Skip ROW if present (CREATE ROW POLICY vs CREATE POLICY)
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROW" {
		p.nextToken()
	}

	// Skip POLICY
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "POLICY" {
		p.nextToken()
	}

	// Skip the rest of the statement (policy names, ON table, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseDropRowPolicy() *ast.DropRowPolicyQuery {
	query := &ast.DropRowPolicyQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DROP

	// Skip ROW if present (DROP ROW POLICY vs DROP POLICY)
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROW" {
		p.nextToken()
	}

	// Skip POLICY
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "POLICY" {
		p.nextToken()
	}

	// Handle IF EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.EXISTS) {
			query.IfExists = true
			p.nextToken()
		}
	}

	// Skip the rest of the statement (policy names, ON table, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseAlterRowPolicy() *ast.CreateRowPolicyQuery {
	query := &ast.CreateRowPolicyQuery{
		Position: p.current.Pos,
		IsAlter:  true,
	}

	p.nextToken() // skip ALTER

	// Skip ROW if present (ALTER ROW POLICY vs ALTER POLICY)
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROW" {
		p.nextToken()
	}

	// Skip POLICY
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "POLICY" {
		p.nextToken()
	}

	// Skip the rest of the statement (policy names, ON table, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseShowCreateRowPolicy(pos token.Position) *ast.ShowCreateRowPolicyQuery {
	query := &ast.ShowCreateRowPolicyQuery{
		Position: pos,
	}

	// Skip ROW if present (SHOW CREATE ROW POLICY vs SHOW CREATE POLICY)
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROW" {
		p.nextToken()
	}

	// Skip POLICY
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "POLICY" {
		p.nextToken()
	}

	// Skip tokens until FORMAT or end of statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.FORMAT) {
		p.nextToken()
	}

	// Parse FORMAT clause if present
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			query.Format = p.current.Value
			p.nextToken()
		}
	}

	return query
}

func (p *Parser) parseCreateRole(pos token.Position) *ast.CreateRoleQuery {
	query := &ast.CreateRoleQuery{
		Position: pos,
	}

	// Skip ROLE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROLE" {
		p.nextToken()
	}

	// Skip the rest of the statement (role names, SETTINGS, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseDropRole() *ast.DropRoleQuery {
	query := &ast.DropRoleQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DROP

	// Skip ROLE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROLE" {
		p.nextToken()
	}

	// Handle IF EXISTS
	if p.currentIs(token.IF) {
		p.nextToken()
		if p.currentIs(token.EXISTS) {
			query.IfExists = true
			p.nextToken()
		}
	}

	// Skip the rest of the statement (role names, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseAlterRole() *ast.CreateRoleQuery {
	query := &ast.CreateRoleQuery{
		Position: p.current.Pos,
		IsAlter:  true,
	}

	p.nextToken() // skip ALTER

	// Skip ROLE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROLE" {
		p.nextToken()
	}

	// Skip the rest of the statement (role names, SETTINGS, RENAME TO, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseCreateQuota(pos token.Position) *ast.CreateQuotaQuery {
	query := &ast.CreateQuotaQuery{
		Position: pos,
	}

	// Skip QUOTA keyword
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "QUOTA" {
		p.nextToken()
	}

	// Parse quota name
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		query.Name = p.current.Value
		p.nextToken()
	}

	// Skip the rest of the statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseShowCreateRole(pos token.Position) *ast.ShowCreateRoleQuery {
	query := &ast.ShowCreateRoleQuery{
		Position:  pos,
		RoleCount: 1, // Default to 1 role
	}

	// Skip ROLE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROLE" {
		p.nextToken()
	}

	// Count role names (separated by commas)
	// Skip first role name
	for p.currentIs(token.IDENT) || p.currentIs(token.STRING) || p.current.Token.IsKeyword() {
		p.nextToken()
		// Handle role@host syntax
		if p.currentIs(token.IDENT) && strings.HasPrefix(p.current.Value, "@") {
			p.nextToken()
		}
		break
	}

	// Count additional roles
	for p.currentIs(token.COMMA) {
		query.RoleCount++
		p.nextToken()
		// Skip role name
		for p.currentIs(token.IDENT) || p.currentIs(token.STRING) || p.current.Token.IsKeyword() {
			p.nextToken()
			// Handle role@host syntax
			if p.currentIs(token.IDENT) && strings.HasPrefix(p.current.Value, "@") {
				p.nextToken()
			}
			break
		}
	}

	// Skip tokens until FORMAT or end of statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.FORMAT) {
		p.nextToken()
	}

	// Parse FORMAT clause if present
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			query.Format = p.current.Value
			p.nextToken()
		}
	}

	return query
}

func (p *Parser) parseCreateResource(pos token.Position) *ast.CreateResourceQuery {
	query := &ast.CreateResourceQuery{
		Position: pos,
	}

	// Skip RESOURCE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "RESOURCE" {
		p.nextToken()
	}

	// Get resource name
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		query.Name = p.current.Value
		p.nextToken()
	}

	// Skip the rest of the statement (resource definition, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseDropResource() *ast.DropResourceQuery {
	query := &ast.DropResourceQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DROP

	// Skip RESOURCE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "RESOURCE" {
		p.nextToken()
	}

	// Skip the rest of the statement (IF EXISTS, name, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseCreateWorkload(pos token.Position) *ast.CreateWorkloadQuery {
	query := &ast.CreateWorkloadQuery{
		Position: pos,
	}

	// Skip WORKLOAD
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "WORKLOAD" {
		p.nextToken()
	}

	// Get workload name
	if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		query.Name = p.current.Value
		p.nextToken()
	}

	// Check for IN (parent workload)
	if p.currentIs(token.IN) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			query.Parent = p.current.Value
			p.nextToken()
		}
	}

	// Skip the rest of the statement (SETTINGS, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseDropWorkload() *ast.DropWorkloadQuery {
	query := &ast.DropWorkloadQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DROP

	// Skip WORKLOAD
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "WORKLOAD" {
		p.nextToken()
	}

	// Skip the rest of the statement (IF EXISTS, name, etc.)
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
}

func (p *Parser) parseCreateDictionary(create *ast.CreateQuery) {
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

	// Parse dictionary name (possibly database.name)
	name := p.parseIdentifierName()
	if p.currentIs(token.DOT) {
		create.Database = name
		p.nextToken()
		name = p.parseIdentifierName()
	}
	create.Table = name

	// Handle ON CLUSTER
	if p.currentIs(token.ON) {
		p.nextToken()
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "CLUSTER" {
			p.nextToken()
			create.OnCluster = p.parseIdentifierName()
		}
	}

	// Parse column definitions (attributes) if present
	if p.currentIs(token.LPAREN) {
		p.nextToken() // skip (
		create.DictionaryAttrs = p.parseDictionaryAttributes()
		if p.currentIs(token.RPAREN) {
			p.nextToken() // skip )
		}
	}

	// Initialize dictionary definition
	dictDef := &ast.DictionaryDefinition{
		Position: p.current.Pos,
	}

	// Parse PRIMARY KEY, SOURCE, LIFETIME, LAYOUT, RANGE, SETTINGS
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		// Handle PRIMARY as a keyword token
		if p.currentIs(token.PRIMARY) {
			p.nextToken() // skip PRIMARY
			if p.currentIs(token.KEY) {
				p.nextToken() // skip KEY
				dictDef.PrimaryKey = p.parseDictionaryPrimaryKey()
			}
			continue
		}
		if p.currentIs(token.IDENT) {
			upper := strings.ToUpper(p.current.Value)
			switch upper {
			case "PRIMARY":
				p.nextToken() // skip PRIMARY
				if p.currentIs(token.KEY) {
					p.nextToken() // skip KEY
					dictDef.PrimaryKey = p.parseDictionaryPrimaryKey()
				}
			case "SOURCE":
				p.nextToken() // skip SOURCE
				dictDef.Source = p.parseDictionarySource()
			case "LIFETIME":
				p.nextToken() // skip LIFETIME
				dictDef.Lifetime = p.parseDictionaryLifetime()
			case "LAYOUT":
				p.nextToken() // skip LAYOUT
				dictDef.Layout = p.parseDictionaryLayout()
			case "RANGE":
				p.nextToken() // skip RANGE
				dictDef.Range = p.parseDictionaryRange()
			case "SETTINGS":
				p.nextToken() // skip SETTINGS
				// Skip settings for now
				for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.isDictionaryClauseKeyword() {
					p.nextToken()
				}
			case "COMMENT":
				p.nextToken() // skip COMMENT
				if p.currentIs(token.STRING) {
					create.Comment = p.current.Value
					p.nextToken()
				}
			default:
				p.nextToken()
			}
		} else {
			p.nextToken()
		}
	}

	// Only set dictionary definition if it has any content
	if len(dictDef.PrimaryKey) > 0 || dictDef.Source != nil || dictDef.Lifetime != nil || dictDef.Layout != nil || dictDef.Range != nil {
		create.DictionaryDef = dictDef
	}
}

func (p *Parser) isDictionaryClauseKeyword() bool {
	if !p.currentIs(token.IDENT) {
		return false
	}
	upper := strings.ToUpper(p.current.Value)
	switch upper {
	case "PRIMARY", "SOURCE", "LIFETIME", "LAYOUT", "RANGE", "SETTINGS", "COMMENT":
		return true
	}
	return false
}

func (p *Parser) parseDictionaryAttributes() []*ast.DictionaryAttributeDeclaration {
	var attrs []*ast.DictionaryAttributeDeclaration

	for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) {
		attr := p.parseDictionaryAttribute()
		if attr != nil {
			attrs = append(attrs, attr)
		}

		// Handle comma between attributes
		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return attrs
}

func (p *Parser) parseDictionaryAttribute() *ast.DictionaryAttributeDeclaration {
	attr := &ast.DictionaryAttributeDeclaration{
		Position: p.current.Pos,
	}

	// Parse attribute name
	attr.Name = p.parseIdentifierName()
	if attr.Name == "" {
		return nil
	}

	// Parse type
	if !p.currentIs(token.COMMA) && !p.currentIs(token.RPAREN) {
		attr.Type = p.parseDataType()
	}

	// Parse optional clauses: DEFAULT, EXPRESSION, HIERARCHICAL, INJECTIVE, IS_OBJECT_ID
	for !p.currentIs(token.EOF) && !p.currentIs(token.COMMA) && !p.currentIs(token.RPAREN) {
		if p.currentIs(token.DEFAULT) {
			p.nextToken()
			attr.Default = p.parseExpression(LOWEST)
		} else if p.currentIs(token.IDENT) {
			upper := strings.ToUpper(p.current.Value)
			switch upper {
			case "EXPRESSION":
				p.nextToken()
				attr.Expression = p.parseExpression(LOWEST)
			case "HIERARCHICAL":
				attr.Hierarchical = true
				p.nextToken()
			case "INJECTIVE":
				attr.Injective = true
				p.nextToken()
			case "IS_OBJECT_ID":
				attr.IsObjectID = true
				p.nextToken()
			default:
				p.nextToken()
			}
		} else {
			p.nextToken()
		}
	}

	return attr
}

func (p *Parser) parseDictionaryPrimaryKey() []ast.Expression {
	var keys []ast.Expression

	// Can be single identifier or tuple (id1, id2)
	if p.currentIs(token.LPAREN) {
		p.nextToken() // skip (
		for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) {
			expr := p.parseExpression(LOWEST)
			if expr != nil {
				keys = append(keys, expr)
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
		if p.currentIs(token.RPAREN) {
			p.nextToken() // skip )
		}
	} else {
		// Single identifier
		expr := p.parseExpression(LOWEST)
		if expr != nil {
			keys = append(keys, expr)
		}
	}

	return keys
}

func (p *Parser) parseDictionarySource() *ast.DictionarySource {
	source := &ast.DictionarySource{
		Position: p.current.Pos,
	}

	if !p.currentIs(token.LPAREN) {
		return source
	}
	p.nextToken() // skip (

	// Parse source type (e.g., CLICKHOUSE, MYSQL, FILE)
	if p.currentIs(token.IDENT) {
		source.Type = strings.ToUpper(p.current.Value)
		p.nextToken()
	}

	// Parse key-value arguments in parentheses
	if p.currentIs(token.LPAREN) {
		p.nextToken() // skip (
		source.Args = p.parseKeyValuePairs()
		if p.currentIs(token.RPAREN) {
			p.nextToken() // skip )
		}
	}

	if p.currentIs(token.RPAREN) {
		p.nextToken() // skip )
	}

	return source
}

func (p *Parser) parseKeyValuePairs() []*ast.KeyValuePair {
	var pairs []*ast.KeyValuePair

	for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) {
		pair := &ast.KeyValuePair{
			Position: p.current.Pos,
		}

		// Parse key (the key is not included in EXPLAIN output, just the value)
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			pair.Key = p.current.Value
			p.nextToken()
		}

		// Parse value (can be various types - string, number, function call, identifier)
		if !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) && !p.currentIs(token.IDENT) && !p.current.Token.IsKeyword() {
			pair.Value = p.parseExpression(LOWEST)
		} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			// If next token is an identifier/keyword, treat it as start of next pair
			// unless it's a function call (has LPAREN after)
			if p.peekIs(token.LPAREN) {
				pair.Value = p.parseExpression(LOWEST)
			}
			// Otherwise no value for this pair (key only)
		}

		pairs = append(pairs, pair)
	}

	return pairs
}

func (p *Parser) parseDictionaryLifetime() *ast.DictionaryLifetime {
	lifetime := &ast.DictionaryLifetime{
		Position: p.current.Pos,
	}

	if !p.currentIs(token.LPAREN) {
		return lifetime
	}
	p.nextToken() // skip (

	// Parse MIN and MAX or just a single value
	for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) {
		if p.currentIs(token.IDENT) {
			upper := strings.ToUpper(p.current.Value)
			if upper == "MIN" {
				p.nextToken()
				lifetime.Min = p.parseExpression(LOWEST)
			} else if upper == "MAX" {
				p.nextToken()
				lifetime.Max = p.parseExpression(LOWEST)
			} else {
				p.nextToken()
			}
		} else {
			p.nextToken()
		}
	}

	if p.currentIs(token.RPAREN) {
		p.nextToken() // skip )
	}

	return lifetime
}

func (p *Parser) parseDictionaryLayout() *ast.DictionaryLayout {
	layout := &ast.DictionaryLayout{
		Position: p.current.Pos,
	}

	if !p.currentIs(token.LPAREN) {
		return layout
	}
	p.nextToken() // skip (

	// Parse layout type (e.g., FLAT, HASHED, COMPLEX_KEY_HASHED)
	if p.currentIs(token.IDENT) {
		layout.Type = strings.ToUpper(p.current.Value)
		p.nextToken()
	}

	// Parse optional arguments in parentheses
	if p.currentIs(token.LPAREN) {
		p.nextToken() // skip (
		layout.Args = p.parseKeyValuePairs()
		if p.currentIs(token.RPAREN) {
			p.nextToken() // skip )
		}
	}

	if p.currentIs(token.RPAREN) {
		p.nextToken() // skip )
	}

	return layout
}

func (p *Parser) parseDictionaryRange() *ast.DictionaryRange {
	dictRange := &ast.DictionaryRange{
		Position: p.current.Pos,
	}

	if !p.currentIs(token.LPAREN) {
		return dictRange
	}
	p.nextToken() // skip (

	// Parse MIN and MAX
	for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) {
		if p.currentIs(token.IDENT) {
			upper := strings.ToUpper(p.current.Value)
			if upper == "MIN" {
				p.nextToken()
				dictRange.Min = p.parseExpression(LOWEST)
			} else if upper == "MAX" {
				p.nextToken()
				dictRange.Max = p.parseExpression(LOWEST)
			} else {
				p.nextToken()
			}
		} else {
			p.nextToken()
		}
	}

	if p.currentIs(token.RPAREN) {
		p.nextToken() // skip )
	}

	return dictRange
}

func (p *Parser) parseIndexDefinition() *ast.IndexDefinition {
	idx := &ast.IndexDefinition{
		Position: p.current.Pos,
	}

	p.nextToken() // skip INDEX

	// Parse index name
	idx.Name = p.parseIdentifierName()

	// Parse expression (the column or expression being indexed)
	idx.Expression = p.parseExpression(LOWEST)

	// Parse TYPE
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "TYPE" {
		p.nextToken()
		// Type is a function call like bloom_filter(0.025) or minmax
		pos := p.current.Pos
		typeName := p.parseIdentifierName()
		if typeName != "" {
			idx.Type = &ast.FunctionCall{
				Position: pos,
				Name:     typeName,
			}
			// Check for parentheses (type parameters)
			if p.currentIs(token.LPAREN) {
				p.nextToken()
				if !p.currentIs(token.RPAREN) {
					idx.Type.Arguments = p.parseExpressionList()
				}
				p.expect(token.RPAREN)
			}
		}
	}

	// Parse GRANULARITY
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "GRANULARITY" {
		p.nextToken()
		idx.Granularity = p.parseExpression(LOWEST)
	}

	return idx
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

	// Check if next token indicates type is omitted
	// DEFAULT/MATERIALIZED/ALIAS indicate we go straight to default expression
	// CODEC indicates we go straight to codec specification (no type)
	isCodec := p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "CODEC"
	if p.currentIs(token.DEFAULT) || p.currentIs(token.MATERIALIZED) || p.currentIs(token.ALIAS) || isCodec {
		// Type is omitted, skip to parsing below
	} else {
		// Parse data type
		col.Type = p.parseDataType()
	}

	// Parse STATISTICS clause (e.g., STATISTICS(tdigest, uniq))
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "STATISTICS" {
		p.nextToken()
		col.Statistics = p.parseStatisticsExpr()
	}

	// Handle COLLATE clause (MySQL compatibility, e.g., varchar(255) COLLATE binary)
	if p.currentIs(token.COLLATE) {
		p.nextToken()
		// Skip collation name
		if p.currentIs(token.IDENT) || p.currentIs(token.STRING) {
			p.nextToken()
		}
	}

	// Handle NOT NULL / NULL constraint
	if p.currentIs(token.NOT) {
		p.nextToken()
		if p.currentIs(token.NULL) {
			notNull := false
			col.Nullable = &notNull
			p.nextToken()
		}
	} else if p.currentIs(token.NULL) {
		// NULL is explicit nullable (default)
		nullable := true
		col.Nullable = &nullable
		p.nextToken()
	}

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

	// Parse PRIMARY KEY (column constraint)
	if p.currentIs(token.PRIMARY) {
		p.nextToken()
		if p.currentIs(token.KEY) {
			col.PrimaryKey = true
			p.nextToken()
		}
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

	// For MySQL-compatible INT types, handle display width and UNSIGNED/SIGNED
	upperName := strings.ToUpper(dt.Name)
	isMySQLIntType := upperName == "INT" || upperName == "TINYINT" || upperName == "SMALLINT" ||
		upperName == "MEDIUMINT" || upperName == "BIGINT"

	if isMySQLIntType && p.currentIs(token.LPAREN) {
		// Skip the display width parameter (e.g., INT(11))
		p.nextToken() // skip (
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) {
			p.nextToken()
		}
		p.expect(token.RPAREN)
	}

	// Handle UNSIGNED/SIGNED modifiers for MySQL INT types
	if isMySQLIntType && p.currentIs(token.IDENT) {
		modifier := strings.ToUpper(p.current.Value)
		if modifier == "UNSIGNED" || modifier == "SIGNED" {
			dt.Name = dt.Name + " " + p.current.Value
			p.nextToken()
		}
	}

	// Parse type parameters
	if p.currentIs(token.LPAREN) {
		dt.HasParentheses = true
		p.nextToken()

		// Determine if this type uses named parameters (Nested, Tuple, JSON)
		upperName := strings.ToUpper(dt.Name)
		usesNamedParams := upperName == "NESTED" || upperName == "TUPLE" || upperName == "JSON" || upperName == "OBJECT"
		// JSON and OBJECT types wrap their parameters in ObjectTypeArgument
		isObjectType := upperName == "JSON" || upperName == "OBJECT"

		// Parse type parameters, but stop on keywords that can't be part of type params
		for !p.currentIs(token.RPAREN) && !p.currentIs(token.EOF) && !p.currentIs(token.COLLATE) {
			var param ast.Expression

			// Special handling for SKIP in JSON/OBJECT types: SKIP path or SKIP REGEXP 'pattern'
			if isObjectType && (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && strings.ToUpper(p.current.Value) == "SKIP" {
				pos := p.current.Pos
				p.nextToken() // consume SKIP

				// Check for SKIP REGEXP 'pattern'
				if p.currentIs(token.REGEXP) || (p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "REGEXP") {
					p.nextToken() // consume REGEXP
					// Parse the pattern string
					if p.currentIs(token.STRING) {
						pattern := p.current.Value
						p.nextToken()
						param = &ast.FunctionCall{
							Position:  pos,
							Name:      "SKIP REGEXP",
							Arguments: []ast.Expression{&ast.Literal{Position: pos, Value: pattern, Type: ast.LiteralString}},
						}
					}
				} else {
					// Parse dotted path: a, a.b, a.b.c, etc.
					var pathParts []string
					for {
						if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
							pathParts = append(pathParts, p.current.Value)
							p.nextToken()
						}
						if p.currentIs(token.DOT) {
							p.nextToken() // consume dot
						} else {
							break
						}
					}
					if len(pathParts) > 0 {
						param = &ast.FunctionCall{
							Position:  pos,
							Name:      "SKIP",
							Arguments: []ast.Expression{&ast.Identifier{Position: pos, Parts: pathParts}},
						}
					}
				}
				// Wrap in ObjectTypeArgument
				if param != nil {
					param = &ast.ObjectTypeArgument{
						Position: param.Pos(),
						Expr:     param,
					}
					dt.Parameters = append(dt.Parameters, param)
				}
				if p.currentIs(token.COMMA) {
					p.nextToken()
				} else {
					break
				}
				continue
			}

			// Check if this is a named parameter: identifier followed by a type name
			// e.g., "a UInt32" where "a" is the name and "UInt32" is the type
			isNamedParam := false
			if usesNamedParams && (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) {
				// Check if current is NOT a type name and peek IS a type name or LPAREN follows for complex types
				// But NOT if peek is '=' which indicates an expression like max_dynamic_paths=8
				if !p.isDataTypeName(p.current.Value) && !p.peekIs(token.EQ) {
					// Current is a name (not a type), next should be a type
					isNamedParam = true
				} else if !p.peekIs(token.EQ) && (p.peekIs(token.IDENT) || p.peekIs(token.LPAREN)) {
					// Current looks like a type name but is followed by another identifier
					// This happens with things like "a Tuple(...)" where "a" looks like it could be a type
					// Check if peek is a known type name
					if p.peekIs(token.IDENT) && p.isDataTypeName(p.peek.Value) {
						isNamedParam = true
					} else if p.peekIs(token.LPAREN) {
						// Could be a function-like type or named with parenthesized type
						// Check if current is a valid type name - if so, it's a type, not a name
						if !p.isDataTypeName(p.current.Value) {
							isNamedParam = true
						}
					}
				}
			}

			if isNamedParam {
				// Parse as name + type pair
				pos := p.current.Pos
				paramName := p.current.Value
				p.nextToken()
				// Parse the type for this parameter
				paramType := p.parseDataType()
				if paramType != nil {
					param = &ast.NameTypePair{
						Position: pos,
						Name:     paramName,
						Type:     paramType,
					}
				}
			} else if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.isDataTypeName(p.current.Value) {
				// It's a type name, parse as data type
				param = p.parseDataType()
			} else {
				// Parse as expression (for things like Decimal(10, 2))
				param = p.parseExpression(LOWEST)
			}

			// Wrap in ObjectTypeArgument for JSON/OBJECT types
			if param != nil {
				if isObjectType {
					param = &ast.ObjectTypeArgument{
						Position: param.Pos(),
						Expr:     param,
					}
				}
				dt.Parameters = append(dt.Parameters, param)
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

func (p *Parser) parseStatisticsExpr() []*ast.FunctionCall {
	var stats []*ast.FunctionCall

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

			// Statistics types can have optional parameters: e.g., tdigest(100)
			if p.currentIs(token.LPAREN) {
				p.nextToken()
				if !p.currentIs(token.RPAREN) {
					fn.Arguments = p.parseExpressionList()
				}
				p.expect(token.RPAREN)
			}

			stats = append(stats, fn)
		}

		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	p.expect(token.RPAREN)
	return stats
}

// parseStatisticsColumnList parses comma-separated column names for ALTER STATISTICS commands
func (p *Parser) parseStatisticsColumnList() []string {
	var columns []string

	for p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		columns = append(columns, p.current.Value)
		p.nextToken()

		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return columns
}

// parseStatisticsTypeList parses comma-separated statistics type names for ALTER STATISTICS TYPE clause
func (p *Parser) parseStatisticsTypeList() []*ast.FunctionCall {
	var types []*ast.FunctionCall

	for p.currentIs(token.IDENT) {
		name := p.current.Value
		pos := p.current.Pos
		p.nextToken()

		fn := &ast.FunctionCall{
			Position: pos,
			Name:     name,
		}

		// Statistics types can have optional parameters
		if p.currentIs(token.LPAREN) {
			p.nextToken()
			if !p.currentIs(token.RPAREN) {
				fn.Arguments = p.parseExpressionList()
			}
			p.expect(token.RPAREN)
		}

		types = append(types, fn)

		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return types
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
	dropFunction := false
	dropDictionary := false
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
		dropFunction = true
		p.nextToken()
	case token.INDEX:
		p.nextToken()
	case token.SETTINGS:
		// DROP SETTINGS PROFILE
		p.nextToken() // skip SETTINGS
		// Skip "PROFILE" if present
		if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROFILE" {
			p.nextToken()
		}
		drop.SettingsProfile = "_pending_"
	default:
		// Handle multi-word DROP types: ROW POLICY, NAMED COLLECTION, DICTIONARY
		if p.currentIs(token.IDENT) {
			upper := strings.ToUpper(p.current.Value)
			switch upper {
			case "DICTIONARY":
				dropDictionary = true
				p.nextToken()
			case "ROW":
				// DROP ROW POLICY
				p.nextToken() // skip ROW
				if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "POLICY" {
					p.nextToken() // skip POLICY
				}
				// Mark as row policy drop - name will be set later
				drop.RowPolicy = "_pending_"
			case "POLICY":
				// DROP POLICY
				p.nextToken()
				drop.Policy = "_pending_"
			case "QUOTA":
				p.nextToken()
				drop.Quota = "_pending_"
			case "ROLE":
				p.nextToken()
				drop.Role = "_pending_"
			case "NAMED":
				// DROP NAMED COLLECTION - skip tokens
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
		var database, tableName string
		if p.currentIs(token.DOT) {
			p.nextToken()
			database = name
			tableName = p.parseIdentifierName()
		} else {
			tableName = name
		}

		if dropUser {
			drop.User = tableName
			// Handle user@host syntax
			if p.currentIs(token.IDENT) && p.current.Value == "@" {
				p.nextToken() // skip @
				// Hostname can be identifier, string, or IP in quotes
				if p.currentIs(token.IDENT) || p.currentIs(token.STRING) || p.current.Token.IsKeyword() {
					drop.User = drop.User + "@" + p.current.Value
					p.nextToken()
				}
			}
		} else if dropFunction {
			drop.Function = tableName
		} else if drop.Role == "_pending_" {
			drop.Role = tableName
		} else if drop.Quota == "_pending_" {
			drop.Quota = tableName
		} else if drop.Policy == "_pending_" {
			drop.Policy = tableName
		} else if drop.RowPolicy == "_pending_" {
			drop.RowPolicy = tableName
		} else if drop.SettingsProfile == "_pending_" {
			drop.SettingsProfile = tableName
		} else if dropDictionary {
			drop.Dictionary = tableName
			// Also set Table/Tables for backward compatibility with AST JSON
			drop.Tables = append(drop.Tables, &ast.TableIdentifier{
				Position: drop.Position,
				Database: database,
				Table:    tableName,
			})
			drop.Table = tableName
			if database != "" {
				drop.Database = database
			}
		} else if drop.DropDatabase {
			drop.Database = tableName
		} else {
			// First table - add to Tables list
			drop.Tables = append(drop.Tables, &ast.TableIdentifier{
				Position: drop.Position,
				Database: database,
				Table:    tableName,
			})
			drop.Table = tableName // Keep for backward compatibility
			if database != "" {
				drop.Database = database
			}
		}
	}

	// Handle multiple tables/users (DROP TABLE IF EXISTS t1, t2, t3 or DROP USER u1, u2@host)
	for p.currentIs(token.COMMA) {
		p.nextToken()
		pos := p.current.Pos
		name := p.parseIdentifierName()
		var database, tableName string
		if p.currentIs(token.DOT) {
			p.nextToken()
			database = name
			tableName = p.parseIdentifierName()
		} else {
			tableName = name
		}
		// Handle user@host syntax for additional users
		if dropUser && p.currentIs(token.IDENT) && p.current.Value == "@" {
			p.nextToken() // skip @
			if p.currentIs(token.IDENT) || p.currentIs(token.STRING) || p.current.Token.IsKeyword() {
				tableName = tableName + "@" + p.current.Value
				p.nextToken()
			}
		}
		if tableName != "" {
			drop.Tables = append(drop.Tables, &ast.TableIdentifier{
				Position: pos,
				Database: database,
				Table:    tableName,
			})
		}
	}

	// Handle PARALLEL WITH (drop multiple tables in parallel)
	// Syntax: DROP TABLE IF EXISTS t1 PARALLEL WITH DROP TABLE IF EXISTS t2
	for p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PARALLEL" {
		p.nextToken() // skip PARALLEL
		if p.currentIs(token.WITH) {
			p.nextToken() // skip WITH
			// Parse the next DROP statement
			if p.currentIs(token.DROP) {
				p.nextToken() // skip DROP
				// Handle TEMPORARY
				if p.currentIs(token.TEMPORARY) {
					p.nextToken()
				}
				// Skip TABLE/DATABASE/etc
				if p.currentIs(token.TABLE) || p.currentIs(token.DATABASE) || p.currentIs(token.VIEW) {
					p.nextToken()
				}
				// Handle IF EXISTS
				if p.currentIs(token.IF) {
					p.nextToken()
					if p.currentIs(token.EXISTS) {
						p.nextToken()
					}
				}
				// Parse table name
				pos := p.current.Pos
				name := p.parseIdentifierName()
				var database, tableName string
				if p.currentIs(token.DOT) {
					p.nextToken()
					database = name
					tableName = p.parseIdentifierName()
				} else {
					tableName = name
				}
				if tableName != "" {
					drop.Tables = append(drop.Tables, &ast.TableIdentifier{
						Position: pos,
						Database: database,
						Table:    tableName,
					})
				}
			}
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
			// Skip the table reference - can be db.table or db.* (wildcard)
			p.parseIdentifierName()
			if p.currentIs(token.DOT) {
				p.nextToken()
				// Handle wildcard (*) or table name
				if p.currentIs(token.ASTERISK) {
					p.nextToken()
				} else {
					p.parseIdentifierName()
				}
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

	// Handle FORMAT clause (for things like DROP TABLE ... FORMAT Null)
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		// Skip format name (Null, etc.)
		if p.currentIs(token.NULL) || p.currentIs(token.IDENT) {
			p.nextToken()
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

	// Parse commands (can be parenthesized for multiple mutations)
	for {
		// Handle parenthesized command syntax: ALTER TABLE t (DELETE WHERE ...), (UPDATE ...)
		if p.currentIs(token.LPAREN) {
			p.nextToken() // skip (
			cmd := p.parseAlterCommand()
			if cmd != nil {
				alter.Commands = append(alter.Commands, cmd)
			}
			p.expect(token.RPAREN)
		} else {
			cmd := p.parseAlterCommand()
			if cmd == nil {
				break
			}
			alter.Commands = append(alter.Commands, cmd)
		}

		if !p.currentIs(token.COMMA) {
			break
		}
		p.nextToken()
	}

	// Parse SETTINGS clause
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		alter.Settings = p.parseSettingsList()
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
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROJECTION" {
			cmd.Type = ast.AlterAddProjection
			p.nextToken()
			cmd.Projection = p.parseProjection()
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "STATISTICS" {
			cmd.Type = ast.AlterAddStatistics
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
			// Parse column list (comma-separated identifiers)
			cmd.StatisticsColumns = p.parseStatisticsColumnList()
			// Parse TYPE clause
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "TYPE" {
				p.nextToken()
				cmd.StatisticsTypes = p.parseStatisticsTypeList()
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
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROJECTION" {
			cmd.Type = ast.AlterDropProjection
			p.nextToken()
			if p.currentIs(token.IF) {
				p.nextToken()
				p.expect(token.EXISTS)
				cmd.IfExists = true
			}
			if p.currentIs(token.IDENT) {
				cmd.ProjectionName = p.current.Value
				p.nextToken()
			}
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "STATISTICS" {
			cmd.Type = ast.AlterDropStatistics
			p.nextToken()
			if p.currentIs(token.IF) {
				p.nextToken()
				p.expect(token.EXISTS)
				cmd.IfExists = true
			}
			cmd.StatisticsColumns = p.parseStatisticsColumnList()
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
			} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROJECTION" {
				cmd.Type = ast.AlterClearProjection
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.ProjectionName = p.current.Value
					p.nextToken()
				}
			} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "STATISTICS" {
				cmd.Type = ast.AlterClearStatistics
				p.nextToken()
				if p.currentIs(token.IF) {
					p.nextToken()
					p.expect(token.EXISTS)
					cmd.IfExists = true
				}
				cmd.StatisticsColumns = p.parseStatisticsColumnList()
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
			} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROJECTION" {
				cmd.Type = ast.AlterMaterializeProjection
				p.nextToken()
				if p.currentIs(token.IDENT) {
					cmd.ProjectionName = p.current.Value
					p.nextToken()
				}
			} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "STATISTICS" {
				cmd.Type = ast.AlterMaterializeStatistics
				p.nextToken()
				if p.currentIs(token.IF) {
					p.nextToken()
					p.expect(token.EXISTS)
					cmd.IfExists = true
				}
				cmd.StatisticsColumns = p.parseStatisticsColumnList()
			}
		} else {
			return nil
		}
	case token.MODIFY:
		p.nextToken()
		if p.currentIs(token.COLUMN) {
			cmd.Type = ast.AlterModifyColumn
			p.nextToken()
			// Handle MODIFY COLUMN name REMOVE ... (e.g., REMOVE COMMENT)
			// Check if the next token after column name is REMOVE
			if (p.currentIs(token.IDENT) || p.current.Token.IsKeyword()) && p.peek.Token == token.IDENT && strings.ToUpper(p.peek.Value) == "REMOVE" {
				// Just parse column name without type
				colName := p.current.Value
				p.nextToken() // skip column name
				cmd.Column = &ast.ColumnDeclaration{Name: colName}
				// Skip REMOVE COMMENT etc.
				for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.COMMA) {
					p.nextToken()
				}
			} else {
				cmd.Column = p.parseColumnDeclaration()
			}
		} else if p.currentIs(token.TTL) {
			cmd.Type = ast.AlterModifyTTL
			p.nextToken()
			cmd.TTL = &ast.TTLClause{
				Position:   p.current.Pos,
				Expression: p.parseExpression(LOWEST),
			}
		} else if p.currentIs(token.SETTINGS) || (p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "SETTING") {
			// Both SETTINGS and SETTING (singular) are accepted
			cmd.Type = ast.AlterModifySetting
			p.nextToken()
			cmd.Settings = p.parseSettingsList()
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "STATISTICS" {
			cmd.Type = ast.AlterModifyStatistics
			p.nextToken()
			// Parse column list (comma-separated identifiers)
			cmd.StatisticsColumns = p.parseStatisticsColumnList()
			// Parse TYPE clause
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "TYPE" {
				p.nextToken()
				cmd.StatisticsTypes = p.parseStatisticsTypeList()
			}
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
	case token.COMMENT:
		p.nextToken()
		if p.currentIs(token.COLUMN) {
			cmd.Type = ast.AlterCommentColumn
			p.nextToken()
			// Handle IF EXISTS
			if p.currentIs(token.IF) {
				p.nextToken()
				p.expect(token.EXISTS)
				cmd.IfExists = true
			}
			// Parse column name
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				cmd.ColumnName = p.current.Value
				p.nextToken()
			}
			// Parse comment string
			if p.currentIs(token.STRING) {
				cmd.Comment = p.current.Value
				p.nextToken()
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
	case token.FETCH:
		p.nextToken()
		if p.currentIs(token.PARTITION) {
			cmd.Type = ast.AlterFetchPartition
			p.nextToken()
			cmd.Partition = p.parseExpression(LOWEST)
			// FROM path
			if p.currentIs(token.FROM) {
				p.nextToken()
				if p.currentIs(token.STRING) {
					cmd.FromPath = p.current.Value
					p.nextToken()
				}
			}
		}
	case token.DELETE:
		// DELETE WHERE condition - mutation to delete rows
		cmd.Type = ast.AlterDeleteWhere
		p.nextToken() // skip DELETE
		if p.currentIs(token.WHERE) {
			p.nextToken() // skip WHERE
			cmd.Where = p.parseExpression(LOWEST)
		}
	case token.UPDATE:
		// UPDATE col = expr, ... WHERE condition - mutation to update rows
		cmd.Type = ast.AlterUpdate
		p.nextToken() // skip UPDATE
		// Parse assignments
		for {
			if !p.currentIs(token.IDENT) {
				break
			}
			assign := &ast.Assignment{
				Position: p.current.Pos,
				Column:   p.current.Value,
			}
			p.nextToken() // skip column name
			if p.currentIs(token.EQ) {
				p.nextToken() // skip =
				assign.Value = p.parseExpression(LOWEST)
			}
			cmd.Assignments = append(cmd.Assignments, assign)
			if !p.currentIs(token.COMMA) {
				break
			}
			p.nextToken() // skip comma
		}
		if p.currentIs(token.WHERE) {
			p.nextToken() // skip WHERE
			cmd.Where = p.parseExpression(LOWEST)
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

	// Check for subquery: DESCRIBE (SELECT ...)
	if p.currentIs(token.LPAREN) {
		desc.TableExpr = p.parseTableExpression()
	} else if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
		// Parse table name or table function
		// Table functions look like: format(CSV, '...'), url('...'), s3Cluster(...)
		pos := p.current.Pos
		tableName := p.current.Value
		p.nextToken()

		// Check if this is a function call (table function)
		if p.currentIs(token.LPAREN) {
			desc.TableFunction = p.parseFunctionCall(tableName, pos)
		} else if p.currentIs(token.DOT) {
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

	// Parse SETTINGS clause
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		desc.Settings = p.parseSettingsList()
	}

	// Parse FORMAT clause
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.currentIs(token.NULL) || p.current.Token.IsKeyword() {
			desc.Format = p.current.Value
			p.nextToken()
		}
	}

	return desc
}

func (p *Parser) parseShow() ast.Statement {
	pos := p.current.Pos

	p.nextToken() // skip SHOW

	// Handle SHOW PRIVILEGES first - it has its own statement type
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PRIVILEGES" {
		p.nextToken()
		return &ast.ShowPrivilegesQuery{Position: pos}
	}

	// Handle SHOW GRANTS - it has its own statement type
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "GRANTS" {
		query := &ast.ShowGrantsQuery{Position: pos}
		// Skip tokens until FORMAT or end of statement
		for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.FORMAT) {
			p.nextToken()
		}
		// Parse FORMAT clause if present
		if p.currentIs(token.FORMAT) {
			p.nextToken()
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				query.Format = p.current.Value
				p.nextToken()
			}
		}
		return query
	}

	show := &ast.ShowQuery{
		Position: pos,
	}

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
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "QUOTA" {
			// SHOW CREATE QUOTA <name>
			p.nextToken()
			query := &ast.ShowCreateQuotaQuery{Position: pos}
			if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
				query.Name = p.current.Value
				p.nextToken()
			}
			// Parse FORMAT clause if present
			if p.currentIs(token.FORMAT) {
				p.nextToken()
				if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					query.Format = p.current.Value
					p.nextToken()
				}
			}
			return query
		} else if p.currentIs(token.SETTINGS) || (p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "PROFILE") {
			// SHOW CREATE SETTINGS PROFILE or SHOW CREATE PROFILE
			return p.parseShowCreateSettingsProfile(pos)
		} else if p.currentIs(token.IDENT) && (strings.ToUpper(p.current.Value) == "ROW" || strings.ToUpper(p.current.Value) == "POLICY") {
			// SHOW CREATE ROW POLICY or SHOW CREATE POLICY
			return p.parseShowCreateRowPolicy(pos)
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "ROLE" {
			// SHOW CREATE ROLE
			return p.parseShowCreateRole(pos)
		} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "DICTIONARY" {
			show.ShowType = ast.ShowCreateDictionary
			p.nextToken()
		} else if p.currentIs(token.VIEW) {
			show.ShowType = ast.ShowCreateView
			p.nextToken()
		} else if p.currentIs(token.USER) {
			show.ShowType = ast.ShowCreateUser
			p.nextToken()
			// Skip user name and host pattern until FORMAT or end
			for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.FORMAT) {
				p.nextToken()
			}
			// Parse FORMAT clause if present
			if p.currentIs(token.FORMAT) {
				p.nextToken()
				if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
					show.Format = p.current.Value
					p.nextToken()
				}
			}
		} else {
			show.ShowType = ast.ShowCreate
			// Handle SHOW CREATE TABLE, SHOW CREATE TEMPORARY TABLE, etc.
			if p.currentIs(token.TEMPORARY) {
				p.nextToken()
			}
			if p.currentIs(token.TABLE) {
				p.nextToken()
			}
		}
	case token.SETTINGS:
		show.ShowType = ast.ShowSettings
		p.nextToken()
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

	// Parse FROM clause (or table/database name for SHOW CREATE TABLE/DATABASE/DICTIONARY/VIEW)
	showCreateTypes := show.ShowType == ast.ShowCreate || show.ShowType == ast.ShowCreateDB || show.ShowType == ast.ShowCreateDictionary || show.ShowType == ast.ShowCreateView
	if p.currentIs(token.FROM) || (showCreateTypes && (p.currentIs(token.IDENT) || p.current.Token.IsKeyword())) {
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

	// Parse NOT LIKE, LIKE or ILIKE clause
	if p.currentIs(token.NOT) {
		p.nextToken()
		if p.currentIs(token.LIKE) || p.currentIs(token.ILIKE) {
			p.nextToken()
			if p.currentIs(token.STRING) {
				// NOT LIKE - store the pattern with a prefix to indicate negation
				show.Like = "!" + p.current.Value // Using ! prefix to indicate NOT LIKE
				p.nextToken()
			}
		}
	} else if p.currentIs(token.LIKE) || p.currentIs(token.ILIKE) {
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

	// Parse FORMAT clause
	if p.currentIs(token.FORMAT) {
		p.nextToken()
		if p.currentIs(token.IDENT) || p.current.Token.IsKeyword() {
			show.Format = p.current.Value
			p.nextToken()
		}
	}

	// Parse SETTINGS clause
	if p.currentIs(token.SETTINGS) {
		show.HasSettings = true
		// Skip SETTINGS and all settings key-value pairs
		p.nextToken()
		for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) && !p.currentIs(token.FORMAT) {
			p.nextToken()
		}
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
			explain.ExplicitType = true
			p.nextToken()
		case "SYNTAX":
			explain.ExplainType = ast.ExplainSyntax
			explain.ExplicitType = true
			p.nextToken()
		case "PLAN":
			explain.ExplainType = ast.ExplainPlan
			explain.ExplicitType = true
			p.nextToken()
		case "PIPELINE":
			explain.ExplainType = ast.ExplainPipeline
			explain.ExplicitType = true
			p.nextToken()
		case "ESTIMATE":
			explain.ExplainType = ast.ExplainEstimate
			explain.ExplicitType = true
			p.nextToken()
		case "QUERY":
			// EXPLAIN QUERY TREE
			p.nextToken()
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "TREE" {
				p.nextToken()
			}
			explain.ExplainType = ast.ExplainQueryTree
			explain.ExplicitType = true
		case "CURRENT":
			// EXPLAIN CURRENT TRANSACTION
			p.nextToken()
			if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "TRANSACTION" {
				p.nextToken()
			}
			explain.ExplainType = ast.ExplainCurrentTransaction
			explain.ExplicitType = true
			return explain // No statement follows CURRENT TRANSACTION
		default:
			explain.ExplainType = ast.ExplainPlan
		}
	}

	// Parse EXPLAIN options (e.g., header = 1, optimize = 0)
	// These come before the actual statement
	// Options can be identifiers or keywords like OPTIMIZE followed by =
	var optionParts []string
	for p.peekIs(token.EQ) && !p.currentIs(token.SELECT) && !p.currentIs(token.WITH) {
		// This is an option (name = value)
		explain.HasSettings = true
		optionName := p.current.Value
		p.nextToken() // skip option name
		p.nextToken() // skip =
		// Get the value
		optionValue := p.current.Value
		if p.currentIs(token.NUMBER) || p.currentIs(token.STRING) || p.currentIs(token.IDENT) {
			optionParts = append(optionParts, optionName+" = "+optionValue)
		}
		p.parseExpression(LOWEST) // skip value expression (may consume more tokens)
		// Skip comma if present
		if p.currentIs(token.COMMA) {
			p.nextToken()
		}
	}
	if len(optionParts) > 0 {
		explain.OptionsString = strings.Join(optionParts, ", ")
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

func (p *Parser) parseSetRole() *ast.SetRoleQuery {
	query := &ast.SetRoleQuery{
		Position: p.current.Pos,
	}

	// Skip SET DEFAULT ROLE ... TO ...
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return query
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

	// Handle CLEANUP
	if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "CLEANUP" {
		opt.Cleanup = true
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
	// Stop when we see:
	// 1. An IDENT followed by DOT (qualified table name like sqllt.table)
	// 2. A plain IDENT (not a command keyword) followed by end-of-statement (SEMICOLON, EOF, FORMAT),
	//    UNLESS the previous part was FAILPOINT (failpoint names are part of the command)
	var parts []string
	for p.currentIs(token.IDENT) || p.isSystemCommandKeyword() {
		// Check if this IDENT is followed by DOT - if so, it's a table name, not part of the command
		if p.currentIs(token.IDENT) && p.peekIs(token.DOT) {
			break
		}
		// Check if this is a plain IDENT (not a command keyword) followed by end-of-statement
		// This indicates it's likely a table name, not part of the command
		// Exception: after FAILPOINT, the identifier is the failpoint name (part of command)
		if p.currentIs(token.IDENT) && !p.isSystemCommandKeyword() {
			if p.peekIs(token.SEMICOLON) || p.peekIs(token.EOF) || p.peekIs(token.FORMAT) {
				// Check if previous part was FAILPOINT or FOR - these are followed by identifiers that are part of command
				if len(parts) > 0 {
					lastPart := strings.ToUpper(parts[len(parts)-1])
					if lastPart == "FAILPOINT" || lastPart == "FOR" {
						// This is a failpoint name or format name, consume it as part of command
						parts = append(parts, p.current.Value)
						p.nextToken()
						continue
					}
				}
				break
			}
		}
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

// isSystemCommandKeyword returns true if current token is a keyword/identifier that is part of SYSTEM command
// and should NOT be treated as a table name
func (p *Parser) isSystemCommandKeyword() bool {
	switch p.current.Token {
	case token.TTL, token.SYNC, token.DROP, token.FORMAT, token.FOR, token.INDEX, token.INSERT,
		token.PRIMARY, token.KEY:
		return true
	}
	// Handle identifiers that are part of SYSTEM commands (not table names)
	if p.currentIs(token.IDENT) {
		upper := strings.ToUpper(p.current.Value)
		switch upper {
		case "SCHEMA", "CACHE", "QUEUE",
			// FAILPOINT names are part of the command, not table names
			"FAILPOINT",
			// These are common parts of SYSTEM commands that end with identifiers
			"ENABLE", "DISABLE", "FLUSH", "RELOAD", "RESTART", "STOP", "START",
			// Parts of STOP/START commands
			"LISTEN", "LISTENING", "MOVES", "MERGES", "TTL", "SENDS", "FETCHES", "PULLING",
			"REPLICATED", "DISTRIBUTED", "CLEANUP",
			// RELOAD targets
			"DICTIONARIES", "DICTIONARY", "MODELS", "MODEL", "FUNCTIONS", "FUNCTION",
			"EMBEDDED", "CONFIG", "SYMBOLS", "ASYNCHRONOUS", "METRICS",
			// FLUSH/DROP targets
			"LOGS", "ASYNC", "UNCOMPRESSED", "COMPILED", "MARK", "QUERY", "MMAP",
			"DNS", "FILESYSTEM", "S3",
			// Other command parts
			"MUTATIONS", "REPLICATION", "QUEUES", "DDL", "REPLICAS", "REPLICA":
			return true
		}
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

	// Parse rename pairs (can have multiple: t1 TO t2, t3 TO t4, ...)
	for {
		pair := &ast.RenamePair{}

		// Parse from table name (can be qualified: database.table)
		fromName := p.parseIdentifierName()
		if p.currentIs(token.DOT) {
			p.nextToken()
			pair.FromDatabase = fromName
			pair.FromTable = p.parseIdentifierName()
		} else {
			pair.FromTable = fromName
		}

		if !p.expect(token.TO) {
			break
		}

		// Parse to table name (can be qualified: database.table)
		toName := p.parseIdentifierName()
		if p.currentIs(token.DOT) {
			p.nextToken()
			pair.ToDatabase = toName
			pair.ToTable = p.parseIdentifierName()
		} else {
			pair.ToTable = toName
		}

		rename.Pairs = append(rename.Pairs, pair)

		// Check for more pairs
		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	// Set legacy From/To fields for backward compatibility (first pair)
	if len(rename.Pairs) > 0 {
		first := rename.Pairs[0]
		if first.FromDatabase != "" {
			rename.From = first.FromDatabase + "." + first.FromTable
		} else {
			rename.From = first.FromTable
		}
		if first.ToDatabase != "" {
			rename.To = first.ToDatabase + "." + first.ToTable
		} else {
			rename.To = first.ToTable
		}
	}

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

	// Parse first table name (can be database.table)
	name1 := p.parseIdentifierName()
	if p.currentIs(token.DOT) {
		p.nextToken()
		exchange.Database1 = name1
		exchange.Table1 = p.parseIdentifierName()
	} else {
		exchange.Table1 = name1
	}

	if !p.expect(token.AND) {
		return nil
	}

	// Parse second table name (can be database.table)
	name2 := p.parseIdentifierName()
	if p.currentIs(token.DOT) {
		p.nextToken()
		exchange.Database2 = name2
		exchange.Table2 = p.parseIdentifierName()
	} else {
		exchange.Table2 = name2
	}

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

func (p *Parser) parseDetach() *ast.DetachQuery {
	detach := &ast.DetachQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip DETACH

	// Check for DATABASE keyword - if present, store in Database field
	isDatabase := false
	if p.currentIs(token.DATABASE) {
		isDatabase = true
		p.nextToken()
	} else if p.currentIs(token.TABLE) {
		p.nextToken()
	}

	// Parse name (can be qualified: database.table for TABLE, not for DATABASE)
	name := p.parseIdentifierName()
	if p.currentIs(token.DOT) && !isDatabase {
		p.nextToken()
		detach.Database = name
		detach.Table = p.parseIdentifierName()
	} else if isDatabase {
		detach.Database = name
	} else {
		detach.Table = name
	}

	return detach
}

func (p *Parser) parseAttach() *ast.AttachQuery {
	attach := &ast.AttachQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip ATTACH

	// Check for DATABASE keyword - if present, store in Database field
	isDatabase := false
	if p.currentIs(token.DATABASE) {
		isDatabase = true
		p.nextToken()
	} else if p.currentIs(token.TABLE) {
		p.nextToken()
	}

	// Parse name (can be qualified: database.table for TABLE, not for DATABASE)
	name := p.parseIdentifierName()
	if p.currentIs(token.DOT) && !isDatabase {
		p.nextToken()
		attach.Database = name
		attach.Table = p.parseIdentifierName()
	} else if isDatabase {
		attach.Database = name
	} else {
		attach.Table = name
	}

	return attach
}

func (p *Parser) parseCheck() *ast.CheckQuery {
	check := &ast.CheckQuery{
		Position: p.current.Pos,
	}

	p.nextToken() // skip CHECK

	// Skip optional TABLE keyword
	if p.currentIs(token.TABLE) {
		p.nextToken()
	}

	// Parse table name (can be qualified: database.table)
	tableName := p.parseIdentifierName()
	if p.currentIs(token.DOT) {
		p.nextToken()
		check.Database = tableName
		check.Table = p.parseIdentifierName()
	} else {
		check.Table = tableName
	}

	// Parse optional FORMAT
	if p.currentIs(token.FORMAT) {
		p.nextToken() // skip FORMAT
		check.Format = p.parseIdentifierName()
	}

	// Parse optional SETTINGS
	if p.currentIs(token.SETTINGS) {
		p.nextToken() // skip SETTINGS
		check.Settings = p.parseSettingsList()
	}

	return check
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

		// Parse frame specification (ROWS/RANGE/GROUPS)
		if p.currentIs(token.IDENT) {
			frameType := strings.ToUpper(p.current.Value)
			if frameType == "ROWS" || frameType == "RANGE" || frameType == "GROUPS" {
				spec.Frame = p.parseWindowFrame()
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

	// Handle parameterized identifiers like {CLICKHOUSE_DATABASE:Identifier}
	if p.currentIs(token.PARAM) {
		name = "{" + p.current.Value + "}"
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

// parseFromSelectSyntax handles ClickHouse's FROM ... SELECT syntax
// e.g., FROM numbers(1) SELECT number
func (p *Parser) parseFromSelectSyntax() *ast.SelectWithUnionQuery {
	query := &ast.SelectWithUnionQuery{
		Position: p.current.Pos,
	}

	sel := &ast.SelectQuery{
		Position: p.current.Pos,
	}

	// Skip FROM
	p.nextToken()

	// Parse table expression
	sel.From = p.parseTablesInSelect()

	// Parse SELECT
	if !p.expect(token.SELECT) {
		return nil
	}

	// Handle DISTINCT
	if p.currentIs(token.DISTINCT) {
		sel.Distinct = true
		p.nextToken()
	}

	// Parse column list
	sel.Columns = p.parseExpressionList()

	// Continue parsing the rest of SELECT (WHERE, GROUP BY, etc.)
	p.parseSelectRemainder(sel)

	query.Selects = append(query.Selects, sel)
	return query
}

// parseSelectRemainder parses the remainder of a SELECT after columns
func (p *Parser) parseSelectRemainder(sel *ast.SelectQuery) {
	// Parse WHERE clause
	if p.currentIs(token.WHERE) {
		p.nextToken()
		sel.Where = p.parseExpression(LOWEST)
	}

	// Parse GROUP BY clause
	if p.currentIs(token.GROUP) {
		p.nextToken()
		if p.expect(token.BY) {
			sel.GroupBy = p.parseExpressionList()
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
		if p.expect(token.BY) {
			sel.OrderBy = p.parseOrderByList()
		}
	}

	// Parse LIMIT clause
	if p.currentIs(token.LIMIT) {
		p.nextToken()
		sel.Limit = p.parseExpression(LOWEST)
	}

	// Parse SETTINGS clause
	if p.currentIs(token.SETTINGS) {
		p.nextToken()
		sel.Settings = p.parseSettingsList()
	}
}

// parseParenthesizedSelect handles (SELECT ...) at statement level
func (p *Parser) parseParenthesizedSelect() *ast.SelectWithUnionQuery {
	pos := p.current.Pos
	p.nextToken() // skip (

	// Check if this is actually a SELECT statement
	if !p.currentIs(token.SELECT) && !p.currentIs(token.WITH) {
		// Not a SELECT, just skip until we find closing paren
		depth := 1
		for depth > 0 && !p.currentIs(token.EOF) {
			if p.currentIs(token.LPAREN) {
				depth++
			} else if p.currentIs(token.RPAREN) {
				depth--
			}
			if depth > 0 {
				p.nextToken()
			}
		}
		if p.currentIs(token.RPAREN) {
			p.nextToken()
		}
		return &ast.SelectWithUnionQuery{Position: pos}
	}

	// Parse the inner query
	inner := p.parseSelectWithUnion()

	p.expect(token.RPAREN)

	// Wrap the result
	query := &ast.SelectWithUnionQuery{
		Position: pos,
	}
	if inner != nil {
		for _, s := range inner.Selects {
			query.Selects = append(query.Selects, s)
		}
		query.UnionModes = inner.UnionModes
		query.UnionAll = inner.UnionAll
	}

	return query
}

// parseExistsStatement handles EXISTS table_name syntax
func (p *Parser) parseExistsStatement() *ast.ExistsQuery {
	exists := &ast.ExistsQuery{
		Position:   p.current.Pos,
		ExistsType: ast.ExistsTable, // default to TABLE
	}

	p.nextToken() // skip EXISTS

	// Check for DICTIONARY, DATABASE, VIEW, or TABLE keyword
	if p.currentIs(token.TABLE) {
		exists.ExistsType = ast.ExistsTable
		p.nextToken()
	} else if p.currentIs(token.DATABASE) {
		exists.ExistsType = ast.ExistsDatabase
		p.nextToken()
	} else if p.currentIs(token.VIEW) {
		exists.ExistsType = ast.ExistsView
		p.nextToken()
	} else if p.currentIs(token.IDENT) && strings.ToUpper(p.current.Value) == "DICTIONARY" {
		exists.ExistsType = ast.ExistsDictionary
		p.nextToken()
	}

	// Parse table/database/dictionary/view name
	name := p.parseIdentifierName()
	if name != "" {
		if p.currentIs(token.DOT) {
			p.nextToken()
			exists.Database = name
			exists.Table = p.parseIdentifierName()
		} else {
			exists.Table = name
		}
	}

	return exists
}

// parseProjection parses a projection definition: name (SELECT ... [ORDER BY col] [GROUP BY ...])
func (p *Parser) parseProjection() *ast.Projection {
	proj := &ast.Projection{
		Position: p.current.Pos,
	}

	// Parse projection name
	if p.currentIs(token.IDENT) {
		proj.Name = p.current.Value
		p.nextToken()
	}

	// Parse (SELECT ...)
	if !p.currentIs(token.LPAREN) {
		return proj
	}
	p.nextToken() // skip (

	proj.Select = &ast.ProjectionSelectQuery{
		Position: p.current.Pos,
	}

	// Parse SELECT keyword (optional in projection)
	if p.currentIs(token.SELECT) {
		p.nextToken()
	}

	// Parse column list
	for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) {
		// Check for GROUP BY or ORDER BY
		if p.currentIs(token.GROUP) || p.currentIs(token.ORDER) {
			break
		}

		col := p.parseExpression(LOWEST)
		if col != nil {
			proj.Select.Columns = append(proj.Select.Columns, col)
		}

		if p.currentIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	// Parse GROUP BY
	if p.currentIs(token.GROUP) {
		p.nextToken() // GROUP
		if p.currentIs(token.BY) {
			p.nextToken() // BY
		}
		for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) && !p.currentIs(token.ORDER) {
			expr := p.parseExpression(LOWEST)
			if expr != nil {
				proj.Select.GroupBy = append(proj.Select.GroupBy, expr)
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
	}

	// Parse ORDER BY
	if p.currentIs(token.ORDER) {
		p.nextToken() // ORDER
		if p.currentIs(token.BY) {
			p.nextToken() // BY
		}
		// Parse ORDER BY columns (comma-separated expressions)
		for !p.currentIs(token.EOF) && !p.currentIs(token.RPAREN) {
			expr := p.parseExpression(LOWEST)
			if expr != nil {
				proj.Select.OrderBy = append(proj.Select.OrderBy, expr)
			} else {
				break
			}
			if p.currentIs(token.COMMA) {
				p.nextToken()
			} else {
				break
			}
		}
	}

	// Skip closing paren
	if p.currentIs(token.RPAREN) {
		p.nextToken()
	}

	return proj
}

// parseGrant handles GRANT statements
func (p *Parser) parseGrant() *ast.GrantQuery {
	grant := &ast.GrantQuery{
		Position: p.current.Pos,
		IsRevoke: false,
	}

	// Skip all tokens until end of statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return grant
}

// parseRevoke handles REVOKE statements
func (p *Parser) parseRevoke() *ast.GrantQuery {
	grant := &ast.GrantQuery{
		Position: p.current.Pos,
		IsRevoke: true,
	}

	// Skip all tokens until end of statement
	for !p.currentIs(token.EOF) && !p.currentIs(token.SEMICOLON) {
		p.nextToken()
	}

	return grant
}

// parseTransactionControl handles BEGIN, COMMIT, ROLLBACK, and SET TRANSACTION SNAPSHOT statements
func (p *Parser) parseTransactionControl() *ast.TransactionControlQuery {
	query := &ast.TransactionControlQuery{
		Position: p.current.Pos,
	}

	switch p.current.Token {
	case token.BEGIN:
		query.Action = "BEGIN"
		p.nextToken() // skip BEGIN
		// Skip optional TRANSACTION keyword
		if p.currentIs(token.TRANSACTION) {
			p.nextToken()
		}
	case token.COMMIT:
		query.Action = "COMMIT"
		p.nextToken() // skip COMMIT
	case token.ROLLBACK:
		query.Action = "ROLLBACK"
		p.nextToken() // skip ROLLBACK
	case token.SET:
		p.nextToken() // skip SET
		if p.currentIs(token.TRANSACTION) {
			p.nextToken() // skip TRANSACTION
			if p.currentIs(token.SNAPSHOT) {
				p.nextToken() // skip SNAPSHOT
				query.Action = "SET_SNAPSHOT"
				// Parse snapshot number
				if p.currentIs(token.NUMBER) {
					// Parse the number value
					val, err := strconv.ParseInt(p.current.Value, 10, 64)
					if err == nil {
						query.Snapshot = val
					}
					p.nextToken()
				}
			}
		}
	}

	return query
}
