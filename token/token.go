// Package token defines constants representing the lexical tokens of ClickHouse SQL.
package token

// Token represents a lexical token.
type Token int

const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	WHITESPACE
	COMMENT

	// Literals
	IDENT   // identifiers
	NUMBER  // integer or float literals
	STRING  // string literals
	PARAM   // parameter placeholders like {name:Type}

	// Operators
	PLUS       // +
	MINUS      // -
	ASTERISK   // *
	SLASH      // /
	PERCENT    // %
	EQ         // =
	NEQ        // != or <>
	LT         // <
	GT         // >
	LTE        // <=
	GTE        // >=
	CONCAT     // ||
	ARROW        // ->
	COLONCOLON   // ::
	NULL_SAFE_EQ // <=>
	CARET        // ^

	// Delimiters
	LPAREN    // (
	RPAREN    // )
	LBRACKET  // [
	RBRACKET  // ]
	LBRACE    // {
	RBRACE    // }
	COMMA     // ,
	DOT       // .
	SEMICOLON // ;
	COLON     // :
	QUESTION  // ?

	// Keywords
	keyword_beg
	ADD
	ALIAS
	ALL
	ALTER
	AND
	ANTI
	ANY
	APPLY
	ARRAY
	AS
	ASC
	ASOF
	ATTACH
	BETWEEN
	BOTH
	BY
	CASE
	CAST
	CHECK
	CLUSTER
	COLLATE
	COLUMN
	COLUMNS
	CONSTRAINT
	CREATE
	CROSS
	CUBE
	DATABASE
	DATABASES
	DEFAULT
	DELETE
	DESC
	DESCRIBE
	DETACH
	DISTINCT
	DISTRIBUTED
	DIV
	DROP
	ELSE
	END
	ENGINE
	EXCEPT
	EXCHANGE
	EXISTS
	EXPLAIN
	EXTRACT
	FALSE
	FETCH
	FILL
	FINAL
	FIRST
	FREEZE
	FOR
	FORMAT
	FROM
	FULL
	FUNCTION
	GLOBAL
	GRANT
	GROUP
	GROUPING
	HAVING
	IF
	ILIKE
	IN
	INDEX
	INF
	INNER
	INSERT
	INTERVAL
	INTO
	IS
	JOIN
	KEY
	KILL
	LEADING
	LEFT
	LIKE
	LIMIT
	LAST
	LIVE
	LOCAL
	MATERIALIZED
	MOD
	MODIFY
	NAN
	NATURAL
	NOT
	NULL
	NULLS
	OFFSET
	ON
	OPTIMIZE
	OR
	ORDER
	OUTER
	OUTFILE
	OVER
	PARTITION
	PASTE
	POPULATE
	PREWHERE
	PRIMARY
	QUALIFY
	REGEXP
	RENAME
	REPLACE
	REVOKE
	RIGHT
	ROLLUP
	SAMPLE
	SELECT
	SEMI
	SET
	SETS
	SETTINGS
	SHOW
	STEP
	SUBSTRING
	SYNC
	SYSTEM
	TABLE
	TABLES
	TEMPORARY
	THEN
	TIES
	TO
	TOP
	TOTALS
	TRAILING
	TRIM
	TRUE
	TRUNCATE
	TTL
	UNION
	UPDATE
	USE
	USER
	USING
	VALUES
	VIEW
	WATCH
	WHEN
	WHERE
	WINDOW
	WITH
	keyword_end
)

var tokens = [...]string{
	ILLEGAL:    "ILLEGAL",
	EOF:        "EOF",
	WHITESPACE: "WHITESPACE",
	COMMENT:    "COMMENT",

	IDENT:  "IDENT",
	NUMBER: "NUMBER",
	STRING: "STRING",
	PARAM:  "PARAM",

	PLUS:       "+",
	MINUS:      "-",
	ASTERISK:   "*",
	SLASH:      "/",
	PERCENT:    "%",
	EQ:         "=",
	NEQ:        "!=",
	LT:         "<",
	GT:         ">",
	LTE:        "<=",
	GTE:        ">=",
	CONCAT:     "||",
	ARROW:        "->",
	COLONCOLON:   "::",
	NULL_SAFE_EQ: "<=>",
	CARET:        "^",

	LPAREN:    "(",
	RPAREN:    ")",
	LBRACKET:  "[",
	RBRACKET:  "]",
	LBRACE:    "{",
	RBRACE:    "}",
	COMMA:     ",",
	DOT:       ".",
	SEMICOLON: ";",
	COLON:     ":",
	QUESTION:  "?",

	ADD:          "ADD",
	ALIAS:        "ALIAS",
	ALL:          "ALL",
	ALTER:        "ALTER",
	AND:          "AND",
	ANTI:         "ANTI",
	ANY:          "ANY",
	APPLY:        "APPLY",
	ARRAY:        "ARRAY",
	AS:           "AS",
	ASC:          "ASC",
	ASOF:         "ASOF",
	ATTACH:       "ATTACH",
	BETWEEN:      "BETWEEN",
	BOTH:         "BOTH",
	BY:           "BY",
	CASE:         "CASE",
	CAST:         "CAST",
	CHECK:        "CHECK",
	CLUSTER:      "CLUSTER",
	COLLATE:      "COLLATE",
	COLUMN:       "COLUMN",
	COLUMNS:      "COLUMNS",
	CONSTRAINT:   "CONSTRAINT",
	CREATE:       "CREATE",
	CROSS:        "CROSS",
	CUBE:         "CUBE",
	DATABASE:     "DATABASE",
	DATABASES:    "DATABASES",
	DEFAULT:      "DEFAULT",
	DELETE:       "DELETE",
	DESC:         "DESC",
	DESCRIBE:     "DESCRIBE",
	DETACH:       "DETACH",
	DISTINCT:     "DISTINCT",
	DISTRIBUTED:  "DISTRIBUTED",
	DIV:          "DIV",
	DROP:         "DROP",
	ELSE:         "ELSE",
	END:          "END",
	ENGINE:       "ENGINE",
	EXCEPT:       "EXCEPT",
	EXCHANGE:     "EXCHANGE",
	EXISTS:       "EXISTS",
	EXPLAIN:      "EXPLAIN",
	EXTRACT:      "EXTRACT",
	FALSE:        "FALSE",
	FETCH:        "FETCH",
	FILL:         "FILL",
	FINAL:        "FINAL",
	FIRST:        "FIRST",
	FREEZE:       "FREEZE",
	FOR:          "FOR",
	FORMAT:       "FORMAT",
	FROM:         "FROM",
	FULL:         "FULL",
	FUNCTION:     "FUNCTION",
	GLOBAL:       "GLOBAL",
	GRANT:        "GRANT",
	GROUP:        "GROUP",
	GROUPING:     "GROUPING",
	HAVING:       "HAVING",
	IF:           "IF",
	ILIKE:        "ILIKE",
	IN:           "IN",
	INDEX:        "INDEX",
	INF:          "INF",
	INNER:        "INNER",
	INSERT:       "INSERT",
	INTERVAL:     "INTERVAL",
	INTO:         "INTO",
	IS:           "IS",
	JOIN:         "JOIN",
	KEY:          "KEY",
	KILL:         "KILL",
	LAST:         "LAST",
	LEADING:      "LEADING",
	LEFT:         "LEFT",
	LIKE:         "LIKE",
	LIMIT:        "LIMIT",
	LIVE:         "LIVE",
	LOCAL:        "LOCAL",
	MATERIALIZED: "MATERIALIZED",
	MOD:          "MOD",
	MODIFY:       "MODIFY",
	NAN:          "NAN",
	NATURAL:      "NATURAL",
	NOT:          "NOT",
	NULL:         "NULL",
	NULLS:        "NULLS",
	OFFSET:       "OFFSET",
	ON:           "ON",
	OPTIMIZE:     "OPTIMIZE",
	OR:           "OR",
	ORDER:        "ORDER",
	OUTER:        "OUTER",
	OUTFILE:      "OUTFILE",
	OVER:         "OVER",
	PARTITION:    "PARTITION",
	PASTE:        "PASTE",
	POPULATE:     "POPULATE",
	PREWHERE:     "PREWHERE",
	PRIMARY:      "PRIMARY",
	QUALIFY:      "QUALIFY",
	REGEXP:       "REGEXP",
	RENAME:       "RENAME",
	REPLACE:      "REPLACE",
	REVOKE:       "REVOKE",
	RIGHT:        "RIGHT",
	ROLLUP:       "ROLLUP",
	SAMPLE:       "SAMPLE",
	SELECT:       "SELECT",
	SEMI:         "SEMI",
	SET:          "SET",
	SETS:         "SETS",
	SETTINGS:     "SETTINGS",
	SHOW:         "SHOW",
	STEP:         "STEP",
	SUBSTRING:    "SUBSTRING",
	SYNC:         "SYNC",
	SYSTEM:       "SYSTEM",
	TABLE:        "TABLE",
	TABLES:       "TABLES",
	TEMPORARY:    "TEMPORARY",
	THEN:         "THEN",
	TIES:         "TIES",
	TO:           "TO",
	TOP:          "TOP",
	TOTALS:       "TOTALS",
	TRAILING:     "TRAILING",
	TRIM:         "TRIM",
	TRUE:         "TRUE",
	TRUNCATE:     "TRUNCATE",
	TTL:          "TTL",
	UNION:        "UNION",
	UPDATE:       "UPDATE",
	USE:          "USE",
	USER:         "USER",
	USING:        "USING",
	VALUES:       "VALUES",
	VIEW:         "VIEW",
	WATCH:        "WATCH",
	WHEN:         "WHEN",
	WHERE:        "WHERE",
	WINDOW:       "WINDOW",
	WITH:         "WITH",
}

func (tok Token) String() string {
	if tok >= 0 && int(tok) < len(tokens) {
		return tokens[tok]
	}
	return ""
}

// Keywords maps keyword strings to their token types.
var Keywords map[string]Token

func init() {
	Keywords = make(map[string]Token)
	for i := keyword_beg + 1; i < keyword_end; i++ {
		Keywords[tokens[i]] = i
	}
}

// Lookup returns the token type for an identifier string.
// If the string is a keyword, it returns the keyword token.
// Otherwise, it returns IDENT.
func Lookup(ident string) Token {
	if tok, ok := Keywords[ident]; ok {
		return tok
	}
	return IDENT
}

// IsKeyword returns true if the token is a keyword.
func (tok Token) IsKeyword() bool {
	return tok > keyword_beg && tok < keyword_end
}

// Position represents a source position.
type Position struct {
	Offset int // byte offset
	Line   int // line number (1-based)
	Column int // column number (1-based)
}
