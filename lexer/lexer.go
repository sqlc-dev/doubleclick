// Package lexer implements a lexer for ClickHouse SQL.
package lexer

import (
	"bufio"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/sqlc-dev/doubleclick/token"
)

// Lexer tokenizes ClickHouse SQL input.
type Lexer struct {
	reader *bufio.Reader
	ch     rune   // current character
	pos    token.Position
	eof    bool
}

// Item represents a lexical token with its value and position.
type Item struct {
	Token  token.Token
	Value  string
	Pos    token.Position
	Quoted bool // true if this identifier was double-quoted
}

// New creates a new Lexer from an io.Reader.
func New(r io.Reader) *Lexer {
	l := &Lexer{
		reader: bufio.NewReader(r),
		pos:    token.Position{Offset: 0, Line: 1, Column: 0},
	}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.eof {
		l.ch = 0
		return
	}

	r, size, err := l.reader.ReadRune()
	if err != nil {
		l.ch = 0
		l.eof = true
		return
	}

	if l.ch == '\n' {
		l.pos.Line++
		l.pos.Column = 1
	} else {
		l.pos.Column++
	}
	l.pos.Offset += size
	l.ch = r
}

func (l *Lexer) peekChar() rune {
	if l.eof {
		return 0
	}
	bytes, err := l.reader.Peek(1)
	if err != nil || len(bytes) == 0 {
		return 0
	}
	r, _ := utf8.DecodeRune(bytes)
	return r
}

func (l *Lexer) skipWhitespace() {
	// Skip whitespace and BOM (byte order mark U+FEFF)
	for unicode.IsSpace(l.ch) || l.ch == '\uFEFF' {
		l.readChar()
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Item {
	l.skipWhitespace()

	pos := l.pos

	if l.eof || l.ch == 0 {
		return Item{Token: token.EOF, Value: "", Pos: pos}
	}

	// Handle comments
	if l.ch == '-' && l.peekChar() == '-' {
		return l.readLineComment()
	}
	if l.ch == '#' {
		return l.readHashComment()
	}
	if l.ch == '/' && l.peekChar() == '*' {
		return l.readBlockComment()
	}
	// Unicode minus (U+2212) is treated as starting a line comment
	// ClickHouse doesn't recognize it as an operator
	if l.ch == '\u2212' {
		return l.readUnicodeMinusComment()
	}

	switch l.ch {
	case '+':
		l.readChar()
		return Item{Token: token.PLUS, Value: "+", Pos: pos}
	case '-':
		if l.peekChar() == '>' {
			l.readChar()
			l.readChar()
			return Item{Token: token.ARROW, Value: "->", Pos: pos}
		}
		l.readChar()
		return Item{Token: token.MINUS, Value: "-", Pos: pos}
	case '*':
		l.readChar()
		return Item{Token: token.ASTERISK, Value: "*", Pos: pos}
	case '/':
		l.readChar()
		return Item{Token: token.SLASH, Value: "/", Pos: pos}
	case '%':
		l.readChar()
		return Item{Token: token.PERCENT, Value: "%", Pos: pos}
	case '=':
		l.readChar()
		if l.ch == '=' {
			l.readChar()
			return Item{Token: token.EQ, Value: "==", Pos: pos}
		}
		return Item{Token: token.EQ, Value: "=", Pos: pos}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Item{Token: token.NEQ, Value: "!=", Pos: pos}
		}
		l.readChar()
		return Item{Token: token.ILLEGAL, Value: "!", Pos: pos}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			// Check for <=>
			if l.ch == '>' {
				l.readChar()
				return Item{Token: token.NULL_SAFE_EQ, Value: "<=>", Pos: pos}
			}
			return Item{Token: token.LTE, Value: "<=", Pos: pos}
		}
		if l.peekChar() == '>' {
			l.readChar()
			l.readChar()
			return Item{Token: token.NEQ, Value: "<>", Pos: pos}
		}
		l.readChar()
		return Item{Token: token.LT, Value: "<", Pos: pos}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Item{Token: token.GTE, Value: ">=", Pos: pos}
		}
		l.readChar()
		return Item{Token: token.GT, Value: ">", Pos: pos}
	case '|':
		if l.peekChar() == '|' {
			l.readChar()
			l.readChar()
			return Item{Token: token.CONCAT, Value: "||", Pos: pos}
		}
		l.readChar()
		return Item{Token: token.ILLEGAL, Value: "|", Pos: pos}
	case ':':
		if l.peekChar() == ':' {
			l.readChar()
			l.readChar()
			return Item{Token: token.COLONCOLON, Value: "::", Pos: pos}
		}
		l.readChar()
		return Item{Token: token.COLON, Value: ":", Pos: pos}
	case '(':
		l.readChar()
		return Item{Token: token.LPAREN, Value: "(", Pos: pos}
	case ')':
		l.readChar()
		return Item{Token: token.RPAREN, Value: ")", Pos: pos}
	case '[':
		l.readChar()
		return Item{Token: token.LBRACKET, Value: "[", Pos: pos}
	case ']':
		l.readChar()
		return Item{Token: token.RBRACKET, Value: "]", Pos: pos}
	case '{':
		return l.readParameter()
	case '}':
		l.readChar()
		return Item{Token: token.RBRACE, Value: "}", Pos: pos}
	case ',':
		l.readChar()
		return Item{Token: token.COMMA, Value: ",", Pos: pos}
	case '.':
		if unicode.IsDigit(l.peekChar()) {
			return l.readNumber()
		}
		l.readChar()
		return Item{Token: token.DOT, Value: ".", Pos: pos}
	case ';':
		l.readChar()
		return Item{Token: token.SEMICOLON, Value: ";", Pos: pos}
	case '?':
		l.readChar()
		return Item{Token: token.QUESTION, Value: "?", Pos: pos}
	case '^':
		l.readChar()
		return Item{Token: token.CARET, Value: "^", Pos: pos}
	case '$':
		// Dollar-quoted strings: $$...$$
		if l.peekChar() == '$' {
			return l.readDollarQuotedString()
		}
		// Otherwise $ starts an identifier (e.g., $alias$name$)
		return l.readDollarIdentifier()
	case '\'':
		return l.readString('\'')
	case '\u2018', '\u2019': // Unicode curly single quotes ' '
		return l.readUnicodeString(l.ch)
	case '"':
		return l.readQuotedIdentifier()
	case '\u201C', '\u201D': // Unicode curly double quotes " "
		return l.readUnicodeQuotedIdentifier(l.ch)
	case '`':
		return l.readBacktickIdentifier()
	case '@':
		// Handle @@ system variables and @ for user@host syntax
		if l.peekChar() == '@' {
			l.readChar() // skip first @
			l.readChar() // skip second @
			// Read the variable name
			if isIdentStart(l.ch) || unicode.IsDigit(l.ch) {
				var sb strings.Builder
				sb.WriteString("@@")
				for isIdentChar(l.ch) {
					sb.WriteRune(l.ch)
					l.readChar()
				}
				return Item{Token: token.IDENT, Value: sb.String(), Pos: pos}
			}
			return Item{Token: token.IDENT, Value: "@@", Pos: pos}
		}
		// Single @ - used in user@host syntax, return as IDENT
		l.readChar()
		return Item{Token: token.IDENT, Value: "@", Pos: pos}
	default:
		if unicode.IsDigit(l.ch) {
			// Check if this is a number or an identifier starting with digits
			// In ClickHouse, identifiers like "02422_data" start with digits
			return l.readNumberOrIdent()
		}
		if isIdentStart(l.ch) {
			return l.readIdentifier()
		}
		ch := l.ch
		l.readChar()
		return Item{Token: token.ILLEGAL, Value: string(ch), Pos: pos}
	}
}

func (l *Lexer) readLineComment() Item {
	pos := l.pos
	var sb strings.Builder
	// Skip --
	sb.WriteRune(l.ch)
	l.readChar()
	sb.WriteRune(l.ch)
	l.readChar()

	for l.ch != '\n' && l.ch != 0 && !l.eof {
		sb.WriteRune(l.ch)
		l.readChar()
	}
	return Item{Token: token.COMMENT, Value: sb.String(), Pos: pos}
}

func (l *Lexer) readHashComment() Item {
	pos := l.pos
	var sb strings.Builder
	// Skip #
	sb.WriteRune(l.ch)
	l.readChar()

	for l.ch != '\n' && l.ch != 0 && !l.eof {
		sb.WriteRune(l.ch)
		l.readChar()
	}
	return Item{Token: token.COMMENT, Value: sb.String(), Pos: pos}
}

// readUnicodeMinusComment reads from a unicode minus (U+2212) to the end of line or semicolon.
// ClickHouse doesn't recognize unicode minus as an operator, so we treat it as a comment.
func (l *Lexer) readUnicodeMinusComment() Item {
	pos := l.pos
	var sb strings.Builder
	// Skip −
	sb.WriteRune(l.ch)
	l.readChar()

	for l.ch != '\n' && l.ch != ';' && l.ch != 0 && !l.eof {
		sb.WriteRune(l.ch)
		l.readChar()
	}
	return Item{Token: token.COMMENT, Value: sb.String(), Pos: pos}
}

func (l *Lexer) readBlockComment() Item {
	pos := l.pos
	var sb strings.Builder
	// Skip /*
	sb.WriteRune(l.ch)
	l.readChar()
	sb.WriteRune(l.ch)
	l.readChar()

	// Track nesting level for nested comments (ClickHouse supports nested /* */ comments)
	nesting := 1

	for !l.eof && nesting > 0 {
		if l.ch == '*' && l.peekChar() == '/' {
			sb.WriteRune(l.ch)
			l.readChar()
			sb.WriteRune(l.ch)
			l.readChar()
			nesting--
		} else if l.ch == '/' && l.peekChar() == '*' {
			sb.WriteRune(l.ch)
			l.readChar()
			sb.WriteRune(l.ch)
			l.readChar()
			nesting++
		} else {
			sb.WriteRune(l.ch)
			l.readChar()
		}
	}
	return Item{Token: token.COMMENT, Value: sb.String(), Pos: pos}
}

func (l *Lexer) readString(quote rune) Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening quote

	for !l.eof {
		if l.ch == quote {
			// Check for escaped quote (e.g., '' becomes ')
			if l.peekChar() == quote {
				sb.WriteRune(l.ch) // Write one quote (the escaped result)
				l.readChar()       // skip first quote
				l.readChar()       // skip second quote
				continue
			}
			l.readChar() // skip closing quote
			break
		}
		if l.ch == '\\' {
			l.readChar() // consume backslash
			if l.eof {
				break
			}
			// Interpret escape sequence
			switch l.ch {
			case '\'':
				sb.WriteRune('\'')
			case '"':
				sb.WriteRune('"')
			case '\\':
				sb.WriteRune('\\')
			case 'n':
				sb.WriteRune('\n')
			case 't':
				sb.WriteRune('\t')
			case 'r':
				sb.WriteRune('\r')
			case '0':
				sb.WriteRune('\x00')
			case 'a':
				sb.WriteRune('\a')
			case 'b':
				sb.WriteRune('\b')
			case 'f':
				sb.WriteRune('\f')
			case 'v':
				sb.WriteRune('\v')
			case 'x':
				// Hex escape: \xNN
				l.readChar()
				if l.eof {
					break
				}
				hex1 := l.ch
				l.readChar()
				if l.eof {
					sb.WriteRune(rune(hexValue(hex1)))
					continue
				}
				hex2 := l.ch
				// Convert hex digits to byte
				val := hexValue(hex1)*16 + hexValue(hex2)
				sb.WriteByte(byte(val))
			default:
				// Unknown escape, preserve both the backslash and the character
				sb.WriteRune('\\')
				sb.WriteRune(l.ch)
			}
			l.readChar()
			continue
		}
		sb.WriteRune(l.ch)
		l.readChar()
	}
	return Item{Token: token.STRING, Value: sb.String(), Pos: pos}
}

func (l *Lexer) readHexString() Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening quote

	for !l.eof {
		if l.ch == '\'' {
			l.readChar() // skip closing quote
			break
		}
		// Read two hex digits and convert to byte
		hex1 := l.ch
		l.readChar()
		if l.eof || l.ch == '\'' {
			// Odd number of hex digits - write single value
			sb.WriteByte(byte(hexValue(hex1)))
			if l.ch == '\'' {
				l.readChar() // skip closing quote
			}
			break
		}
		hex2 := l.ch
		val := hexValue(hex1)*16 + hexValue(hex2)
		sb.WriteByte(byte(val))
		l.readChar()
	}
	return Item{Token: token.STRING, Value: sb.String(), Pos: pos}
}

func (l *Lexer) readQuotedIdentifier() Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening quote

	for !l.eof {
		if l.ch == '"' {
			// Check for SQL-style doubled quote escape ""
			l.readChar()
			if l.ch == '"' {
				// Doubled quote - add single quote and continue
				sb.WriteRune('"')
				l.readChar()
				continue
			}
			// Single quote - end of identifier
			break
		}
		if l.ch == '\\' {
			l.readChar()
			if !l.eof {
				sb.WriteRune(l.ch)
				l.readChar()
			}
			continue
		}
		sb.WriteRune(l.ch)
		l.readChar()
	}
	return Item{Token: token.IDENT, Value: sb.String(), Pos: pos, Quoted: true}
}

// readUnicodeString reads a string enclosed in Unicode curly quotes (' or ')
func (l *Lexer) readUnicodeString(openQuote rune) Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening quote

	// Unicode curly quotes: ' (U+2018) opens, ' (U+2019) closes
	closeQuote := '\u2019' // '
	if openQuote == '\u2019' {
		closeQuote = '\u2019'
	}

	for !l.eof && l.ch != closeQuote {
		sb.WriteRune(l.ch)
		l.readChar()
	}
	if l.ch == closeQuote {
		l.readChar() // skip closing quote
	}
	return Item{Token: token.STRING, Value: sb.String(), Pos: pos}
}

// readUnicodeQuotedIdentifier reads an identifier enclosed in Unicode curly double quotes (" or ")
func (l *Lexer) readUnicodeQuotedIdentifier(openQuote rune) Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening quote

	// Unicode curly double quotes: " (U+201C) opens, " (U+201D) closes
	closeQuote := '\u201D' // "
	if openQuote == '\u201D' {
		closeQuote = '\u201D'
	}

	for !l.eof && l.ch != closeQuote {
		sb.WriteRune(l.ch)
		l.readChar()
	}
	if l.ch == closeQuote {
		l.readChar() // skip closing quote
	}
	return Item{Token: token.IDENT, Value: sb.String(), Pos: pos, Quoted: true}
}

func (l *Lexer) readBacktickIdentifier() Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening backtick

	for !l.eof {
		if l.ch == '`' {
			// Check for escaped backtick (`` becomes `)
			if l.peekChar() == '`' {
				sb.WriteRune('`') // Write one backtick (the escaped result)
				l.readChar()      // skip first backtick
				l.readChar()      // skip second backtick
				continue
			}
			l.readChar() // skip closing backtick
			break
		}
		sb.WriteRune(l.ch)
		l.readChar()
	}
	return Item{Token: token.IDENT, Value: sb.String(), Pos: pos}
}

// readDollarQuotedString reads a dollar-quoted string $$...$$
func (l *Lexer) readDollarQuotedString() Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip first $
	l.readChar() // skip second $

	for !l.eof {
		if l.ch == '$' && l.peekChar() == '$' {
			l.readChar() // skip first $
			l.readChar() // skip second $
			break
		}
		sb.WriteRune(l.ch)
		l.readChar()
	}
	return Item{Token: token.STRING, Value: sb.String(), Pos: pos}
}

// readDollarIdentifier reads an identifier that starts with $ (e.g., $alias$name$)
func (l *Lexer) readDollarIdentifier() Item {
	pos := l.pos
	var sb strings.Builder
	// Include the initial $
	sb.WriteRune(l.ch)
	l.readChar()

	// Continue reading valid identifier characters (including $)
	for isIdentChar(l.ch) || l.ch == '$' {
		sb.WriteRune(l.ch)
		l.readChar()
	}

	return Item{Token: token.IDENT, Value: sb.String(), Pos: pos}
}

func (l *Lexer) readNumber() Item {
	pos := l.pos
	var sb strings.Builder

	// Handle leading dot for decimals like .5
	if l.ch == '.' {
		sb.WriteRune(l.ch)
		l.readChar()
	}

	// Check for hex (0x), binary (0b), or octal (0o) prefix
	if l.ch == '0' {
		sb.WriteRune(l.ch)
		l.readChar()
		if l.ch == 'x' || l.ch == 'X' {
			// Hex literal (may include P notation for floats: 0x1p4, 0x1.2p-3)
			sb.WriteRune(l.ch)
			l.readChar()
			for isHexDigit(l.ch) {
				sb.WriteRune(l.ch)
				l.readChar()
			}
			// Check for hex float decimal point
			if l.ch == '.' {
				sb.WriteRune(l.ch)
				l.readChar()
				for isHexDigit(l.ch) {
					sb.WriteRune(l.ch)
					l.readChar()
				}
			}
			// Check for P exponent (hex float notation)
			if l.ch == 'p' || l.ch == 'P' {
				sb.WriteRune(l.ch)
				l.readChar()
				if l.ch == '+' || l.ch == '-' {
					sb.WriteRune(l.ch)
					l.readChar()
				}
				for unicode.IsDigit(l.ch) {
					sb.WriteRune(l.ch)
					l.readChar()
				}
			}
			return Item{Token: token.NUMBER, Value: sb.String(), Pos: pos}
		} else if l.ch == 'b' || l.ch == 'B' {
			// Binary literal
			sb.WriteRune(l.ch)
			l.readChar()
			for l.ch == '0' || l.ch == '1' {
				sb.WriteRune(l.ch)
				l.readChar()
			}
			return Item{Token: token.NUMBER, Value: sb.String(), Pos: pos}
		} else if l.ch == 'o' || l.ch == 'O' {
			// Octal literal
			sb.WriteRune(l.ch)
			l.readChar()
			for l.ch >= '0' && l.ch <= '7' {
				sb.WriteRune(l.ch)
				l.readChar()
			}
			return Item{Token: token.NUMBER, Value: sb.String(), Pos: pos}
		}
		// Otherwise, continue with normal number parsing (leading 0)
	}

	// Read integer part (including underscores as separators, but only between digits)
	for unicode.IsDigit(l.ch) {
		sb.WriteRune(l.ch)
		l.readChar()
		// Handle underscore separators (only if followed by a digit)
		for l.ch == '_' && unicode.IsDigit(l.peekChar()) {
			l.readChar() // skip underscore
		}
	}

	// Check for decimal point (either followed by digit or end of number like 1.)
	if l.ch == '.' {
		nextCh := l.peekChar()
		// Allow 1. (trailing dot with no digits) and 1.5 (dot with digits)
		// But not 1.something (identifier-like)
		if unicode.IsDigit(nextCh) || (!isIdentStart(nextCh) && nextCh != '.') {
			sb.WriteRune(l.ch)
			l.readChar()
			for unicode.IsDigit(l.ch) {
				sb.WriteRune(l.ch)
				l.readChar()
				// Handle underscore separators
				for l.ch == '_' && unicode.IsDigit(l.peekChar()) {
					l.readChar()
				}
			}
		}
	}

	// Check for exponent
	if l.ch == 'e' || l.ch == 'E' {
		sb.WriteRune(l.ch)
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			sb.WriteRune(l.ch)
			l.readChar()
		}
		for unicode.IsDigit(l.ch) {
			sb.WriteRune(l.ch)
			l.readChar()
			// Handle underscore separators
			for l.ch == '_' && unicode.IsDigit(l.peekChar()) {
				l.readChar()
			}
		}
	}

	return Item{Token: token.NUMBER, Value: sb.String(), Pos: pos}
}

// readNumberOrIdent handles tokens that start with digits.
// In ClickHouse, identifiers can start with digits if followed by underscore and letters
// e.g., "02422_data" is a valid identifier
func (l *Lexer) readNumberOrIdent() Item {
	pos := l.pos
	var sb strings.Builder

	// Peek ahead to see if this will become an identifier
	// We need to look for pattern: digits followed by underscore followed by letter
	// Save position for potential rollback
	startCh := l.ch

	// Read initial digits
	for unicode.IsDigit(l.ch) {
		sb.WriteRune(l.ch)
		l.readChar()
	}

	// Check if followed by underscore and then letter (identifier pattern)
	if l.ch == '_' {
		// Peek to see what follows the underscore
		nextCh := l.peekChar()
		if unicode.IsLetter(nextCh) || nextCh == '_' {
			// This is an identifier that starts with digits
			sb.WriteRune(l.ch)
			l.readChar()
			// Continue reading as identifier
			for isIdentChar(l.ch) {
				sb.WriteRune(l.ch)
				l.readChar()
			}
			return Item{Token: token.IDENT, Value: sb.String(), Pos: pos}
		}
	}

	// Not an identifier, continue as number
	// But we already consumed the digits, so continue from here
	// Handle underscore separators in numbers (only if followed by a digit)
	for l.ch == '_' && unicode.IsDigit(l.peekChar()) {
		l.readChar() // skip underscore
		for unicode.IsDigit(l.ch) {
			sb.WriteRune(l.ch)
			l.readChar()
		}
	}

	// Check for decimal point (either followed by digit or end of number like 1.)
	if l.ch == '.' {
		nextCh := l.peekChar()
		// Allow 1. (trailing dot with no digits) and 1.5 (dot with digits)
		// But not 1.something (identifier-like)
		if unicode.IsDigit(nextCh) || (!isIdentStart(nextCh) && nextCh != '.') {
			sb.WriteRune(l.ch)
			l.readChar()
			for unicode.IsDigit(l.ch) {
				sb.WriteRune(l.ch)
				l.readChar()
				for l.ch == '_' && unicode.IsDigit(l.peekChar()) {
					l.readChar()
				}
			}
		}
	}

	// Check for exponent
	if l.ch == 'e' || l.ch == 'E' {
		sb.WriteRune(l.ch)
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			sb.WriteRune(l.ch)
			l.readChar()
		}
		for unicode.IsDigit(l.ch) {
			sb.WriteRune(l.ch)
			l.readChar()
			for l.ch == '_' && unicode.IsDigit(l.peekChar()) {
				l.readChar()
			}
		}
	}

	// Special case: if the token was just "0" and current char is 'x', 'b', or 'o',
	// this might be a hex/binary/octal number that we need to handle specially
	val := sb.String()
	if val == "0" && (l.ch == 'x' || l.ch == 'X') {
		sb.WriteRune(l.ch)
		l.readChar()
		for isHexDigit(l.ch) {
			sb.WriteRune(l.ch)
			l.readChar()
		}
		// Check for hex float decimal point
		if l.ch == '.' {
			sb.WriteRune(l.ch)
			l.readChar()
			for isHexDigit(l.ch) {
				sb.WriteRune(l.ch)
				l.readChar()
			}
		}
		// Check for P exponent (hex float notation)
		if l.ch == 'p' || l.ch == 'P' {
			sb.WriteRune(l.ch)
			l.readChar()
			if l.ch == '+' || l.ch == '-' {
				sb.WriteRune(l.ch)
				l.readChar()
			}
			for unicode.IsDigit(l.ch) {
				sb.WriteRune(l.ch)
				l.readChar()
			}
		}
	} else if val == "0" && (l.ch == 'b' || l.ch == 'B') && (l.peekChar() == '0' || l.peekChar() == '1') {
		sb.WriteRune(l.ch)
		l.readChar()
		for l.ch == '0' || l.ch == '1' {
			sb.WriteRune(l.ch)
			l.readChar()
		}
	}

	// Handle special case where number starts with 0 but we're inside readNumberOrIdent
	// and the number already consumed is just the leading zero (checking for 0x, 0b, 0o)
	if startCh == '0' && len(sb.String()) == 1 {
		// Already handled above for 0x, 0b
		// Handle 0o for octal
		if l.ch == 'o' || l.ch == 'O' {
			sb.WriteRune(l.ch)
			l.readChar()
			for l.ch >= '0' && l.ch <= '7' {
				sb.WriteRune(l.ch)
				l.readChar()
			}
		}
	}

	return Item{Token: token.NUMBER, Value: sb.String(), Pos: pos}
}

func isHexDigit(ch rune) bool {
	return unicode.IsDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func hexValue(ch rune) int {
	if ch >= '0' && ch <= '9' {
		return int(ch - '0')
	}
	if ch >= 'a' && ch <= 'f' {
		return int(ch-'a') + 10
	}
	if ch >= 'A' && ch <= 'F' {
		return int(ch-'A') + 10
	}
	return 0
}

func (l *Lexer) readIdentifier() Item {
	pos := l.pos
	var sb strings.Builder

	// Check for hex string literal: x'...' or X'...'
	if (l.ch == 'x' || l.ch == 'X') && l.peekChar() == '\'' {
		l.readChar() // skip x
		return l.readHexString() // read as hex-decoded string
	}

	// Check for binary string literal: b'...' or B'...'
	if (l.ch == 'b' || l.ch == 'B') && l.peekChar() == '\'' {
		l.readChar() // skip b
		return l.readString('\'') // read as regular string
	}

	for isIdentChar(l.ch) {
		sb.WriteRune(l.ch)
		l.readChar()
	}

	ident := sb.String()
	tok := token.Lookup(strings.ToUpper(ident))
	return Item{Token: tok, Value: ident, Pos: pos}
}

func (l *Lexer) readParameter() Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening brace

	for !l.eof && l.ch != '}' {
		sb.WriteRune(l.ch)
		l.readChar()
	}
	if l.ch == '}' {
		l.readChar() // skip closing brace
	}
	return Item{Token: token.PARAM, Value: sb.String(), Pos: pos}
}

func isIdentStart(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch)
}

func isIdentChar(ch rune) bool {
	return ch == '_' || ch == '$' || unicode.IsLetter(ch) || unicode.IsDigit(ch)
}

// Tokenize returns all tokens from the reader.
func Tokenize(r io.Reader) []Item {
	l := New(r)
	var items []Item
	for {
		item := l.NextToken()
		items = append(items, item)
		if item.Token == token.EOF {
			break
		}
	}
	return items
}
