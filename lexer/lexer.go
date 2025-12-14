// Package lexer implements a lexer for ClickHouse SQL.
package lexer

import (
	"bufio"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/kyleconroy/doubleclick/token"
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
	Token token.Token
	Value string
	Pos   token.Position
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
	for unicode.IsSpace(l.ch) {
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
	if l.ch == '/' && l.peekChar() == '*' {
		return l.readBlockComment()
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
	case '\'':
		return l.readString('\'')
	case '"':
		return l.readQuotedIdentifier()
	case '`':
		return l.readBacktickIdentifier()
	default:
		if unicode.IsDigit(l.ch) {
			return l.readNumber()
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

func (l *Lexer) readBlockComment() Item {
	pos := l.pos
	var sb strings.Builder
	// Skip /*
	sb.WriteRune(l.ch)
	l.readChar()
	sb.WriteRune(l.ch)
	l.readChar()

	for !l.eof {
		if l.ch == '*' && l.peekChar() == '/' {
			sb.WriteRune(l.ch)
			l.readChar()
			sb.WriteRune(l.ch)
			l.readChar()
			break
		}
		sb.WriteRune(l.ch)
		l.readChar()
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
			case 'b':
				sb.WriteRune('\b')
			case 'f':
				sb.WriteRune('\f')
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
				// Unknown escape, just write the character after backslash
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

func (l *Lexer) readQuotedIdentifier() Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening quote

	for !l.eof && l.ch != '"' {
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
	if l.ch == '"' {
		l.readChar() // skip closing quote
	}
	return Item{Token: token.IDENT, Value: sb.String(), Pos: pos}
}

func (l *Lexer) readBacktickIdentifier() Item {
	pos := l.pos
	var sb strings.Builder
	l.readChar() // skip opening backtick

	for !l.eof && l.ch != '`' {
		sb.WriteRune(l.ch)
		l.readChar()
	}
	if l.ch == '`' {
		l.readChar() // skip closing backtick
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
			// Hex literal
			sb.WriteRune(l.ch)
			l.readChar()
			for isHexDigit(l.ch) {
				sb.WriteRune(l.ch)
				l.readChar()
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

	// Check for decimal point
	if l.ch == '.' && unicode.IsDigit(l.peekChar()) {
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
		return l.readString('\'') // read as regular string
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
