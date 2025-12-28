// Package normalize provides SQL normalization functions for comparing
// semantically equivalent SQL statements that may differ syntactically.
package normalize

import (
	"encoding/hex"
	"regexp"
	"strings"
)

// Pre-compiled regexes for performance
var (
	whitespaceRegex        = regexp.MustCompile(`\s+`)
	operatorSpaceRegex     = regexp.MustCompile(`\s*([=<>!]+|::|->|\|\||&&)\s*`)
	numericUnderscoreRegex = regexp.MustCompile(`(\d)_(\d)`)
	backtickIdentRegex     = regexp.MustCompile("`([^`]+)`")
	hexEscapeRegex         = regexp.MustCompile(`(\\x[0-9A-Fa-f]{2})+`)
	doubleQuotedIdentRegex = regexp.MustCompile(`(\s)"([^"]+)"`)
	asKeywordRegex         = regexp.MustCompile(`\bas\b`)
	leadingZerosRegex      = regexp.MustCompile(`\b0+(\d+)\b`)
	heredocRegex           = regexp.MustCompile(`\$\$([^$]*)\$\$`)
	emptyTupleRegex        = regexp.MustCompile(`\(\)`)
	hexStringRegex         = regexp.MustCompile(`[xX]'([^']*)'`)
	innerJoinRegex         = regexp.MustCompile(`(?i)\bINNER\s+JOIN\b`)
	leftOuterJoinRegex     = regexp.MustCompile(`(?i)\bLEFT\s+OUTER\s+JOIN\b`)
	rightOuterJoinRegex    = regexp.MustCompile(`(?i)\bRIGHT\s+OUTER\s+JOIN\b`)
	ascRegex               = regexp.MustCompile(`\bASC\b`)
	offsetRowsRegex        = regexp.MustCompile(`\bOFFSET\s+(\S+)\s+ROWS?\b`)
	engineEqualsRegex      = regexp.MustCompile(`(?i)\bENGINE\s*=\s*`)
	insertIntoTableRegex   = regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+TABLE\b`)
	unionDistinctRegex     = regexp.MustCompile(`(?i)\bUNION\s+DISTINCT\b`)
	regexpOperatorRegex    = regexp.MustCompile(`('[^']*')\s+REGEXP\s+('[^']*')`)
	orderByEmptyRegex      = regexp.MustCompile(`\bORDER BY \(\)\b`)
	spaceBeforeParenRegex  = regexp.MustCompile(`(\w+)\s+\((\w)`)
	withTiesRegex          = regexp.MustCompile(`(?i)\bWITH\s+TIES\b`)
	parenColumnEqualsRegex = regexp.MustCompile(`\((\w+)=`)
	notParenDigitRegex     = regexp.MustCompile(`\bNOT\s*\((\d+)\)`)
	notLowerParenRegex     = regexp.MustCompile(`\bnot\s*\((\d+)\)`)
	isNotNullParenRegex    = regexp.MustCompile(`\((\w+)\s+IS\s+NOT\s+NULL\)`)
	isNullParenRegex       = regexp.MustCompile(`\((\w+)\s+IS\s+NULL\)`)
	// Alias AS normalization: remove optional AS keyword in alias contexts
	// Matches: expr AS alias (where expr ends with word/digit/closing paren)
	aliasAsRegex = regexp.MustCompile(`(\d+|\)|\w)\s+AS\s+(\w)`)
	// ORDER BY single column parentheses normalization
	// ORDER BY (col) -> ORDER BY col
	orderBySingleParenRegex = regexp.MustCompile(`(?i)\bORDER BY\s+\((\w+)\)`)
	// PRIMARY KEY single column parentheses normalization
	// PRIMARY KEY (col) -> PRIMARY KEY col
	primaryKeySingleParenRegex = regexp.MustCompile(`(?i)\bPRIMARY KEY\s+\((\w+)\)`)
	// Parentheses around IN expressions: (x IN(...)) -> x IN(...)
	// Handles both with and without space after IN
	// Must be preceded by space or comma (not a function call like sum(x IN ...))
	parenInExprRegex = regexp.MustCompile(`([\s,])\((\w+\s*IN\s*\([^)]*\))\)`)
	// LIMIT syntax normalization: LIMIT offset, count -> LIMIT count OFFSET offset
	limitCommaRegex = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)\s*,\s*(\d+)\b`)
	// Spaces around dots in identifiers: system . one -> system.one
	spaceDotSpaceRegex = regexp.MustCompile(`(\w)\s*\.\s*(\w)`)
	// Trailing .0 in float literals: 1.0 -> 1
	trailingDotZeroRegex = regexp.MustCompile(`\b(\d+)\.0+\b`)
	// Add spaces around arithmetic operators: num/2 -> num / 2, 1+1 -> 1 + 1, 1+-a -> 1 + -a
	// Match when operator is between word chars or ), or word and - (for unary minus)
	arithmeticNoSpaceRegex = regexp.MustCompile(`([\w)])([/*%+])([\w-])`)
	// Add spaces around binary minus: x-1 -> x - 1 (but not -1 which is unary)
	// Match when ) or word is directly followed by - and then a word/digit
	binaryMinusNoSpaceRegex = regexp.MustCompile(`([\w)])-([\w])`)
)

// DecodeHexEscapes decodes \xNN escape sequences in a string to raw bytes.
// This allows comparing strings with hex escapes to decoded strings.
func DecodeHexEscapes(s string) string {
	return hexEscapeRegex.ReplaceAllStringFunc(s, func(match string) string {
		// Decode all consecutive hex escapes together
		var result []byte
		for i := 0; i < len(match); i += 4 {
			// Each \xNN is 4 characters
			if i+4 > len(match) {
				break
			}
			hexStr := match[i+2 : i+4] // Skip \x prefix
			b, err := hex.DecodeString(hexStr)
			if err != nil || len(b) != 1 {
				return match // Return original on error
			}
			result = append(result, b[0])
		}
		return string(result)
	})
}

// Whitespace collapses all whitespace sequences to a single space
// and trims leading/trailing whitespace.
func Whitespace(s string) string {
	return strings.TrimSpace(whitespaceRegex.ReplaceAllString(s, " "))
}

// EscapesInStrings normalizes escape sequences within string literals:
//   - \' -> '' (backslash-escaped quote to SQL-standard)
//   - \\ -> \ (double backslash to single backslash)
//
// This allows comparing strings with different escape styles.
func EscapesInStrings(s string) string {
	var result strings.Builder
	result.Grow(len(s))
	i := 0
	for i < len(s) {
		ch := s[i]
		if ch == '\'' {
			// Start of a single-quoted string
			result.WriteByte(ch)
			i++
			for i < len(s) {
				ch = s[i]
				if ch == '\\' && i+1 < len(s) && s[i+1] == '\'' {
					// Backslash-escaped quote -> convert to SQL-standard ''
					result.WriteString("''")
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == '\\' {
					// Escaped backslash \\ -> single backslash \
					result.WriteByte('\\')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == 't' {
					// Escaped tab \t -> actual tab
					result.WriteByte('\t')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == 'n' {
					// Escaped newline \n -> actual newline
					result.WriteByte('\n')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == 'r' {
					// Escaped carriage return \r -> actual carriage return
					result.WriteByte('\r')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == 'a' {
					// Escaped alert \a -> actual alert (bell)
					result.WriteByte('\a')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == 'b' {
					// Escaped backspace \b -> actual backspace
					result.WriteByte('\b')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == 'f' {
					// Escaped form feed \f -> actual form feed
					result.WriteByte('\f')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == 'v' {
					// Escaped vertical tab \v -> actual vertical tab
					result.WriteByte('\v')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == '?' {
					// Escaped question mark \? -> actual question mark
					result.WriteByte('?')
					i += 2
				} else if ch == '\\' && i+1 < len(s) && s[i+1] == '"' {
					// Escaped double quote \" -> actual double quote
					result.WriteByte('"')
					i += 2
				} else if ch == '\\' && i+3 < len(s) && s[i+1] == 'x' {
					// Hex escape \xNN -> decoded byte
					hexStr := s[i+2 : i+4]
					b, err := hex.DecodeString(hexStr)
					if err == nil && len(b) == 1 {
						result.WriteByte(b[0])
						i += 4
					} else {
						result.WriteByte(ch)
						i++
					}
				} else if ch == '\'' {
					// Either end of string or escaped quote
					result.WriteByte(ch)
					i++
					if i < len(s) && s[i] == '\'' {
						// Escaped quote ''
						result.WriteByte(s[i])
						i++
					} else {
						// End of string
						break
					}
				} else {
					result.WriteByte(ch)
					i++
				}
			}
		} else {
			result.WriteByte(ch)
			i++
		}
	}
	return result.String()
}

// CommasOutsideStrings removes spaces after commas that are outside of string literals.
func CommasOutsideStrings(s string) string {
	var result strings.Builder
	result.Grow(len(s))
	inString := false
	stringChar := byte(0)
	i := 0
	for i < len(s) {
		ch := s[i]
		if !inString {
			if ch == '\'' || ch == '"' {
				inString = true
				stringChar = ch
				result.WriteByte(ch)
				i++
			} else if ch == ',' && i+1 < len(s) && s[i+1] == ' ' {
				// Skip space after comma outside of strings
				result.WriteByte(ch)
				i += 2
			} else {
				result.WriteByte(ch)
				i++
			}
		} else {
			// Inside string
			if ch == stringChar {
				// Check for escaped quote ('' or "")
				if i+1 < len(s) && s[i+1] == stringChar {
					result.WriteByte(ch)
					result.WriteByte(s[i+1])
					i += 2
				} else {
					inString = false
					result.WriteByte(ch)
					i++
				}
			} else if ch == '\\' && i+1 < len(s) {
				// Escaped character - keep both
				result.WriteByte(ch)
				result.WriteByte(s[i+1])
				i += 2
			} else {
				result.WriteByte(ch)
				i++
			}
		}
	}
	return result.String()
}

// ForFormat normalizes SQL for format comparison by applying various
// normalizations that make semantically equivalent SQL statements match.
// This includes whitespace normalization, operator spacing, escape sequences,
// and various SQL syntax equivalences.
func ForFormat(s string) string {
	normalized := Whitespace(s)
	// Normalize spaces around operators (remove spaces)
	normalized = operatorSpaceRegex.ReplaceAllString(normalized, "$1")
	// Normalize commas: remove spaces after commas outside of strings
	normalized = CommasOutsideStrings(normalized)
	// Normalize backslash-escaped quotes to SQL-standard (\' -> '')
	normalized = EscapesInStrings(normalized)
	// Remove underscores from numeric literals (100_000 -> 100000)
	for numericUnderscoreRegex.MatchString(normalized) {
		normalized = numericUnderscoreRegex.ReplaceAllString(normalized, "$1$2")
	}
	// Normalize backtick identifiers to unquoted
	normalized = backtickIdentRegex.ReplaceAllString(normalized, "$1")
	// Normalize double-quoted identifiers to unquoted (but not in strings)
	// This handles "identifier" -> identifier (e.g., 2 "union" -> 2 union)
	normalized = doubleQuotedIdentRegex.ReplaceAllString(normalized, "$1$2")
	// Normalize AS keyword case: as -> AS
	normalized = asKeywordRegex.ReplaceAllString(normalized, "AS")
	// Remove optional AS keyword in alias contexts (1 AS x -> 1 x)
	// This handles the equivalence of "expr AS alias" and "expr alias"
	normalized = aliasAsRegex.ReplaceAllString(normalized, "$1 $2")
	// Remove leading zeros from integer literals (077 -> 77)
	normalized = leadingZerosRegex.ReplaceAllString(normalized, "$1")
	// Normalize heredocs ($$...$$ -> '...')
	normalized = heredocRegex.ReplaceAllString(normalized, "'$1'")
	// Normalize empty tuple () to tuple()
	normalized = emptyTupleRegex.ReplaceAllString(normalized, "tuple()")
	// Normalize hex string literals x'...' to just '...' (decoded form)
	// The formatter outputs the decoded string, so we need to normalize for comparison
	normalized = hexStringRegex.ReplaceAllString(normalized, "'$1'")
	// Decode hex escape sequences (\xNN -> actual character)
	normalized = DecodeHexEscapes(normalized)
	// Normalize "INNER JOIN" to "JOIN" (they're equivalent) - case insensitive
	normalized = innerJoinRegex.ReplaceAllString(normalized, "JOIN")
	// Normalize "LEFT OUTER JOIN" to "LEFT JOIN"
	normalized = leftOuterJoinRegex.ReplaceAllString(normalized, "LEFT JOIN")
	// Normalize "RIGHT OUTER JOIN" to "RIGHT JOIN"
	normalized = rightOuterJoinRegex.ReplaceAllString(normalized, "RIGHT JOIN")
	// Normalize "ORDER BY x ASC" to "ORDER BY x" (ASC is default)
	normalized = ascRegex.ReplaceAllString(normalized, "")
	// Normalize "OFFSET n ROWS" to "OFFSET n"
	normalized = offsetRowsRegex.ReplaceAllString(normalized, "OFFSET $1")
	// Normalize CROSS JOIN to comma
	normalized = strings.ReplaceAll(normalized, "CROSS JOIN", ",")
	// Normalize ENGINE = X to ENGINE X (and engine X to ENGINE X)
	normalized = engineEqualsRegex.ReplaceAllString(normalized, "ENGINE ")
	// Normalize INSERT INTO TABLE to INSERT INTO
	normalized = insertIntoTableRegex.ReplaceAllString(normalized, "INSERT INTO")
	// Normalize UNION DISTINCT to UNION (DISTINCT is default)
	normalized = unionDistinctRegex.ReplaceAllString(normalized, "UNION")
	// Normalize REGEXP operator to match() function (they're equivalent)
	// 'x' REGEXP 'y' -> match('x','y')
	normalized = regexpOperatorRegex.ReplaceAllString(normalized, "match($1,$2)")
	// Normalize ORDER BY () to ORDER BY tuple()
	normalized = orderByEmptyRegex.ReplaceAllString(normalized, "ORDER BY tuple()")
	// Remove parentheses around IN expressions BEFORE removing spaces
	// (x IN (...)) -> x IN (...) - this must be done before spaceBeforeParenRegex
	normalized = parenInExprRegex.ReplaceAllString(normalized, "$1$2")
	// Normalize INSERT INTO table (cols) to have no space before ( (or consistent spacing)
	// This matches "tablename (" and removes the space: "tablename("
	normalized = spaceBeforeParenRegex.ReplaceAllString(normalized, "$1($2")
	// Normalize WITH TIES to TIES (for LIMIT)
	normalized = withTiesRegex.ReplaceAllString(normalized, "TIES")
	// Normalize parentheses around simple column references in WHERE: (database=...) to database=...
	normalized = parenColumnEqualsRegex.ReplaceAllString(normalized, "$1=")
	// Normalize parentheses around single values after operators like NOT
	normalized = notParenDigitRegex.ReplaceAllString(normalized, "NOT $1")
	normalized = notLowerParenRegex.ReplaceAllString(normalized, "not $1")
	// Normalize parentheses around IS NULL and IS NOT NULL expressions
	// This handles both standalone (x IS NULL) and inside lambdas x -> (x IS NULL)
	normalized = isNotNullParenRegex.ReplaceAllString(normalized, "$1 IS NOT NULL")
	normalized = isNullParenRegex.ReplaceAllString(normalized, "$1 IS NULL")
	// Normalize ORDER BY (col) to ORDER BY col
	normalized = orderBySingleParenRegex.ReplaceAllString(normalized, "ORDER BY $1")
	// Normalize PRIMARY KEY (col) to PRIMARY KEY col
	normalized = primaryKeySingleParenRegex.ReplaceAllString(normalized, "PRIMARY KEY $1")
	// Normalize LIMIT offset, count to LIMIT count OFFSET offset
	normalized = limitCommaRegex.ReplaceAllString(normalized, "LIMIT $2 OFFSET $1")
	// Normalize spaces around dots in identifiers: system . one -> system.one
	normalized = spaceDotSpaceRegex.ReplaceAllString(normalized, "$1.$2")
	// Normalize trailing .0 in float literals: 1.0 -> 1
	normalized = trailingDotZeroRegex.ReplaceAllString(normalized, "$1")
	// Add spaces around arithmetic operators (/, *, %): num/2 -> num / 2
	normalized = arithmeticNoSpaceRegex.ReplaceAllString(normalized, "$1 $2 $3")
	// Add spaces around binary minus: x-1 -> x - 1
	normalized = binaryMinusNoSpaceRegex.ReplaceAllString(normalized, "$1 - $2")
	// Re-normalize whitespace after replacements
	normalized = Whitespace(normalized)
	// Strip trailing semicolon and any spaces before it
	normalized = strings.TrimSuffix(strings.TrimSpace(normalized), ";")
	return strings.TrimSpace(normalized)
}

// StripComments removes SQL comments from a query string.
// It handles:
//   - Line comments: -- to end of line
//   - Block comments: /* ... */ with nesting support
func StripComments(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	i := 0
	for i < len(s) {
		// Check for line comment: --
		if i+1 < len(s) && s[i] == '-' && s[i+1] == '-' {
			// Skip until end of line
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}

		// Check for block comment: /* ... */
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
			depth := 1
			i += 2
			for i < len(s) && depth > 0 {
				if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
					depth++
					i += 2
				} else if i+1 < len(s) && s[i] == '*' && s[i+1] == '/' {
					depth--
					i += 2
				} else {
					i++
				}
			}
			continue
		}

		// Check for string literal - don't strip comments inside strings
		if s[i] == '\'' {
			result.WriteByte(s[i])
			i++
			for i < len(s) {
				if s[i] == '\'' {
					result.WriteByte(s[i])
					i++
					// Check for escaped quote ''
					if i < len(s) && s[i] == '\'' {
						result.WriteByte(s[i])
						i++
						continue
					}
					break
				}
				result.WriteByte(s[i])
				i++
			}
			continue
		}

		result.WriteByte(s[i])
		i++
	}

	return result.String()
}
