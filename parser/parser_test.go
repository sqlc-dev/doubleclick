package parser_test

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/sqlc-dev/doubleclick/parser"
)

// whitespaceRegex matches sequences of whitespace characters
var whitespaceRegex = regexp.MustCompile(`\s+`)

// normalizeWhitespace collapses all whitespace sequences to a single space
// and trims leading/trailing whitespace. This allows comparing SQL statements
// while ignoring formatting differences.
func normalizeWhitespace(s string) string {
	return strings.TrimSpace(whitespaceRegex.ReplaceAllString(s, " "))
}

// checkSkipped runs skipped todo tests to see which ones now pass.
// Use with: go test ./parser -check-skipped -v
var checkSkipped = flag.Bool("check-skipped", false, "Run skipped todo tests to see which ones now pass")

// checkFormat runs skipped todo_format tests to see which ones now pass.
// Use with: go test ./parser -check-format -v
var checkFormat = flag.Bool("check-format", false, "Run skipped todo_format tests to see which ones now pass")

// testMetadata holds optional metadata for a test case
type testMetadata struct {
	Todo       bool   `json:"todo,omitempty"`
	TodoFormat bool   `json:"todo_format,omitempty"` // true if format roundtrip test is pending
	Source     string `json:"source,omitempty"`
	Explain    *bool  `json:"explain,omitempty"`
	Skip       bool   `json:"skip,omitempty"`
	ParseError bool   `json:"parse_error,omitempty"` // true if query is intentionally invalid SQL
}

// TestParser tests the parser using test cases from the testdata directory.
// Each subdirectory in testdata represents a test case with:
// - query.sql: The SQL query to parse
// - metadata.json (optional): Metadata including:
//   - todo: true if the test is not yet expected to pass
//   - explain: false to skip the test (e.g., when ClickHouse couldn't parse it)
//   - skip: true to skip the test entirely (e.g., causes infinite loop)
//   - parse_error: true if the query is intentionally invalid SQL (expected to fail parsing)
func TestParser(t *testing.T) {
	testdataDir := "testdata"

	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("Failed to read testdata directory: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(testdataDir, entry.Name())

		t.Run(entry.Name(), func(t *testing.T) {
			t.Parallel()

			// Create context with 1 second timeout
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			// Read the query file
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}
			query := string(queryBytes)

			// Read optional metadata
			var metadata testMetadata
			metadataPath := filepath.Join(testDir, "metadata.json")
			if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
				if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
					t.Fatalf("Failed to parse metadata.json: %v", err)
				}
			}

			// Skip tests marked with skip: true (these cause infinite loops or other critical issues)
			if metadata.Skip {
				t.Skip("Skipping: skip is true in metadata")
			}

			// Skip tests where explain is explicitly false (e.g., ClickHouse couldn't parse it)
			// Unless -check-skipped is set and the test is a todo test
			if metadata.Explain != nil && !*metadata.Explain {
				if !(*checkSkipped && metadata.Todo) {
					t.Skipf("Skipping: explain is false in metadata")
					return
				}
			}

			// Parse the query - we only check the first statement
			stmts, parseErr := parser.Parse(ctx, strings.NewReader(query))
			if len(stmts) == 0 {
				// If parse_error is true, this is expected - the query is intentionally invalid
				if metadata.ParseError {
					t.Skipf("Expected parse error (intentionally invalid SQL)")
					return
				}
				if metadata.Todo {
					if *checkSkipped {
						t.Skipf("STILL FAILING (parse error): %v", parseErr)
					} else {
						t.Skipf("TODO: Parser does not yet support (error: %v)", parseErr)
					}
					return
				}
				t.Fatalf("Parse error: %v", parseErr)
			}

			// If parse_error is true but we parsed successfully, skip (our parser is more permissive)
			if metadata.ParseError {
				t.Skipf("Parsed query marked as parse_error (parser is more permissive)")
				return
			}

			// Verify we can serialize to JSON
			_, jsonErr := json.Marshal(stmts[0])
			if jsonErr != nil {
				if metadata.Todo {
					if *checkSkipped {
						t.Skipf("STILL FAILING (JSON serialization): %v", jsonErr)
					} else {
						t.Skipf("TODO: JSON serialization failed: %v", jsonErr)
					}
					return
				}
				t.Fatalf("JSON marshal error: %v\nQuery: %s", jsonErr, query)
			}

			// Check explain output if explain.txt exists
			explainPath := filepath.Join(testDir, "explain.txt")
			if expectedBytes, err := os.ReadFile(explainPath); err == nil {
				expected := strings.TrimSpace(string(expectedBytes))
				// Strip server error messages from expected output
				// These are messages like "The query succeeded but the server error '43' was expected..."
				if idx := strings.Index(expected, "\nThe query succeeded but the server error"); idx != -1 {
					expected = strings.TrimSpace(expected[:idx])
				}
				actual := strings.TrimSpace(parser.Explain(stmts[0]))
				// Use case-insensitive comparison since ClickHouse EXPLAIN AST has inconsistent casing
				// (e.g., Float64_NaN vs Float64_nan, GREATEST vs greatest)
				if !strings.EqualFold(actual, expected) {
					if metadata.Todo {
						if *checkSkipped {
							t.Skipf("STILL FAILING (explain mismatch):\nExpected:\n%s\n\nGot:\n%s", expected, actual)
						} else {
							t.Skipf("TODO: Explain output mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", query, expected, actual)
						}
						return
					}
					t.Errorf("Explain output mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", query, expected, actual)
				}
			}

			// Check AST JSON output if ast.json exists (golden file for AST regression testing)
			astPath := filepath.Join(testDir, "ast.json")
			if expectedASTBytes, err := os.ReadFile(astPath); err == nil {
				actualASTBytes, _ := json.MarshalIndent(stmts[0], "", "  ")
				expectedAST := strings.TrimSpace(string(expectedASTBytes))
				actualAST := strings.TrimSpace(string(actualASTBytes))
				if actualAST != expectedAST {
					if metadata.Todo {
						if *checkSkipped {
							t.Skipf("STILL FAILING (AST mismatch):\nExpected:\n%s\n\nGot:\n%s", expectedAST, actualAST)
						} else {
							t.Skipf("TODO: AST JSON mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", query, expectedAST, actualAST)
						}
						return
					}
					t.Errorf("AST JSON mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", query, expectedAST, actualAST)
				}
			}

			// Check Format output (roundtrip test)
			// Skip if todo_format is true, unless -check-format flag is set
			if !metadata.TodoFormat || *checkFormat {
				formatted := parser.Format(stmts)
				expected := strings.TrimSpace(query)
				// Compare with whitespace normalization to ignore formatting differences
				formattedNorm := normalizeWhitespace(formatted)
				expectedNorm := normalizeWhitespace(expected)
				if formattedNorm != expectedNorm {
					if metadata.TodoFormat {
						if *checkFormat {
							t.Logf("FORMAT STILL FAILING:\nExpected:\n%s\n\nGot:\n%s", expected, formatted)
						}
					} else {
						t.Errorf("Format output mismatch\nExpected:\n%s\n\nGot:\n%s", expected, formatted)
					}
				} else if metadata.TodoFormat && *checkFormat {
					// Automatically remove the todo_format flag from metadata.json
					metadata.TodoFormat = false
					updatedBytes, err := json.Marshal(metadata)
					if err != nil {
						t.Errorf("Failed to marshal updated metadata: %v", err)
					} else if err := os.WriteFile(metadataPath, append(updatedBytes, '\n'), 0644); err != nil {
						t.Errorf("Failed to write updated metadata.json: %v", err)
					} else {
						t.Logf("FORMAT ENABLED - removed todo_format flag from: %s", entry.Name())
					}
				}
			}

			// If we get here with a todo test and -check-skipped is set, the test passes!
			// Automatically remove the todo flag from metadata.json
			if metadata.Todo && *checkSkipped {
				metadata.Todo = false
				updatedBytes, err := json.Marshal(metadata)
				if err != nil {
					t.Errorf("Failed to marshal updated metadata: %v", err)
				} else if err := os.WriteFile(metadataPath, append(updatedBytes, '\n'), 0644); err != nil {
					t.Errorf("Failed to write updated metadata.json: %v", err)
				} else {
					t.Logf("ENABLED - removed todo flag from: %s", entry.Name())
				}
			}
		})
	}
}

// BenchmarkParser benchmarks the parser performance using a complex query
func BenchmarkParser(b *testing.B) {
	query := `
		SELECT
			u.id,
			u.name,
			count(*) AS order_count,
			sum(o.amount) AS total
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.status = 'active' AND o.created_at > '2023-01-01'
		GROUP BY u.id, u.name
		HAVING count(*) > 0
		ORDER BY total DESC
		LIMIT 100
	`

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := parser.Parse(ctx, strings.NewReader(query))
		if err != nil {
			b.Fatal(err)
		}
	}
}
