package parser_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sqlc-dev/doubleclick/parser"
)

// checkSkipped runs skipped todo tests to see which ones now pass.
// Use with: go test ./parser -check-skipped -v
var checkSkipped = flag.Bool("check-skipped", false, "Run skipped todo tests to see which ones now pass")

// splitOnDelimiter splits content on "---" lines (multi-query separator)
func splitOnDelimiter(content string) []string {
	var blocks []string
	var current strings.Builder
	for _, line := range strings.Split(content, "\n") {
		if strings.TrimSpace(line) == "---" {
			if block := strings.TrimSpace(current.String()); block != "" {
				blocks = append(blocks, block)
			}
			current.Reset()
		} else {
			current.WriteString(line)
			current.WriteString("\n")
		}
	}
	if block := strings.TrimSpace(current.String()); block != "" {
		blocks = append(blocks, block)
	}
	return blocks
}

// parseQueryBlock parses a single query block (handles comments and multi-line)
func parseQueryBlock(content string) string {
	var queryParts []string
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "#") {
			continue
		}
		// Remove trailing comment if present (but not inside strings - simple heuristic)
		lineContent := trimmed
		if idx := strings.Index(trimmed, " -- "); idx >= 0 {
			lineContent = strings.TrimSpace(trimmed[:idx])
		}
		// Check if line ends with semicolon (statement terminator)
		if strings.HasSuffix(lineContent, ";") {
			queryParts = append(queryParts, lineContent)
			break
		}
		queryParts = append(queryParts, trimmed)
	}
	return strings.Join(queryParts, " ")
}

// testMetadata holds optional metadata for a test case
type testMetadata struct {
	Todo       bool   `json:"todo,omitempty"`
	Source     string `json:"source,omitempty"`
	Explain    *bool  `json:"explain,omitempty"`
	Skip       bool   `json:"skip,omitempty"`
	ParseError bool   `json:"parse_error,omitempty"` // true if query is intentionally invalid SQL
}

// TestParser tests the parser using test cases from the testdata directory.
// Each subdirectory in testdata represents a test case with:
// - query.sql: The SQL query to parse (multiple queries separated by ---)
// - metadata.json (optional): Metadata including:
//   - todo: true if the test is not yet expected to pass
//   - explain: false to skip the test (e.g., when ClickHouse couldn't parse it)
//   - skip: true to skip the test entirely (e.g., causes infinite loop)
//   - parse_error: true if the query is intentionally invalid SQL (expected to fail parsing)
// - explain.txt: Expected EXPLAIN output (multiple outputs separated by ---)
//
// Multi-query support: Use "---" on its own line to separate multiple queries
// in query.sql and their corresponding explain outputs in explain.txt.
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

			// Read the query file (supports multiple queries separated by ---)
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}

			// Split into query blocks and parse each one
			queryBlocks := splitOnDelimiter(string(queryBytes))
			if len(queryBlocks) == 0 {
				t.Fatalf("No query blocks found in query.sql")
			}

			// Parse each query block
			queries := make([]string, len(queryBlocks))
			for i, block := range queryBlocks {
				queries[i] = parseQueryBlock(block)
			}

			// Read explain.txt and split into expected outputs
			explainPath := filepath.Join(testDir, "explain.txt")
			var expectedExplains []string
			if expectedBytes, err := os.ReadFile(explainPath); err == nil {
				expectedExplains = splitOnDelimiter(string(expectedBytes))
				// Strip server error messages from each expected output
				for i, expected := range expectedExplains {
					if idx := strings.Index(expected, "\nThe query succeeded but the server error"); idx != -1 {
						expectedExplains[i] = strings.TrimSpace(expected[:idx])
					}
				}
			}

			// Validate query count matches explain count (if multiple explains exist)
			// For backward compatibility: if only 1 explain exists, only test the first query
			if len(expectedExplains) > 1 && len(queries) != len(expectedExplains) {
				t.Fatalf("Query count (%d) does not match explain count (%d)", len(queries), len(expectedExplains))
			}
			// If only 1 explain exists but multiple queries, only test the first query
			if len(expectedExplains) == 1 && len(queries) > 1 {
				queries = queries[:1]
			}

			// Read ast.json and split into expected outputs (if exists)
			astPath := filepath.Join(testDir, "ast.json")
			var expectedASTs []string
			if expectedASTBytes, err := os.ReadFile(astPath); err == nil {
				expectedASTs = splitOnDelimiter(string(expectedASTBytes))
				// For backward compatibility: if only 1 AST exists but multiple queries,
				// we already truncated queries above for explain, so this should align
			}

			// Process each query
			for i, query := range queries {
				queryLabel := query
				if len(queries) > 1 {
					queryLabel = fmt.Sprintf("[%d] %s", i+1, query)
				}

				// Parse the query
				stmts, err := parser.Parse(ctx, strings.NewReader(query))
				if err != nil {
					// If parse_error is true, this is expected - the query is intentionally invalid
					if metadata.ParseError {
						t.Skipf("Expected parse error (intentionally invalid SQL): %s", queryLabel)
						return
					}
					if metadata.Todo {
						if *checkSkipped {
							t.Skipf("STILL FAILING (parse error): %v", err)
						} else {
							t.Skipf("TODO: Parser does not yet support: %s (error: %v)", queryLabel, err)
						}
						return
					}
					t.Fatalf("Parse error: %v\nQuery: %s", err, queryLabel)
				}

				// If we successfully parsed a query marked as parse_error, note it
				// (The query might have been fixed or the parser is too permissive)

				if len(stmts) == 0 {
					if metadata.Todo {
						if *checkSkipped {
							t.Skipf("STILL FAILING (no statements): parser returned no statements")
						} else {
							t.Skipf("TODO: Parser returned no statements for: %s", queryLabel)
						}
						return
					}
					t.Fatalf("Expected at least 1 statement, got 0\nQuery: %s", queryLabel)
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
					t.Fatalf("JSON marshal error: %v\nQuery: %s", jsonErr, queryLabel)
				}

				// Check explain output
				if i < len(expectedExplains) {
					expected := strings.TrimSpace(expectedExplains[i])
					actual := strings.TrimSpace(parser.Explain(stmts[0]))
					// Use case-insensitive comparison since ClickHouse EXPLAIN AST has inconsistent casing
					// (e.g., Float64_NaN vs Float64_nan, GREATEST vs greatest)
					if !strings.EqualFold(actual, expected) {
						if metadata.Todo {
							if *checkSkipped {
								t.Skipf("STILL FAILING (explain mismatch):\nExpected:\n%s\n\nGot:\n%s", expected, actual)
							} else {
								t.Skipf("TODO: Explain output mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", queryLabel, expected, actual)
							}
							return
						}
						t.Errorf("Explain output mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", queryLabel, expected, actual)
					}
				}

				// Check AST JSON output
				if i < len(expectedASTs) {
					actualASTBytes, _ := json.MarshalIndent(stmts[0], "", "  ")
					expectedAST := strings.TrimSpace(expectedASTs[i])
					actualAST := strings.TrimSpace(string(actualASTBytes))
					if actualAST != expectedAST {
						if metadata.Todo {
							if *checkSkipped {
								t.Skipf("STILL FAILING (AST mismatch):\nExpected:\n%s\n\nGot:\n%s", expectedAST, actualAST)
							} else {
								t.Skipf("TODO: AST JSON mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", queryLabel, expectedAST, actualAST)
							}
							return
						}
						t.Errorf("AST JSON mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", queryLabel, expectedAST, actualAST)
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
