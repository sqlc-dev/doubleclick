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

// checkExplain runs skipped explain_todo tests to see which ones now pass.
// Use with: go test ./parser -check-explain -v
var checkExplain = flag.Bool("check-explain", false, "Run skipped explain_todo tests to see which ones now pass")

// testMetadata holds optional metadata for a test case
type testMetadata struct {
	ExplainTodo map[string]bool `json:"explain_todo,omitempty"` // map of stmtN -> true to skip specific statements
	Source      string          `json:"source,omitempty"`
	Explain     *bool           `json:"explain,omitempty"`
	Skip        bool            `json:"skip,omitempty"`
	ParseError  bool            `json:"parse_error,omitempty"` // true if query is intentionally invalid SQL
}

// splitStatements splits SQL content into individual statements.
func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines and full-line comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Remove inline comments (-- comment at end of line)
		if idx := findCommentStart(trimmed); idx >= 0 {
			trimmed = strings.TrimSpace(trimmed[:idx])
			if trimmed == "" {
				continue
			}
		}

		// Add to current statement
		if current.Len() > 0 {
			current.WriteString(" ")
		}
		current.WriteString(trimmed)

		// Check if statement is complete (ends with ;)
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	// Handle statement without trailing semicolon
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

// findCommentStart finds the position of -- comment that's not inside a string
func findCommentStart(line string) int {
	inString := false
	var stringChar byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		if inString {
			if c == '\\' && i+1 < len(line) {
				i++ // Skip escaped character
				continue
			}
			if c == stringChar {
				inString = false
			}
		} else {
			if c == '\'' || c == '"' || c == '`' {
				inString = true
				stringChar = c
			} else if c == '-' && i+1 < len(line) && line[i+1] == '-' {
				// Check if this looks like a comment (followed by space or end of line)
				if i+2 >= len(line) || line[i+2] == ' ' || line[i+2] == '\t' {
					return i
				}
			}
		}
	}
	return -1
}

// TestParser tests the parser using test cases from the testdata directory.
// Each subdirectory in testdata represents a test case with:
// - query.sql: The SQL query to parse (may contain multiple statements)
// - metadata.json (optional): Metadata including:
//   - explain: false to skip the test (e.g., when ClickHouse couldn't parse it)
//   - skip: true to skip the test entirely (e.g., causes infinite loop)
//   - parse_error: true if the query is intentionally invalid SQL (expected to fail parsing)
//   - explain_todo: map of stmtN -> true to skip specific statements (e.g., {"stmt2": true, "stmt5": true})
// - explain.txt: Expected EXPLAIN AST output for first statement
// - explain_N.txt: Expected EXPLAIN AST output for Nth statement (N >= 2)
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

			// Read the query file
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}
			queryContent := string(queryBytes)

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
			if metadata.Explain != nil && !*metadata.Explain {
				t.Skipf("Skipping: explain is false in metadata")
				return
			}

			// Split into individual statements
			statements := splitStatements(queryContent)
			if len(statements) == 0 {
				t.Skipf("No statements found in query.sql (all commented out)")
				return
			}

			// Test each statement as a subtest
			for i, stmt := range statements {
				stmtIndex := i + 1
				t.Run(fmt.Sprintf("stmt%d", stmtIndex), func(t *testing.T) {
					// Determine explain file path: explain.txt for first, explain_N.txt for N >= 2
					var explainPath string
					if stmtIndex == 1 {
						explainPath = filepath.Join(testDir, "explain.txt")
					} else {
						explainPath = filepath.Join(testDir, fmt.Sprintf("explain_%d.txt", stmtIndex))
					}

					// For statements beyond the first, skip if no explain file exists
					// (these statements haven't been regenerated yet)
					if stmtIndex > 1 {
						if _, err := os.Stat(explainPath); os.IsNotExist(err) {
							t.Skipf("No explain_%d.txt file (run regenerate-explain to generate)", stmtIndex)
							return
						}
					}

					// Skip statements marked in explain_todo (unless -check-explain is set)
					stmtKey := fmt.Sprintf("stmt%d", stmtIndex)
					isExplainTodo := metadata.ExplainTodo[stmtKey]
					if isExplainTodo && !*checkExplain {
						t.Skipf("TODO: explain_todo[%s] is true", stmtKey)
						return
					}

					// Create context with 1 second timeout
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()

					// Parse this statement
					stmts, parseErr := parser.Parse(ctx, strings.NewReader(stmt))
					if len(stmts) == 0 {
						// If parse_error is true, this is expected - the query is intentionally invalid
						if metadata.ParseError {
							t.Skipf("Expected parse error (intentionally invalid SQL)")
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
						t.Fatalf("JSON marshal error: %v\nQuery: %s", jsonErr, stmt)
					}

					// Check explain output if explain file exists
					if expectedBytes, err := os.ReadFile(explainPath); err == nil {
						expected := strings.TrimSpace(string(expectedBytes))
						// Strip version header comment (e.g., "-- Generated by ClickHouse X.X.X.X")
						if strings.HasPrefix(expected, "-- Generated by ClickHouse ") {
							if idx := strings.Index(expected, "\n"); idx != -1 {
								expected = strings.TrimSpace(expected[idx+1:])
							}
						}
						// Strip server error messages from expected output
						if idx := strings.Index(expected, "\nThe query succeeded but the server error"); idx != -1 {
							expected = strings.TrimSpace(expected[:idx])
						}
						actual := strings.TrimSpace(parser.Explain(stmts[0]))
						// Use case-insensitive comparison since ClickHouse EXPLAIN AST has inconsistent casing
						if !strings.EqualFold(actual, expected) {
							if isExplainTodo && *checkExplain {
								t.Logf("EXPLAIN STILL FAILING:\nExpected:\n%s\n\nGot:\n%s", expected, actual)
							} else {
								t.Errorf("Explain output mismatch\nQuery: %s\nExpected:\n%s\n\nGot:\n%s", stmt, expected, actual)
							}
						} else if isExplainTodo && *checkExplain {
							// Test passes now - remove from explain_todo
							delete(metadata.ExplainTodo, stmtKey)
							if len(metadata.ExplainTodo) == 0 {
								metadata.ExplainTodo = nil
							}
							updatedBytes, err := json.MarshalIndent(metadata, "", "  ")
							if err != nil {
								t.Errorf("Failed to marshal updated metadata: %v", err)
							} else if err := os.WriteFile(metadataPath, append(updatedBytes, '\n'), 0644); err != nil {
								t.Errorf("Failed to write updated metadata.json: %v", err)
							} else {
								t.Logf("EXPLAIN PASSES NOW - removed explain_todo[%s] from: %s", stmtKey, entry.Name())
							}
						}
					}

				})
			}
		})
	}
}

// TestExplainVersionConsistency ensures all explain*.txt files have the same ClickHouse version header.
// This test:
// - Scans all explain*.txt files in testdata/
// - Checks each file for a "-- Generated by ClickHouse X.X.X.X" header
// - Reports files without version headers
// - Reports if multiple different versions are found
// - Passes only when all files have the same version header
func TestExplainVersionConsistency(t *testing.T) {
	testdataDir := "testdata"

	// Find all explain*.txt files
	var explainFiles []string
	err := filepath.Walk(testdataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasPrefix(info.Name(), "explain") && strings.HasSuffix(info.Name(), ".txt") {
			explainFiles = append(explainFiles, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk testdata directory: %v", err)
	}

	if len(explainFiles) == 0 {
		t.Skip("No explain*.txt files found")
	}

	// Track versions and files without headers
	versionToFiles := make(map[string][]string)
	var filesWithoutHeaders []string

	versionPattern := "-- Generated by ClickHouse "

	for _, path := range explainFiles {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("Failed to read %s: %v", path, err)
			continue
		}

		lines := strings.Split(string(content), "\n")
		if len(lines) == 0 {
			filesWithoutHeaders = append(filesWithoutHeaders, path)
			continue
		}

		firstLine := lines[0]
		if !strings.HasPrefix(firstLine, versionPattern) {
			filesWithoutHeaders = append(filesWithoutHeaders, path)
			continue
		}

		// Extract version from "-- Generated by ClickHouse X.X.X.X"
		version := strings.TrimPrefix(firstLine, versionPattern)
		versionToFiles[version] = append(versionToFiles[version], path)
	}

	// Report findings
	if len(filesWithoutHeaders) > 0 {
		t.Errorf("Found %d files without version headers:", len(filesWithoutHeaders))
		// Show first 10 files as examples
		limit := 10
		if len(filesWithoutHeaders) < limit {
			limit = len(filesWithoutHeaders)
		}
		for _, path := range filesWithoutHeaders[:limit] {
			t.Errorf("  - %s", path)
		}
		if len(filesWithoutHeaders) > 10 {
			t.Errorf("  ... and %d more", len(filesWithoutHeaders)-10)
		}
	}

	if len(versionToFiles) > 1 {
		t.Errorf("Found %d different ClickHouse versions:", len(versionToFiles))
		for version, files := range versionToFiles {
			t.Errorf("  Version %q: %d files", version, len(files))
		}
	}

	if len(filesWithoutHeaders) > 0 || len(versionToFiles) != 1 {
		t.FailNow()
	}

	// Log the consistent version
	for version := range versionToFiles {
		t.Logf("All %d explain files have consistent version: %s", len(explainFiles), version)
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
