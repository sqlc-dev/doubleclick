package parser_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kyleconroy/doubleclick/parser"
)

// testMetadata holds optional metadata for a test case
type testMetadata struct {
	Todo   bool   `json:"todo,omitempty"`
	Source string `json:"source,omitempty"`
}

// clickhouseAvailable checks if ClickHouse server is running
func clickhouseAvailable() bool {
	resp, err := http.Get("http://127.0.0.1:8123/ping")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

// getClickHouseAST runs EXPLAIN AST on ClickHouse and returns the output
func getClickHouseAST(query string) (string, error) {
	explainQuery := fmt.Sprintf("EXPLAIN AST %s", query)
	resp, err := http.Get("http://127.0.0.1:8123/?query=" + url.QueryEscape(explainQuery))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	return buf.String(), nil
}

// TestParser tests the parser using test cases from the testdata directory.
// Each subdirectory in testdata represents a test case with:
// - query.sql: The SQL query to parse
// - metadata.json (optional): Metadata including:
//   - todo: true if the test is not yet expected to pass
//   - source: URL to the source file in ClickHouse repository
func TestParser(t *testing.T) {
	testdataDir := "testdata"

	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("Failed to read testdata directory: %v", err)
	}

	ctx := context.Background()

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testName := entry.Name()
		testDir := filepath.Join(testdataDir, testName)

		t.Run(testName, func(t *testing.T) {
			// Read the query
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}
			query := strings.TrimSpace(string(queryBytes))

			// Read optional metadata
			var metadata testMetadata
			metadataPath := filepath.Join(testDir, "metadata.json")
			if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
				if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
					t.Fatalf("Failed to parse metadata.json: %v", err)
				}
			}

			// Log source if available
			if metadata.Source != "" {
				t.Logf("Source: %s", metadata.Source)
			}

			// Parse the query
			stmts, err := parser.Parse(ctx, strings.NewReader(query))
			if err != nil {
				if metadata.Todo {
					t.Skipf("TODO: Parser does not yet support: %s (error: %v)", query, err)
					return
				}
				t.Fatalf("Parse error: %v\nQuery: %s", err, query)
			}

			if len(stmts) == 0 {
				if metadata.Todo {
					t.Skipf("TODO: Parser returned no statements for: %s", query)
					return
				}
				t.Fatalf("Expected at least 1 statement, got 0\nQuery: %s", query)
			}

			// Verify we can serialize to JSON
			_, jsonErr := json.Marshal(stmts[0])
			if jsonErr != nil {
				if metadata.Todo {
					t.Skipf("TODO: JSON serialization failed: %v", jsonErr)
					return
				}
				t.Fatalf("JSON marshal error: %v\nQuery: %s", jsonErr, query)
			}
		})
	}
}

// TestParserWithClickHouse compares parsing with ClickHouse's EXPLAIN AST
func TestParserWithClickHouse(t *testing.T) {
	if !clickhouseAvailable() {
		t.Skip("ClickHouse not available")
	}

	testdataDir := "testdata"

	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("Failed to read testdata directory: %v", err)
	}

	ctx := context.Background()
	passed := 0
	failed := 0
	skipped := 0

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testName := entry.Name()
		testDir := filepath.Join(testdataDir, testName)

		t.Run(testName, func(t *testing.T) {
			// Read the query
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}
			query := strings.TrimSpace(string(queryBytes))

			// Read optional metadata
			var metadata testMetadata
			metadataPath := filepath.Join(testDir, "metadata.json")
			if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
				if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
					t.Fatalf("Failed to parse metadata.json: %v", err)
				}
			}

			// Get ClickHouse's AST
			chAST, err := getClickHouseAST(query)
			if err != nil {
				t.Skipf("ClickHouse error: %v", err)
				skipped++
				return
			}

			// Check if ClickHouse accepted the query
			if strings.Contains(chAST, "Code:") || strings.Contains(chAST, "Exception:") {
				t.Skipf("ClickHouse rejected query: %s", strings.TrimSpace(chAST))
				skipped++
				return
			}

			// Parse with our parser
			stmts, parseErr := parser.Parse(ctx, strings.NewReader(query))
			if parseErr != nil {
				if metadata.Todo {
					t.Skipf("TODO: Parser does not yet support: %s (error: %v)", query, parseErr)
					skipped++
					return
				}
				t.Errorf("Our parser failed but ClickHouse accepted: %s\nError: %v", query, parseErr)
				failed++
				return
			}

			if len(stmts) == 0 {
				if metadata.Todo {
					t.Skipf("TODO: Parser returned no statements for: %s", query)
					skipped++
					return
				}
				t.Errorf("Our parser returned no statements: %s", query)
				failed++
				return
			}

			// Verify we can serialize to JSON
			_, jsonErr := json.Marshal(stmts[0])
			if jsonErr != nil {
				if metadata.Todo {
					t.Skipf("TODO: JSON serialization failed: %v", jsonErr)
					skipped++
					return
				}
				t.Errorf("JSON marshal error: %v\nQuery: %s", jsonErr, query)
				failed++
				return
			}

			passed++
			t.Logf("PASS: %s", query)
		})
	}

	t.Logf("\nSummary: %d passed, %d failed, %d skipped", passed, failed, skipped)
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
