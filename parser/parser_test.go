package parser_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kyleconroy/doubleclick/parser"
)

// testMetadata holds optional metadata for a test case
type testMetadata struct {
	Todo   bool   `json:"todo,omitempty"`
	Source string `json:"source,omitempty"`
}

// astJSON represents the structure of ast.json from ClickHouse EXPLAIN AST
type astJSON struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data []struct {
		Explain string `json:"explain"`
	} `json:"data"`
	Rows       int `json:"rows"`
	Statistics struct {
		Elapsed  float64 `json:"elapsed"`
		RowsRead int     `json:"rows_read"`
		BytesRead int    `json:"bytes_read"`
	} `json:"statistics"`
	Error bool `json:"error,omitempty"`
}

// TestParser tests the parser using test cases from the testdata directory.
// Each subdirectory in testdata represents a test case with:
// - query.sql: The SQL query to parse
// - ast.json: Expected AST from ClickHouse EXPLAIN AST
// - metadata.json (optional): Metadata including:
//   - todo: true if the test is not yet expected to pass
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

		testName := entry.Name()
		testDir := filepath.Join(testdataDir, testName)

		t.Run(testName, func(t *testing.T) {
			// Create context with 1 second timeout
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			// Read the query (only first line, as ast.json was generated from first statement)
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}
			// Get first line only (ast.json contains AST for first statement)
			lines := strings.SplitN(string(queryBytes), "\n", 2)
			query := strings.TrimSpace(lines[0])

			// Read optional metadata
			var metadata testMetadata
			metadataPath := filepath.Join(testDir, "metadata.json")
			if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
				if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
					t.Fatalf("Failed to parse metadata.json: %v", err)
				}
			}

			// Read expected AST from ClickHouse
			var expectedAST astJSON
			astPath := filepath.Join(testDir, "ast.json")
			if astBytes, err := os.ReadFile(astPath); err == nil {
				if err := json.Unmarshal(astBytes, &expectedAST); err != nil {
					t.Fatalf("Failed to parse ast.json: %v", err)
				}
			}

			// Skip tests where ClickHouse also couldn't parse the query
			if expectedAST.Error {
				t.Skipf("ClickHouse also failed to parse this query")
				return
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

			// TODO: Compare parsed AST against expectedAST.Data
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
