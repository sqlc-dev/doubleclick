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
	Todo    bool   `json:"todo,omitempty"`
	Source  string `json:"source,omitempty"`
	Explain *bool  `json:"explain,omitempty"`
	Skip    bool   `json:"skip,omitempty"`
}

// TestParser tests the parser using test cases from the testdata directory.
// Each subdirectory in testdata represents a test case with:
// - query.sql: The SQL query to parse
// - metadata.json (optional): Metadata including:
//   - todo: true if the test is not yet expected to pass
//   - explain: false to skip the test (e.g., when ClickHouse couldn't parse it)
//   - skip: true to skip the test entirely (e.g., causes infinite loop)
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

			// Read the query (only first line)
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}
			// Get first line only
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

			// Skip tests marked with skip: true
			if metadata.Skip {
				t.Skip("Skipping: skip is true in metadata")
			}

			// Skip tests where explain is explicitly false (e.g., ClickHouse couldn't parse it)
			if metadata.Explain != nil && !*metadata.Explain {
				t.Skipf("Skipping: explain is false in metadata")
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
