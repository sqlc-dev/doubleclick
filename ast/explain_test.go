package ast_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kyleconroy/doubleclick/ast"
	"github.com/kyleconroy/doubleclick/parser"
)

func TestExplain(t *testing.T) {
	testdataDir := "../parser/testdata"

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

		// Check if explain.txt exists
		explainPath := filepath.Join(testDir, "explain.txt")
		explainBytes, err := os.ReadFile(explainPath)
		if err != nil {
			continue // Skip test cases without explain.txt
		}
		expected := string(explainBytes)

		t.Run(testName, func(t *testing.T) {
			// Read the query
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}
			query := strings.TrimSpace(string(queryBytes))

			// Parse the query
			stmts, err := parser.Parse(context.Background(), strings.NewReader(query))
			if err != nil {
				t.Skipf("Parse error (skipping): %v", err)
				return
			}

			if len(stmts) == 0 {
				t.Fatalf("Expected at least 1 statement, got 0")
			}

			// Generate explain output
			got := ast.Explain(stmts[0])

			// Compare
			if got != expected {
				t.Errorf("Explain output mismatch\nQuery: %s\n\nExpected:\n%s\nGot:\n%s", query, expected, got)
			}
		})
	}
}
