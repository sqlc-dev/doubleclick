package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kyleconroy/doubleclick/parser"
)

type testMetadata struct {
	Todo       bool   `json:"todo,omitempty"`
	Source     string `json:"source,omitempty"`
	Explain    *bool  `json:"explain,omitempty"`
	Skip       bool   `json:"skip,omitempty"`
	ParseError bool   `json:"parse_error,omitempty"`
}

func main() {
	testdataDir := "parser/testdata"

	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read testdata directory: %v\n", err)
		os.Exit(1)
	}

	var generated, skipped, failed int

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(testdataDir, entry.Name())
		testName := entry.Name()

		// Read optional metadata
		var metadata testMetadata
		metadataPath := filepath.Join(testDir, "metadata.json")
		if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
			if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
				fmt.Printf("SKIP %s: failed to parse metadata.json: %v\n", testName, err)
				skipped++
				continue
			}
		}

		// Skip tests marked with skip: true
		if metadata.Skip {
			skipped++
			continue
		}

		// Skip tests where explain is explicitly false
		if metadata.Explain != nil && !*metadata.Explain {
			skipped++
			continue
		}

		// Skip tests marked as todo (they don't pass yet)
		if metadata.Todo {
			skipped++
			continue
		}

		// Skip tests marked as parse_error (intentionally invalid SQL)
		if metadata.ParseError {
			skipped++
			continue
		}

		// Check if explain.txt exists (we only generate ast.json for tests with explain.txt)
		explainPath := filepath.Join(testDir, "explain.txt")
		expectedBytes, err := os.ReadFile(explainPath)
		if err != nil {
			skipped++
			continue
		}

		// Read the query
		queryPath := filepath.Join(testDir, "query.sql")
		queryBytes, err := os.ReadFile(queryPath)
		if err != nil {
			fmt.Printf("SKIP %s: failed to read query.sql: %v\n", testName, err)
			skipped++
			continue
		}

		// Build query from non-comment lines until we hit a line ending with semicolon
		var queryParts []string
		for _, line := range strings.Split(string(queryBytes), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			lineContent := trimmed
			if idx := strings.Index(trimmed, " -- "); idx >= 0 {
				lineContent = strings.TrimSpace(trimmed[:idx])
			}
			if strings.HasSuffix(lineContent, ";") {
				queryParts = append(queryParts, lineContent)
				break
			}
			queryParts = append(queryParts, trimmed)
		}
		query := strings.Join(queryParts, " ")

		// Parse the query with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		stmts, err := parser.Parse(ctx, strings.NewReader(query))
		cancel()

		if err != nil {
			fmt.Printf("FAIL %s: parse error: %v\n", testName, err)
			failed++
			continue
		}

		if len(stmts) == 0 {
			fmt.Printf("FAIL %s: no statements returned\n", testName)
			failed++
			continue
		}

		// Compare explain output
		expected := strings.TrimSpace(string(expectedBytes))
		// Strip server error messages from expected output
		if idx := strings.Index(expected, "\nThe query succeeded but the server error"); idx != -1 {
			expected = strings.TrimSpace(expected[:idx])
		}
		actual := strings.TrimSpace(parser.Explain(stmts[0]))

		if actual != expected {
			fmt.Printf("FAIL %s: explain mismatch\n", testName)
			failed++
			continue
		}

		// Generate ast.json
		astBytes, err := json.MarshalIndent(stmts[0], "", "  ")
		if err != nil {
			fmt.Printf("FAIL %s: JSON marshal error: %v\n", testName, err)
			failed++
			continue
		}

		astPath := filepath.Join(testDir, "ast.json")
		if err := os.WriteFile(astPath, append(astBytes, '\n'), 0644); err != nil {
			fmt.Printf("FAIL %s: failed to write ast.json: %v\n", testName, err)
			failed++
			continue
		}

		generated++
	}

	fmt.Printf("\nGenerated: %d, Skipped: %d, Failed: %d\n", generated, skipped, failed)
}
