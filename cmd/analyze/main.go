package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sqlc-dev/doubleclick/parser"
)

type testMetadata struct {
	Todo       bool  `json:"todo,omitempty"`
	Explain    *bool `json:"explain,omitempty"`
	Skip       bool  `json:"skip,omitempty"`
	ParseError bool  `json:"parse_error,omitempty"`
}

func main() {
	testdataDir := "parser/testdata"
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		fmt.Println("Error reading testdata:", err)
		return
	}

	var truncatedTests []struct {
		name     string
		expLines int
		actLines int
		expected string
		actual   string
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(testdataDir, entry.Name())
		metadataPath := filepath.Join(testDir, "metadata.json")

		// Read metadata
		var metadata testMetadata
		metadataBytes, err := os.ReadFile(metadataPath)
		if err != nil {
			continue
		}
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			continue
		}

		// Only check tests marked as todo
		if !metadata.Todo {
			continue
		}

		// Skip tests with skip or explain=false or parse_error
		if metadata.Skip || (metadata.Explain != nil && !*metadata.Explain) || metadata.ParseError {
			continue
		}

		// Read query
		queryPath := filepath.Join(testDir, "query.sql")
		queryBytes, err := os.ReadFile(queryPath)
		if err != nil {
			continue
		}

		// Build query
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

		// Parse query
		stmts, err := parser.Parse(context.Background(), strings.NewReader(query))
		if err != nil {
			continue
		}
		if len(stmts) == 0 {
			continue
		}

		// Check explain output
		explainPath := filepath.Join(testDir, "explain.txt")
		expectedBytes, err := os.ReadFile(explainPath)
		if err != nil {
			continue
		}
		expected := strings.TrimSpace(string(expectedBytes))
		if idx := strings.Index(expected, "\nThe query succeeded but the server error"); idx != -1 {
			expected = strings.TrimSpace(expected[:idx])
		}

		actual := strings.TrimSpace(parser.Explain(stmts[0]))

		if actual == expected {
			continue // Test passes
		}

		expLines := len(strings.Split(expected, "\n"))
		actLines := len(strings.Split(actual, "\n"))

		// Check if expected is significantly shorter (truncated)
		if expLines < actLines/2 {
			truncatedTests = append(truncatedTests, struct {
				name     string
				expLines int
				actLines int
				expected string
				actual   string
			}{entry.Name(), expLines, actLines, expected, actual})
		}
	}

	fmt.Printf("Found %d tests with truncated expected output\n\n", len(truncatedTests))

	// Show first 5 examples
	for i, t := range truncatedTests {
		if i >= 5 {
			break
		}
		fmt.Printf("=== %s ===\n", t.name)
		fmt.Printf("Expected lines: %d, Actual lines: %d\n", t.expLines, t.actLines)
		fmt.Printf("\nExpected:\n%s\n", t.expected)
		fmt.Printf("\nActual (first 20 lines):\n")
		lines := strings.Split(t.actual, "\n")
		for j, line := range lines {
			if j >= 20 {
				fmt.Printf("... (%d more lines)\n", len(lines)-20)
				break
			}
			fmt.Println(line)
		}
		fmt.Println()
	}

	// Analyze patterns
	fmt.Println("\n=== Pattern Analysis ===")
	patterns := make(map[string]int)
	for _, t := range truncatedTests {
		// Check what's in expected that might be different
		if strings.Contains(t.expected, "SelectQuery (children 1)") {
			patterns["SelectQuery children=1"]++
		}
		if strings.Contains(t.expected, "CreateQuery") && strings.Contains(t.expected, "(children 1)") {
			patterns["CreateQuery children=1"]++
		}
		if !strings.Contains(t.expected, "TablesInSelectQuery") && strings.Contains(t.actual, "TablesInSelectQuery") {
			patterns["Missing TablesInSelectQuery"]++
		}
		if !strings.Contains(t.expected, "ExpressionList") && strings.Contains(t.actual, "ExpressionList") {
			patterns["Missing some ExpressionList"]++
		}
	}
	for pattern, count := range patterns {
		fmt.Printf("%s: %d\n", pattern, count)
	}
}
