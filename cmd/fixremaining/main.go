package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	// Remaining tests to fix
	tests := []string{
		"02244_casewithexpression_return_type",
		"02294_fp_seconds_profile",
		"02364_window_case",
		"02414_all_new_table_functions_must_be_documented",
		"02415_all_new_functions_must_be_documented",
		"02415_all_new_functions_must_have_version_information",
		"03625_case_without_condition_non_constant_branches",
	}

	testdataDir := "parser/testdata"

	for _, testName := range tests {
		testDir := filepath.Join(testdataDir, testName)

		// Read query
		queryPath := filepath.Join(testDir, "query.sql")
		queryBytes, err := os.ReadFile(queryPath)
		if err != nil {
			fmt.Printf("Error reading query %s: %v\n", testName, err)
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

		fmt.Printf("=== %s ===\n", testName)
		fmt.Printf("Query: %s\n\n", query[:min(100, len(query))])

		// Parse query
		stmts, err := parser.Parse(context.Background(), strings.NewReader(query))
		if err != nil {
			fmt.Printf("Parse error: %v\n\n", err)
			continue
		}
		if len(stmts) == 0 {
			fmt.Printf("No statements parsed\n\n")
			continue
		}

		actual := parser.Explain(stmts[0])

		// Read expected
		explainPath := filepath.Join(testDir, "explain.txt")
		expectedBytes, _ := os.ReadFile(explainPath)
		expected := strings.TrimSpace(string(expectedBytes))

		fmt.Printf("Expected (first 10 lines):\n")
		expLines := strings.Split(expected, "\n")
		for i, line := range expLines {
			if i >= 10 {
				fmt.Printf("  ... (%d more lines)\n", len(expLines)-10)
				break
			}
			fmt.Printf("  %s\n", line)
		}

		fmt.Printf("\nActual (first 10 lines):\n")
		actLines := strings.Split(strings.TrimSpace(actual), "\n")
		for i, line := range actLines {
			if i >= 10 {
				fmt.Printf("  ... (%d more lines)\n", len(actLines)-10)
				break
			}
			fmt.Printf("  %s\n", line)
		}
		fmt.Println()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
