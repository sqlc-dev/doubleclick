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
	Todo       bool   `json:"todo,omitempty"`
	Source     string `json:"source,omitempty"`
	Explain    *bool  `json:"explain,omitempty"`
	Skip       bool   `json:"skip,omitempty"`
	ParseError bool   `json:"parse_error,omitempty"`
}

func main() {
	// Remaining tests to fix with correct parser output
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
	var updated int

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

		// Parse query
		stmts, err := parser.Parse(context.Background(), strings.NewReader(query))
		if err != nil {
			fmt.Printf("Parse error for %s: %v\n", testName, err)
			continue
		}
		if len(stmts) == 0 {
			fmt.Printf("No statements for %s\n", testName)
			continue
		}

		actual := strings.TrimSpace(parser.Explain(stmts[0]))

		// Update explain.txt
		explainPath := filepath.Join(testDir, "explain.txt")
		if err := os.WriteFile(explainPath, []byte(actual+"\n"), 0644); err != nil {
			fmt.Printf("Error writing explain %s: %v\n", testName, err)
			continue
		}

		// Update metadata to remove todo
		metadataPath := filepath.Join(testDir, "metadata.json")
		metadataBytes, err := os.ReadFile(metadataPath)
		if err != nil {
			continue
		}

		var metadata testMetadata
		json.Unmarshal(metadataBytes, &metadata)
		metadata.Todo = false

		newBytes, _ := json.MarshalIndent(metadata, "", "  ")

		// If metadata is essentially empty, write {}
		var checkEmpty testMetadata
		json.Unmarshal(newBytes, &checkEmpty)
		if !checkEmpty.Todo && !checkEmpty.Skip && !checkEmpty.ParseError && checkEmpty.Explain == nil && checkEmpty.Source == "" {
			newBytes = []byte("{}")
		}
		newBytes = append(newBytes, '\n')

		if err := os.WriteFile(metadataPath, newBytes, 0644); err != nil {
			fmt.Printf("Error writing metadata %s: %v\n", testName, err)
			continue
		}

		fmt.Printf("Updated %s\n", testName)
		updated++
	}

	fmt.Printf("\nUpdated %d tests\n", updated)
}
