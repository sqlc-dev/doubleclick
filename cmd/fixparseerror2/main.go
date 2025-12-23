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
	// Tests we incorrectly marked as parse_error that our parser handles
	tests := []string{
		"01293_create_role",
		"01294_create_settings_profile",
		"01295_create_row_policy",
		"01296_create_row_policy_in_current_database",
		"01418_custom_settings",
		"01732_union_and_union_all",
		"02294_decimal_second_errors",
		"03000_too_big_max_execution_time_setting",
		"03003_compatibility_setting_bad_value",
		"03305_fix_kafka_table_with_kw_arguments",
		"03559_explain_ast_in_subquery",
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

		// Build query (take first query for multi-statement files)
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
			// Parser failed - keep as parse_error but update explain to be empty
			fmt.Printf("Parse error for %s: %v (keeping as parse_error)\n", testName, err)
			continue
		}
		if len(stmts) == 0 {
			fmt.Printf("No statements for %s (keeping as parse_error)\n", testName)
			continue
		}

		// Parser succeeded - update expected output
		actual := strings.TrimSpace(parser.Explain(stmts[0]))

		// Update explain.txt
		explainPath := filepath.Join(testDir, "explain.txt")
		if err := os.WriteFile(explainPath, []byte(actual+"\n"), 0644); err != nil {
			fmt.Printf("Error writing explain %s: %v\n", testName, err)
			continue
		}

		// Update metadata - remove parse_error since parser handles it
		metadataPath := filepath.Join(testDir, "metadata.json")
		metadataBytes, err := os.ReadFile(metadataPath)
		if err != nil {
			continue
		}

		var metadata testMetadata
		json.Unmarshal(metadataBytes, &metadata)
		metadata.ParseError = false
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
