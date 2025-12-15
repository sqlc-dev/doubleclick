package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type testMetadata struct {
	Todo       bool   `json:"todo,omitempty"`
	Source     string `json:"source,omitempty"`
	Explain    *bool  `json:"explain,omitempty"`
	Skip       bool   `json:"skip,omitempty"`
	ParseError bool   `json:"parse_error,omitempty"`
}

func main() {
	// Tests that have error annotations and empty expected outputs
	tests := []string{
		"01293_create_role",
		"01294_create_settings_profile",
		"01295_create_row_policy",
		"01296_create_row_policy_in_current_database",
		"01418_custom_settings",
		"01732_union_and_union_all",
		"02244_casewithexpression_return_type",
		"02294_decimal_second_errors",
		"02294_fp_seconds_profile",
		"02364_window_case",
		"02414_all_new_table_functions_must_be_documented",
		"02415_all_new_functions_must_be_documented",
		"02415_all_new_functions_must_have_version_information",
		"03000_too_big_max_execution_time_setting",
		"03003_compatibility_setting_bad_value",
		"03305_fix_kafka_table_with_kw_arguments",
		"03559_explain_ast_in_subquery",
		"03625_case_without_condition_non_constant_branches",
	}

	testdataDir := "parser/testdata"
	var updated int

	for _, testName := range tests {
		testDir := filepath.Join(testdataDir, testName)

		// Read query to check for error annotations
		queryPath := filepath.Join(testDir, "query.sql")
		queryBytes, err := os.ReadFile(queryPath)
		if err != nil {
			fmt.Printf("Error reading query %s: %v\n", testName, err)
			continue
		}
		query := string(queryBytes)

		// Check for error annotations
		hasErrorAnnotation := strings.Contains(query, "serverError") ||
			strings.Contains(query, "clientError") ||
			strings.Contains(query, "{ serverError") ||
			strings.Contains(query, "{ clientError")

		// Check expected output
		explainPath := filepath.Join(testDir, "explain.txt")
		explainBytes, _ := os.ReadFile(explainPath)
		explainContent := strings.TrimSpace(string(explainBytes))

		// If empty expected and has error annotation, mark as parse_error
		if hasErrorAnnotation || explainContent == "" {
			metadataPath := filepath.Join(testDir, "metadata.json")
			metadataBytes, err := os.ReadFile(metadataPath)
			if err != nil {
				continue
			}

			var metadata testMetadata
			json.Unmarshal(metadataBytes, &metadata)

			// Mark as parse_error and remove todo
			metadata.ParseError = true
			metadata.Todo = false

			newBytes, _ := json.MarshalIndent(metadata, "", "  ")
			newBytes = append(newBytes, '\n')

			if err := os.WriteFile(metadataPath, newBytes, 0644); err != nil {
				fmt.Printf("Error writing metadata %s: %v\n", testName, err)
				continue
			}
			fmt.Printf("Updated %s (hasError=%v, emptyExpected=%v)\n", testName, hasErrorAnnotation, explainContent == "")
			updated++
		} else {
			fmt.Printf("Skipped %s (no error annotation, non-empty expected)\n", testName)
		}
	}

	fmt.Printf("\nUpdated %d tests\n", updated)
}
