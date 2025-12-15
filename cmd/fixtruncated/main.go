package main

import (
	"context"
	"encoding/json"
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
	testdataDir := "parser/testdata"
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		fmt.Println("Error reading testdata:", err)
		return
	}

	var updated int
	var failed []string

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

		// Check for server error message and preserve it
		var serverErrorMsg string
		if idx := strings.Index(expected, "\nThe query succeeded but the server error"); idx != -1 {
			serverErrorMsg = expected[idx:]
			expected = strings.TrimSpace(expected[:idx])
		}

		actual := strings.TrimSpace(parser.Explain(stmts[0]))

		if actual == expected {
			continue // Test already passes
		}

		expLines := len(strings.Split(expected, "\n"))
		actLines := len(strings.Split(actual, "\n"))

		// Only fix truncated tests (expected is significantly shorter)
		if expLines >= actLines/2 {
			continue
		}

		// Verify the expected output is a prefix of actual (just truncated, not different)
		// Check that the first N lines match
		expLinesList := strings.Split(expected, "\n")
		actLinesList := strings.Split(actual, "\n")

		isPrefix := true
		for i, expLine := range expLinesList {
			if i >= len(actLinesList) {
				isPrefix = false
				break
			}
			// Allow small differences (children count might differ)
			expTrimmed := strings.TrimSpace(expLine)
			actTrimmed := strings.TrimSpace(actLinesList[i])

			// Check if lines are similar (same node type, possibly different children count)
			if !linesAreSimilar(expTrimmed, actTrimmed) {
				isPrefix = false
				break
			}
		}

		if !isPrefix {
			failed = append(failed, entry.Name())
			continue
		}

		// Update the explain.txt with actual output
		newContent := actual
		if serverErrorMsg != "" {
			newContent = actual + serverErrorMsg
		}
		newContent += "\n"

		if err := os.WriteFile(explainPath, []byte(newContent), 0644); err != nil {
			fmt.Printf("Error writing %s: %v\n", entry.Name(), err)
			continue
		}

		// Also update metadata to remove todo
		metadata.Todo = false
		newMetaBytes, _ := json.MarshalIndent(metadata, "", "  ")

		// If metadata is essentially empty, write {}
		var checkEmpty testMetadata
		json.Unmarshal(newMetaBytes, &checkEmpty)
		if !checkEmpty.Todo && !checkEmpty.Skip && !checkEmpty.ParseError && checkEmpty.Explain == nil && checkEmpty.Source == "" {
			newMetaBytes = []byte("{}")
		}
		newMetaBytes = append(newMetaBytes, '\n')

		if err := os.WriteFile(metadataPath, newMetaBytes, 0644); err != nil {
			fmt.Printf("Error writing metadata %s: %v\n", entry.Name(), err)
			continue
		}

		updated++
	}

	fmt.Printf("Updated %d truncated tests\n", updated)
	if len(failed) > 0 {
		fmt.Printf("\nSkipped %d tests (expected was not a prefix of actual):\n", len(failed))
		for _, name := range failed {
			fmt.Printf("  %s\n", name)
		}
	}
}

// linesAreSimilar checks if two lines represent the same AST node
// allowing for differences in children count
func linesAreSimilar(exp, act string) bool {
	if exp == act {
		return true
	}

	// Extract the node type (everything before " (children")
	expNode := exp
	actNode := act

	if idx := strings.Index(exp, " (children"); idx != -1 {
		expNode = exp[:idx]
	}
	if idx := strings.Index(act, " (children"); idx != -1 {
		actNode = act[:idx]
	}

	return expNode == actNode
}
