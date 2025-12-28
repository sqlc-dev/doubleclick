package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type testMetadata struct {
	ExplainTodo map[string]bool `json:"explain_todo,omitempty"`
	Explain     *bool           `json:"explain,omitempty"`
	Skip        bool            `json:"skip,omitempty"`
	ParseError  bool            `json:"parse_error,omitempty"`
}

type todoTest struct {
	name           string
	querySize      int
	explainTodoLen int
}

func main() {
	testdataDir := "parser/testdata"
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading testdata: %v\n", err)
		os.Exit(1)
	}

	var todoTests []todoTest

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(testdataDir, entry.Name())
		metadataPath := filepath.Join(testDir, "metadata.json")

		// Read metadata
		metadataBytes, err := os.ReadFile(metadataPath)
		if err != nil {
			continue
		}

		var metadata testMetadata
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			continue
		}

		// Skip tests with skip or explain=false or parse_error
		if metadata.Skip || (metadata.Explain != nil && !*metadata.Explain) || metadata.ParseError {
			continue
		}

		// Check for explain_todo entries
		if len(metadata.ExplainTodo) == 0 {
			continue
		}

		// Read query to get its size
		queryPath := filepath.Join(testDir, "query.sql")
		queryBytes, err := os.ReadFile(queryPath)
		if err != nil {
			continue
		}

		todoTests = append(todoTests, todoTest{
			name:           entry.Name(),
			querySize:      len(queryBytes),
			explainTodoLen: len(metadata.ExplainTodo),
		})
	}

	if len(todoTests) == 0 {
		fmt.Printf("No explain_todo tests found!\n")
		return
	}

	// Sort by explain_todo count (fewest first), then by query size
	sort.Slice(todoTests, func(i, j int) bool {
		if todoTests[i].explainTodoLen != todoTests[j].explainTodoLen {
			return todoTests[i].explainTodoLen < todoTests[j].explainTodoLen
		}
		return todoTests[i].querySize < todoTests[j].querySize
	})

	// Print the best candidate
	next := todoTests[0]
	testDir := filepath.Join(testdataDir, next.name)

	fmt.Printf("Next explain_todo test: %s (%d pending statements)\n\n", next.name, next.explainTodoLen)

	// Print query.sql contents
	queryPath := filepath.Join(testDir, "query.sql")
	queryBytes, _ := os.ReadFile(queryPath)
	fmt.Printf("Query (%d bytes):\n%s\n", next.querySize, string(queryBytes))

	// Print explain.txt contents if it exists
	explainPath := filepath.Join(testDir, "explain.txt")
	if explainBytes, err := os.ReadFile(explainPath); err == nil {
		fmt.Printf("\nExpected EXPLAIN output:\n%s\n", string(explainBytes))
	}

	// Print explain_todo entries
	metadataPath := filepath.Join(testDir, "metadata.json")
	if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
		var metadata testMetadata
		if json.Unmarshal(metadataBytes, &metadata) == nil {
			fmt.Printf("\nPending statements (explain_todo):\n")
			for stmt := range metadata.ExplainTodo {
				fmt.Printf("  - %s\n", stmt)
			}
		}
	}

	fmt.Printf("\nRemaining explain_todo tests: %d\n", len(todoTests))
}
