package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type testMetadata struct {
	Todo       bool  `json:"todo,omitempty"`
	Explain    *bool `json:"explain,omitempty"`
	Skip       bool  `json:"skip,omitempty"`
	ParseError bool  `json:"parse_error,omitempty"`
}

type todoTest struct {
	name      string
	querySize int
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

		// Only include tests marked as todo
		if !metadata.Todo {
			continue
		}

		// Skip tests with skip or explain=false or parse_error
		if metadata.Skip || (metadata.Explain != nil && !*metadata.Explain) || metadata.ParseError {
			continue
		}

		// Read query to get its size
		queryPath := filepath.Join(testDir, "query.sql")
		queryBytes, err := os.ReadFile(queryPath)
		if err != nil {
			continue
		}

		todoTests = append(todoTests, todoTest{
			name:      entry.Name(),
			querySize: len(queryBytes),
		})
	}

	if len(todoTests) == 0 {
		fmt.Println("No todo tests found!")
		return
	}

	// Sort by query size (shortest first)
	sort.Slice(todoTests, func(i, j int) bool {
		return todoTests[i].querySize < todoTests[j].querySize
	})

	// Print the shortest one
	next := todoTests[0]
	testDir := filepath.Join(testdataDir, next.name)

	fmt.Printf("Next test: %s\n\n", next.name)

	// Print query.sql contents
	queryPath := filepath.Join(testDir, "query.sql")
	queryBytes, _ := os.ReadFile(queryPath)
	fmt.Printf("Query (%d bytes):\n%s\n", next.querySize, string(queryBytes))

	// Print explain.txt contents if it exists
	explainPath := filepath.Join(testDir, "explain.txt")
	if explainBytes, err := os.ReadFile(explainPath); err == nil {
		fmt.Printf("\nExpected EXPLAIN output:\n%s\n", string(explainBytes))
	}

	fmt.Printf("\nRemaining todo tests: %d\n", len(todoTests))
}
