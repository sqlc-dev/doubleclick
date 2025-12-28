package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

var formatFlag = flag.Bool("format", false, "Find tests with todo_format (required)")

type testMetadata struct {
	TodoFormat bool  `json:"todo_format,omitempty"`
	Explain    *bool `json:"explain,omitempty"`
	Skip       bool  `json:"skip,omitempty"`
	ParseError bool  `json:"parse_error,omitempty"`
}

type todoTest struct {
	name      string
	querySize int
}

func main() {
	flag.Parse()

	if !*formatFlag {
		fmt.Fprintf(os.Stderr, "Usage: go run ./cmd/next-test -format\n")
		fmt.Fprintf(os.Stderr, "Finds tests with todo_format: true in metadata.\n")
		os.Exit(1)
	}

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

		// Check for todo_format
		if !metadata.TodoFormat {
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
		fmt.Printf("No todo_format tests found!\n")
		return
	}

	// Sort by query size (shortest first)
	sort.Slice(todoTests, func(i, j int) bool {
		return todoTests[i].querySize < todoTests[j].querySize
	})

	// Print the shortest one
	next := todoTests[0]
	testDir := filepath.Join(testdataDir, next.name)

	fmt.Printf("Next todo_format test: %s\n\n", next.name)

	// Print query.sql contents
	queryPath := filepath.Join(testDir, "query.sql")
	queryBytes, _ := os.ReadFile(queryPath)
	fmt.Printf("Query (%d bytes):\n%s\n", next.querySize, string(queryBytes))

	// Print explain.txt contents if it exists
	explainPath := filepath.Join(testDir, "explain.txt")
	if explainBytes, err := os.ReadFile(explainPath); err == nil {
		fmt.Printf("\nExpected EXPLAIN output:\n%s\n", string(explainBytes))
	}

	fmt.Printf("\nRemaining todo_format tests: %d\n", len(todoTests))
}
