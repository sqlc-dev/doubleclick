package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

var formatFlag = flag.Bool("format", false, "Find tests with todo_format: true")
var explainFlag = flag.Bool("explain", false, "Find tests with explain_todo entries (fewest first)")

type testMetadata struct {
	TodoFormat  bool            `json:"todo_format,omitempty"`
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
	flag.Parse()

	if !*formatFlag && !*explainFlag {
		fmt.Fprintf(os.Stderr, "Usage: go run ./cmd/next-test [-format | -explain]\n")
		fmt.Fprintf(os.Stderr, "  -format   Find tests with todo_format: true\n")
		fmt.Fprintf(os.Stderr, "  -explain  Find tests with explain_todo entries (fewest first)\n")
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

		// Skip tests with skip or explain=false or parse_error
		if metadata.Skip || (metadata.Explain != nil && !*metadata.Explain) || metadata.ParseError {
			continue
		}

		// Check based on flag
		if *formatFlag {
			if !metadata.TodoFormat {
				continue
			}
		} else if *explainFlag {
			if len(metadata.ExplainTodo) == 0 {
				continue
			}
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

	todoType := "todo_format"
	if *explainFlag {
		todoType = "explain_todo"
	}

	if len(todoTests) == 0 {
		fmt.Printf("No %s tests found!\n", todoType)
		return
	}

	// Sort based on mode
	if *explainFlag {
		// Sort by explain_todo count (fewest first), then by query size
		sort.Slice(todoTests, func(i, j int) bool {
			if todoTests[i].explainTodoLen != todoTests[j].explainTodoLen {
				return todoTests[i].explainTodoLen < todoTests[j].explainTodoLen
			}
			return todoTests[i].querySize < todoTests[j].querySize
		})
	} else {
		// Sort by query size (shortest first)
		sort.Slice(todoTests, func(i, j int) bool {
			return todoTests[i].querySize < todoTests[j].querySize
		})
	}

	// Print the best candidate
	next := todoTests[0]
	testDir := filepath.Join(testdataDir, next.name)

	if *explainFlag {
		fmt.Printf("Next %s test: %s (%d pending statements)\n\n", todoType, next.name, next.explainTodoLen)
	} else {
		fmt.Printf("Next %s test: %s\n\n", todoType, next.name)
	}

	// Print query.sql contents
	queryPath := filepath.Join(testDir, "query.sql")
	queryBytes, _ := os.ReadFile(queryPath)
	fmt.Printf("Query (%d bytes):\n%s\n", next.querySize, string(queryBytes))

	// Print explain.txt contents if it exists
	explainPath := filepath.Join(testDir, "explain.txt")
	if explainBytes, err := os.ReadFile(explainPath); err == nil {
		fmt.Printf("\nExpected EXPLAIN output:\n%s\n", string(explainBytes))
	}

	// Print explain_todo entries if in explain mode
	if *explainFlag {
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
	}

	fmt.Printf("\nRemaining %s tests: %d\n", todoType, len(todoTests))
}
