package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	aftership "github.com/AfterShip/clickhouse-sql-parser/parser"
)

type testMetadata struct {
	Todo       bool  `json:"todo,omitempty"`
	Explain    *bool `json:"explain,omitempty"`
	Skip       bool  `json:"skip,omitempty"`
	ParseError bool  `json:"parse_error,omitempty"`
}

func tryParseWithAfterShip(query string) (parsed bool, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			parsed = false
		}
	}()
	p := aftership.NewParser(query)
	stmts, err := p.ParseStmts()
	return err == nil && len(stmts) > 0, false
}

func main() {
	testdataDir := "parser/testdata"

	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	var todoTotal, aftershipParsed, aftershipFailed, aftershipPanicked int
	var parsedQueries, failedQueries []string

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(testdataDir, entry.Name())

		// Read metadata
		var metadata testMetadata
		metadataPath := filepath.Join(testDir, "metadata.json")
		metadataBytes, err := os.ReadFile(metadataPath)
		if err != nil {
			continue
		}
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			continue
		}

		// Only look at todo tests (excluding parse_error and explain:false)
		if !metadata.Todo || metadata.ParseError || metadata.Skip {
			continue
		}
		if metadata.Explain != nil && !*metadata.Explain {
			continue
		}

		todoTotal++

		// Read query
		queryPath := filepath.Join(testDir, "query.sql")
		queryBytes, err := os.ReadFile(queryPath)
		if err != nil {
			continue
		}

		var queryParts []string
		for _, line := range strings.Split(string(queryBytes), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "#") {
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

		// Try AfterShip parser
		parsed, panicked := tryParseWithAfterShip(query)
		if panicked {
			aftershipPanicked++
			aftershipFailed++
			failedQueries = append(failedQueries, fmt.Sprintf("[PANIC] %s", entry.Name()))
		} else if parsed {
			aftershipParsed++
			parsedQueries = append(parsedQueries, entry.Name())
		} else {
			aftershipFailed++
			failedQueries = append(failedQueries, entry.Name())
		}
	}

	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║   Comparison: TODO queries (doubleclick can't parse yet)   ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Total TODO tests:        %3d                              ║\n", todoTotal)
	fmt.Printf("║  AfterShip CAN parse:     %3d  (%4.1f%%)                     ║\n", aftershipParsed, float64(aftershipParsed)/float64(todoTotal)*100)
	fmt.Printf("║  AfterShip CANNOT parse:  %3d  (%4.1f%%)                     ║\n", aftershipFailed, float64(aftershipFailed)/float64(todoTotal)*100)
	fmt.Printf("║  AfterShip CRASHED:       %3d                               ║\n", aftershipPanicked)
	fmt.Println("╚════════════════════════════════════════════════════════════╝")

	if len(parsedQueries) > 0 {
		fmt.Printf("\nAfterShip CAN parse these %d TODO queries:\n", len(parsedQueries))
		for i, q := range parsedQueries {
			if i >= 30 {
				fmt.Printf("  ... and %d more\n", len(parsedQueries)-30)
				break
			}
			fmt.Printf("  - %s\n", q)
		}
	}
}
