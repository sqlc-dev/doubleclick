package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sqlc-dev/doubleclick/parser"
)

func main() {
	testName := flag.String("test", "", "Single test directory name to process (if empty, process all)")
	dryRun := flag.Bool("dry-run", false, "Print what would be done without making changes")
	flag.Parse()

	testdataDir := "parser/testdata"

	if *testName != "" {
		// Process single test
		if err := processTest(filepath.Join(testdataDir, *testName), *dryRun); err != nil {
			fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", *testName, err)
			os.Exit(1)
		}
		return
	}

	// Process all tests
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading testdata: %v\n", err)
		os.Exit(1)
	}

	var processed, skipped, errors int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		testDir := filepath.Join(testdataDir, entry.Name())
		if err := processTest(testDir, *dryRun); err != nil {
			if strings.Contains(err.Error(), "no statements found") {
				skipped++
			} else {
				fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", entry.Name(), err)
				errors++
			}
		} else {
			processed++
		}
	}

	fmt.Printf("\nProcessed: %d, Skipped: %d, Errors: %d\n", processed, skipped, errors)
	if errors > 0 {
		os.Exit(1)
	}
}

func processTest(testDir string, dryRun bool) error {
	queryPath := filepath.Join(testDir, "query.sql")
	queryBytes, err := os.ReadFile(queryPath)
	if err != nil {
		return fmt.Errorf("reading query.sql: %w", err)
	}

	statements := splitStatements(string(queryBytes))
	if len(statements) == 0 {
		return fmt.Errorf("no statements found")
	}

	testName := filepath.Base(testDir)
	goldenDir := filepath.Join(testDir, "golden", "ast")

	if dryRun {
		fmt.Printf("Would process %s (%d statements) -> %s/\n", testName, len(statements), goldenDir)
		for i, stmt := range statements {
			fmt.Printf("  [%d] %s -> stmt_%04d.json\n", i+1, truncate(stmt, 60), i+1)
		}
		return nil
	}

	// Create golden/ast directory
	if err := os.MkdirAll(goldenDir, 0755); err != nil {
		return fmt.Errorf("creating golden directory: %w", err)
	}

	var stmtErrors []string
	for i, stmt := range statements {
		stmtNum := i + 1

		// Parse the statement
		stmts, parseErr := parser.Parse(context.Background(), strings.NewReader(stmt))
		if len(stmts) == 0 {
			stmtErrors = append(stmtErrors, fmt.Sprintf("stmt %d: parse error: %v", stmtNum, parseErr))
			continue
		}

		// Marshal to pretty JSON
		jsonBytes, err := json.MarshalIndent(stmts[0], "", "  ")
		if err != nil {
			stmtErrors = append(stmtErrors, fmt.Sprintf("stmt %d: json marshal error: %v", stmtNum, err))
			continue
		}

		// Write to golden file
		outputPath := filepath.Join(goldenDir, fmt.Sprintf("stmt_%04d.json", stmtNum))
		if err := os.WriteFile(outputPath, append(jsonBytes, '\n'), 0644); err != nil {
			return fmt.Errorf("writing %s: %w", outputPath, err)
		}
	}

	// Print summary
	if len(stmtErrors) > 0 {
		fmt.Printf("%s: %d stmts, %d errors\n", testName, len(statements), len(stmtErrors))
		for _, e := range stmtErrors {
			fmt.Printf("  %s\n", e)
		}
	} else {
		fmt.Printf("%s: %d stmts OK\n", testName, len(statements))
	}

	return nil
}

// splitStatements splits SQL content into individual statements.
func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines and full-line comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Remove inline comments
		if idx := findCommentStart(trimmed); idx >= 0 {
			trimmed = strings.TrimSpace(trimmed[:idx])
			if trimmed == "" {
				continue
			}
		}

		if current.Len() > 0 {
			current.WriteString(" ")
		}
		current.WriteString(trimmed)

		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" && stmt != ";" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

func findCommentStart(line string) int {
	inString := false
	var stringChar byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		if inString {
			if c == '\\' && i+1 < len(line) {
				i++
				continue
			}
			if c == stringChar {
				inString = false
			}
		} else {
			if c == '\'' || c == '"' || c == '`' {
				inString = true
				stringChar = c
			} else if c == '-' && i+1 < len(line) && line[i+1] == '-' {
				if i+2 >= len(line) || line[i+2] == ' ' || line[i+2] == '\t' {
					return i
				}
			}
		}
	}
	return -1
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
