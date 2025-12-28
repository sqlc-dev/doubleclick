package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	testName := flag.String("test", "", "Single test directory name to process (if empty, process all)")
	clickhouseBin := flag.String("bin", "./clickhouse", "Path to ClickHouse binary")
	dryRun := flag.Bool("dry-run", false, "Print statements without executing")
	flag.Parse()

	// Check if clickhouse binary exists
	if !*dryRun {
		if _, err := os.Stat(*clickhouseBin); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "ClickHouse binary not found at %s\n", *clickhouseBin)
			fmt.Fprintf(os.Stderr, "Run: ./scripts/clickhouse.sh download\n")
			os.Exit(1)
		}
	}

	testdataDir := "parser/testdata"

	if *testName != "" {
		// Process single test
		if err := processTest(filepath.Join(testdataDir, *testName), *clickhouseBin, *dryRun); err != nil {
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

	var errors []string
	var processed, skipped int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		testDir := filepath.Join(testdataDir, entry.Name())
		if err := processTest(testDir, *clickhouseBin, *dryRun); err != nil {
			if strings.Contains(err.Error(), "no statements found") {
				skipped++
				continue
			}
			errors = append(errors, fmt.Sprintf("%s: %v", entry.Name(), err))
		} else {
			processed++
		}
	}

	fmt.Printf("\nProcessed: %d, Skipped: %d, Errors: %d\n", processed, skipped, len(errors))
	if len(errors) > 0 {
		fmt.Fprintf(os.Stderr, "\nErrors:\n")
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "  %s\n", e)
		}
		os.Exit(1)
	}
}

func processTest(testDir, clickhouseBin string, dryRun bool) error {
	queryPath := filepath.Join(testDir, "query.sql")
	queryBytes, err := os.ReadFile(queryPath)
	if err != nil {
		return fmt.Errorf("reading query.sql: %w", err)
	}

	statements := splitStatements(string(queryBytes))
	if len(statements) == 0 {
		return fmt.Errorf("no statements found")
	}

	fmt.Printf("Processing %s (%d statements)\n", filepath.Base(testDir), len(statements))

	// Only process statements 2+ (skip first statement, keep existing explain.txt)
	for i, stmt := range statements {
		stmtNum := i + 1 // 1-indexed
		if dryRun {
			fmt.Printf("  [%d] %s\n", stmtNum, truncate(stmt, 80))
			continue
		}

		// Skip the first statement - don't touch explain.txt
		if i == 0 {
			fmt.Printf("  [%d] (skipped - keeping existing explain.txt)\n", stmtNum)
			continue
		}

		explain, err := explainAST(clickhouseBin, stmt)
		if err != nil {
			fmt.Printf("  [%d] ERROR: %v\n", stmtNum, err)
			// Skip statements that fail - they might be intentionally invalid
			continue
		}

		// Output filename: explain_N.txt for N >= 2
		outputPath := filepath.Join(testDir, fmt.Sprintf("explain_%d.txt", stmtNum))

		if err := os.WriteFile(outputPath, []byte(explain+"\n"), 0644); err != nil {
			return fmt.Errorf("writing %s: %w", outputPath, err)
		}
		fmt.Printf("  [%d] -> %s\n", stmtNum, filepath.Base(outputPath))
	}

	return nil
}

// splitStatements splits SQL content into individual statements.
// It handles:
// - Comments (-- line comments)
// - Multi-line statements
// - Multiple statements separated by semicolons
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

		// Remove inline comments (-- comment at end of line)
		// But be careful about comments inside strings
		if idx := findCommentStart(trimmed); idx >= 0 {
			trimmed = strings.TrimSpace(trimmed[:idx])
			if trimmed == "" {
				continue
			}
		}

		// Add to current statement
		if current.Len() > 0 {
			current.WriteString(" ")
		}
		current.WriteString(trimmed)

		// Check if statement is complete (ends with ;)
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			// Remove trailing semicolon for EXPLAIN AST
			stmt = strings.TrimSuffix(stmt, ";")
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	// Handle statement without trailing semicolon
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		stmt = strings.TrimSuffix(stmt, ";")
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

// findCommentStart finds the position of -- comment that's not inside a string
func findCommentStart(line string) int {
	inString := false
	var stringChar byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		if inString {
			if c == '\\' && i+1 < len(line) {
				i++ // Skip escaped character
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
				// Check if this looks like a comment (followed by space or end of line)
				if i+2 >= len(line) || line[i+2] == ' ' || line[i+2] == '\t' {
					return i
				}
			}
		}
	}
	return -1
}

// explainAST runs EXPLAIN AST on the statement using clickhouse local
func explainAST(clickhouseBin, stmt string) (string, error) {
	query := fmt.Sprintf("EXPLAIN AST %s", stmt)
	cmd := exec.Command(clickhouseBin, "local", "--query", query)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg == "" {
			errMsg = err.Error()
		}
		return "", fmt.Errorf("%s", errMsg)
	}

	return strings.TrimSpace(stdout.String()), nil
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ") // Normalize whitespace
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
