package parser_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	aftership "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// TestAfterShipParser tests the AfterShip/clickhouse-sql-parser against all valid testdata queries.
// Use with: go test ./parser -run TestAfterShipParser -v
func TestAfterShipParser(t *testing.T) {
	testdataDir := "testdata"

	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("Failed to read testdata directory: %v", err)
	}

	var passed, failed, skipped int
	var failedTests []string

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(testdataDir, entry.Name())

		t.Run(entry.Name(), func(t *testing.T) {
			// Read optional metadata
			var metadata testMetadata
			metadataPath := filepath.Join(testDir, "metadata.json")
			if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
				if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
					t.Fatalf("Failed to parse metadata.json: %v", err)
				}
			}

			// Skip tests marked with skip: true
			if metadata.Skip {
				skipped++
				t.Skip("Skipping: skip is true in metadata")
			}

			// Skip tests where explain is explicitly false (ClickHouse couldn't parse it)
			if metadata.Explain != nil && !*metadata.Explain {
				skipped++
				t.Skipf("Skipping: explain is false in metadata")
				return
			}

			// Skip tests marked as parse_error (intentionally invalid SQL)
			if metadata.ParseError {
				skipped++
				t.Skipf("Skipping: parse_error is true (intentionally invalid SQL)")
				return
			}

			// Read the query (handle multi-line queries)
			queryPath := filepath.Join(testDir, "query.sql")
			queryBytes, err := os.ReadFile(queryPath)
			if err != nil {
				t.Fatalf("Failed to read query.sql: %v", err)
			}

			// Build query from non-comment lines until we hit a line ending with semicolon
			var queryParts []string
			for _, line := range strings.Split(string(queryBytes), "\n") {
				trimmed := strings.TrimSpace(line)
				if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "#") {
					continue
				}
				// Remove trailing comment if present
				lineContent := trimmed
				if idx := strings.Index(trimmed, " -- "); idx >= 0 {
					lineContent = strings.TrimSpace(trimmed[:idx])
				}
				// Check if line ends with semicolon
				if strings.HasSuffix(lineContent, ";") {
					queryParts = append(queryParts, lineContent)
					break
				}
				queryParts = append(queryParts, trimmed)
			}
			query := strings.Join(queryParts, " ")

			// Parse using AfterShip parser
			p := aftership.NewParser(query)
			stmts, parseErr := p.ParseStmts()

			if parseErr != nil {
				failed++
				failedTests = append(failedTests, entry.Name())
				t.Errorf("AfterShip parse error: %v\nQuery: %s", parseErr, query)
				return
			}

			if len(stmts) == 0 {
				failed++
				failedTests = append(failedTests, entry.Name())
				t.Errorf("AfterShip parser returned no statements\nQuery: %s", query)
				return
			}

			passed++
		})
	}

	t.Logf("\n=== AfterShip Parser Results ===")
	t.Logf("Passed:  %d", passed)
	t.Logf("Failed:  %d", failed)
	t.Logf("Skipped: %d", skipped)
	t.Logf("Total:   %d", passed+failed+skipped)
	if len(failedTests) > 0 && len(failedTests) <= 50 {
		t.Logf("\nFailed tests:")
		for _, name := range failedTests {
			t.Logf("  - %s", name)
		}
	}
}

// tryParseWithAfterShip attempts to parse a query with AfterShip parser, recovering from panics.
func tryParseWithAfterShip(query string) (stmts []aftership.Expr, parseErr error, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			parseErr = nil
			stmts = nil
		}
	}()
	p := aftership.NewParser(query)
	stmts, parseErr = p.ParseStmts()
	return stmts, parseErr, false
}

// TestAfterShipParserSummary provides a summary of AfterShip parser compatibility.
// Use with: go test ./parser -run TestAfterShipParserSummary -v
func TestAfterShipParserSummary(t *testing.T) {
	testdataDir := "testdata"

	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("Failed to read testdata directory: %v", err)
	}

	var passed, failed, skipped, panics int
	var failedQueries []struct {
		name  string
		query string
		err   string
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(testdataDir, entry.Name())

		// Read optional metadata
		var metadata testMetadata
		metadataPath := filepath.Join(testDir, "metadata.json")
		if metadataBytes, err := os.ReadFile(metadataPath); err == nil {
			json.Unmarshal(metadataBytes, &metadata)
		}

		// Skip invalid/unparseable tests
		if metadata.Skip || metadata.ParseError || (metadata.Explain != nil && !*metadata.Explain) {
			skipped++
			continue
		}

		// Read and prepare query
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

		// Parse using AfterShip parser (with panic recovery)
		stmts, parseErr, panicked := tryParseWithAfterShip(query)

		if panicked {
			panics++
			failed++
			failedQueries = append(failedQueries, struct {
				name  string
				query string
				err   string
			}{entry.Name(), query, "PANIC: parser crashed"})
		} else if parseErr != nil || len(stmts) == 0 {
			failed++
			errMsg := "no statements returned"
			if parseErr != nil {
				errMsg = parseErr.Error()
			}
			failedQueries = append(failedQueries, struct {
				name  string
				query string
				err   string
			}{entry.Name(), query, errMsg})
		} else {
			passed++
		}
	}

	total := passed + failed + skipped
	passRate := float64(passed) / float64(passed+failed) * 100

	t.Logf("\n")
	t.Logf("╔════════════════════════════════════════════════════════════╗")
	t.Logf("║        AfterShip/clickhouse-sql-parser Results            ║")
	t.Logf("╠════════════════════════════════════════════════════════════╣")
	t.Logf("║  Passed:      %5d                                        ║", passed)
	t.Logf("║  Failed:      %5d (includes %d panics/crashes)            ║", failed, panics)
	t.Logf("║  Skipped:     %5d (invalid SQL / unparseable)            ║", skipped)
	t.Logf("║  Total:       %5d                                        ║", total)
	t.Logf("║  Pass Rate:   %5.1f%% (of valid queries)                   ║", passRate)
	t.Logf("╚════════════════════════════════════════════════════════════╝")

	// Show first 20 failed queries
	if len(failedQueries) > 0 {
		t.Logf("\nFirst %d failed queries:", min(20, len(failedQueries)))
		for i, fq := range failedQueries {
			if i >= 20 {
				t.Logf("  ... and %d more", len(failedQueries)-20)
				break
			}
			shortQuery := fq.query
			if len(shortQuery) > 60 {
				shortQuery = shortQuery[:60] + "..."
			}
			t.Logf("  [%s] %s", fq.name, fq.err)
			t.Logf("    Query: %s", shortQuery)
		}
	}
}
