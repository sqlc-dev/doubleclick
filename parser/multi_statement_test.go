package parser_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kyleconroy/doubleclick/parser"
)

func TestMultiStatementParsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{
			name:     "two selects with semicolon",
			sql:      "SELECT 1; SELECT 2;",
			expected: 2,
		},
		{
			name:     "three selects",
			sql:      "SELECT 1; SELECT 2; SELECT 3;",
			expected: 3,
		},
		{
			name:     "mixed statements",
			sql:      "SELECT 1; CREATE TABLE t (a Int32); DROP TABLE t;",
			expected: 3,
		},
		{
			name:     "no trailing semicolon",
			sql:      "SELECT 1; SELECT 2",
			expected: 2,
		},
		{
			name:     "multiple semicolons between statements",
			sql:      "SELECT 1;; SELECT 2;;; SELECT 3",
			expected: 3,
		},
		{
			name:     "newlines between statements",
			sql:      "SELECT 1;\nSELECT 2;\nSELECT 3;",
			expected: 3,
		},
		{
			name:     "single statement",
			sql:      "SELECT 1;",
			expected: 1,
		},
		{
			name:     "complex multi-statement",
			sql:      "SELECT a, b FROM t1 WHERE x > 10; INSERT INTO t2 VALUES (1, 'hello'); SELECT * FROM t3 ORDER BY id;",
			expected: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stmts, err := parser.Parse(ctx, strings.NewReader(tc.sql))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(stmts) != tc.expected {
				t.Errorf("Expected %d statements, got %d", tc.expected, len(stmts))
			}
		})
	}
}

func TestParseString(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT 1; SELECT 2; SELECT 3;"

	stmts, err := parser.ParseString(ctx, sql)
	if err != nil {
		t.Fatalf("ParseString error: %v", err)
	}
	if len(stmts) != 3 {
		t.Errorf("Expected 3 statements, got %d", len(stmts))
	}
}

func TestParseFile(t *testing.T) {
	// Create a temporary SQL file with multiple statements
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "test.sql")

	content := `-- This is a SQL file with multiple statements
SELECT 1;

-- A more complex query
SELECT a, b, c
FROM my_table
WHERE x > 10;

-- Create a table
CREATE TABLE test_table (
    id UInt32,
    name String
);

-- Insert some data
INSERT INTO test_table VALUES (1, 'hello');

-- Final select
SELECT * FROM test_table ORDER BY id;
`
	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	ctx := context.Background()
	stmts, err := parser.ParseFile(ctx, sqlFile)
	if err != nil {
		t.Fatalf("ParseFile error: %v", err)
	}
	if len(stmts) != 5 {
		t.Errorf("Expected 5 statements, got %d", len(stmts))
	}
}

func TestParseFileNotFound(t *testing.T) {
	ctx := context.Background()
	_, err := parser.ParseFile(ctx, "/nonexistent/file.sql")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}
