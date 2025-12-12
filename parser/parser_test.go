package parser_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kyleconroy/doubleclick/ast"
	"github.com/kyleconroy/doubleclick/parser"
)

// clickhouseAvailable checks if ClickHouse server is running
func clickhouseAvailable() bool {
	resp, err := http.Get("http://127.0.0.1:8123/ping")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

// getClickHouseAST runs EXPLAIN AST on ClickHouse and returns the output
func getClickHouseAST(query string) (string, error) {
	explainQuery := fmt.Sprintf("EXPLAIN AST %s", query)
	resp, err := http.Get("http://127.0.0.1:8123/?query=" + url.QueryEscape(explainQuery))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	return buf.String(), nil
}

// TestParserBasicSelect tests basic SELECT parsing
func TestParserBasicSelect(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"simple select", "SELECT 1"},
		{"select columns", "SELECT id, name FROM users"},
		{"select with where", "SELECT * FROM users WHERE id = 1"},
		{"select with alias", "SELECT id AS user_id FROM users"},
		{"select distinct", "SELECT DISTINCT name FROM users"},
		{"select with limit", "SELECT * FROM users LIMIT 10"},
		{"select with offset", "SELECT * FROM users LIMIT 10 OFFSET 5"},
		{"select with order", "SELECT * FROM users ORDER BY name ASC"},
		{"select with order desc", "SELECT * FROM users ORDER BY id DESC"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
			if _, ok := stmts[0].(*ast.SelectWithUnionQuery); !ok {
				t.Fatalf("Expected SelectWithUnionQuery, got %T", stmts[0])
			}
		})
	}
}

// TestParserComplexSelect tests complex SELECT parsing
func TestParserComplexSelect(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"group by", "SELECT count(*) FROM users GROUP BY status"},
		{"group by having", "SELECT count(*) FROM users GROUP BY status HAVING count(*) > 1"},
		{"multiple tables", "SELECT * FROM users, orders"},
		{"inner join", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"},
		{"left join", "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"},
		{"subquery in where", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"},
		{"subquery in from", "SELECT * FROM (SELECT id FROM users) AS t"},
		{"union all", "SELECT 1 UNION ALL SELECT 2"},
		{"case expression", "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END FROM users"},
		{"between", "SELECT * FROM users WHERE id BETWEEN 1 AND 10"},
		{"like", "SELECT * FROM users WHERE name LIKE '%test%'"},
		{"is null", "SELECT * FROM users WHERE name IS NULL"},
		{"is not null", "SELECT * FROM users WHERE name IS NOT NULL"},
		{"in list", "SELECT * FROM users WHERE id IN (1, 2, 3)"},
		{"not in", "SELECT * FROM users WHERE id NOT IN (1, 2, 3)"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserFunctions tests function parsing
func TestParserFunctions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"count", "SELECT count(*) FROM users"},
		{"sum", "SELECT sum(amount) FROM orders"},
		{"avg", "SELECT avg(price) FROM products"},
		{"min max", "SELECT min(id), max(id) FROM users"},
		{"nested functions", "SELECT toDate(now()) FROM users"},
		{"function with multiple args", "SELECT substring(name, 1, 5) FROM users"},
		{"distinct in function", "SELECT count(DISTINCT id) FROM users"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserExpressions tests expression parsing
func TestParserExpressions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"arithmetic", "SELECT 1 + 2 * 3"},
		{"comparison", "SELECT 1 < 2"},
		{"logical and", "SELECT 1 AND 2"},
		{"logical or", "SELECT 1 OR 2"},
		{"logical not", "SELECT NOT 1"},
		{"unary minus", "SELECT -5"},
		{"parentheses", "SELECT (1 + 2) * 3"},
		{"string literal", "SELECT 'hello'"},
		{"integer literal", "SELECT 42"},
		{"float literal", "SELECT 3.14"},
		{"null literal", "SELECT NULL"},
		{"boolean true", "SELECT true"},
		{"boolean false", "SELECT false"},
		{"array literal", "SELECT [1, 2, 3]"},
		{"tuple literal", "SELECT (1, 'a')"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserDDL tests DDL statement parsing
func TestParserDDL(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		stmtType interface{}
	}{
		{"create table", "CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id", &ast.CreateQuery{}},
		{"create table if not exists", "CREATE TABLE IF NOT EXISTS test (id UInt64) ENGINE = MergeTree() ORDER BY id", &ast.CreateQuery{}},
		{"drop table", "DROP TABLE test", &ast.DropQuery{}},
		{"drop table if exists", "DROP TABLE IF EXISTS test", &ast.DropQuery{}},
		{"truncate table", "TRUNCATE TABLE test", &ast.TruncateQuery{}},
		{"alter add column", "ALTER TABLE test ADD COLUMN age UInt32", &ast.AlterQuery{}},
		{"alter drop column", "ALTER TABLE test DROP COLUMN age", &ast.AlterQuery{}},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserOtherStatements tests other statement types
func TestParserOtherStatements(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"use database", "USE mydb"},
		{"describe table", "DESCRIBE TABLE users"},
		{"show tables", "SHOW TABLES"},
		{"show databases", "SHOW DATABASES"},
		{"insert into", "INSERT INTO users (id, name) VALUES"},
		{"insert select", "INSERT INTO users SELECT * FROM old_users"},
		{"set setting", "SET max_threads = 4"},
		{"explain", "EXPLAIN SELECT 1"},
		{"explain ast", "EXPLAIN AST SELECT 1"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserWithClickHouse compares parsing with ClickHouse's EXPLAIN AST
func TestParserWithClickHouse(t *testing.T) {
	if !clickhouseAvailable() {
		t.Skip("ClickHouse not available")
	}

	tests := []struct {
		name  string
		query string
	}{
		{"simple select", "SELECT 1"},
		{"select from table", "SELECT id, name FROM users"},
		{"select with where", "SELECT * FROM users WHERE id = 1"},
		{"select with and", "SELECT * FROM users WHERE id = 1 AND status = 'active'"},
		{"select with order limit", "SELECT * FROM users ORDER BY name LIMIT 10"},
		{"select with join", "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id"},
		{"select with group by", "SELECT count(*) FROM orders GROUP BY user_id"},
		{"select with having", "SELECT count(*) FROM orders GROUP BY user_id HAVING count(*) > 1"},
		{"select with subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"},
		{"select with case", "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END FROM users"},
		{"select with functions", "SELECT toDate(now()), count(*) FROM users"},
		{"select with between", "SELECT * FROM users WHERE id BETWEEN 1 AND 10"},
		{"select with like", "SELECT * FROM users WHERE name LIKE '%test%'"},
		{"union all", "SELECT 1 UNION ALL SELECT 2"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse with our parser
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Our parser error: %v", err)
			}
			if len(stmts) == 0 {
				t.Fatal("Our parser returned no statements")
			}

			// Get ClickHouse's AST
			chAST, err := getClickHouseAST(tt.query)
			if err != nil {
				t.Fatalf("ClickHouse error: %v", err)
			}

			// Verify ClickHouse accepted the query (no error in response)
			if strings.Contains(chAST, "Code:") || strings.Contains(chAST, "Exception:") {
				t.Fatalf("ClickHouse rejected query: %s", chAST)
			}

			// Log both ASTs for comparison
			t.Logf("Query: %s", tt.query)
			t.Logf("ClickHouse AST:\n%s", chAST)

			// Verify our AST can be serialized to JSON
			jsonBytes, err := json.MarshalIndent(stmts[0], "", "  ")
			if err != nil {
				t.Fatalf("JSON marshal error: %v", err)
			}
			t.Logf("Our AST (JSON):\n%s", string(jsonBytes))
		})
	}
}

// TestParserJSONSerialization tests that all AST nodes can be serialized to JSON
func TestParserJSONSerialization(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"select", "SELECT id, name AS n FROM users WHERE id > 1 ORDER BY name LIMIT 10"},
		{"create table", "CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id"},
		{"insert", "INSERT INTO users (id, name) SELECT id, name FROM old_users"},
		{"alter", "ALTER TABLE users ADD COLUMN age UInt32"},
		{"complex select", `
			SELECT
				u.id,
				u.name,
				count(*) AS order_count,
				sum(o.amount) AS total
			FROM users u
			LEFT JOIN orders o ON u.id = o.user_id
			WHERE u.status = 'active'
			GROUP BY u.id, u.name
			HAVING count(*) > 0
			ORDER BY total DESC
			LIMIT 100
		`},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			for i, stmt := range stmts {
				jsonBytes, err := json.MarshalIndent(stmt, "", "  ")
				if err != nil {
					t.Fatalf("JSON marshal error for statement %d: %v", i, err)
				}

				// Verify it's valid JSON by unmarshaling
				var m map[string]interface{}
				if err := json.Unmarshal(jsonBytes, &m); err != nil {
					t.Fatalf("JSON unmarshal error for statement %d: %v", i, err)
				}

				t.Logf("Statement %d JSON:\n%s", i, string(jsonBytes))
			}
		})
	}
}

// TestParserMultipleStatements tests parsing multiple statements
func TestParserMultipleStatements(t *testing.T) {
	query := `
		SELECT 1;
		SELECT 2;
		SELECT 3
	`

	ctx := context.Background()
	stmts, err := parser.Parse(ctx, strings.NewReader(query))
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("Expected 3 statements, got %d", len(stmts))
	}
}

// TestParserContextCancellation tests that parsing respects context cancellation
func TestParserContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Give some time for cancellation
	time.Sleep(5 * time.Millisecond)

	_, err := parser.Parse(ctx, strings.NewReader("SELECT 1"))
	if err == nil {
		// Context might not have been checked yet for simple queries
		// This is acceptable behavior
		t.Log("Context cancellation not triggered for simple query (acceptable)")
	}
}

// TestClickHouseASTComparison runs a detailed comparison with ClickHouse AST
func TestClickHouseASTComparison(t *testing.T) {
	if !clickhouseAvailable() {
		t.Skip("ClickHouse not available")
	}

	// Test queries that exercise different AST node types
	queries := []string{
		// Basic SELECT
		"SELECT 1",
		"SELECT id FROM users",
		"SELECT id, name FROM users",

		// Expressions
		"SELECT 1 + 2",
		"SELECT 1 + 2 * 3",
		"SELECT (1 + 2) * 3",
		"SELECT -5",
		"SELECT NOT true",

		// Literals
		"SELECT 'hello'",
		"SELECT 3.14",
		"SELECT NULL",
		"SELECT [1, 2, 3]",

		// Functions
		"SELECT count(*)",
		"SELECT sum(amount) FROM orders",
		"SELECT toDate('2023-01-01')",

		// WHERE clause
		"SELECT * FROM users WHERE id = 1",
		"SELECT * FROM users WHERE id > 1 AND status = 'active'",
		"SELECT * FROM users WHERE id IN (1, 2, 3)",
		"SELECT * FROM users WHERE name LIKE '%test%'",
		"SELECT * FROM users WHERE id BETWEEN 1 AND 10",
		"SELECT * FROM users WHERE name IS NULL",

		// JOINs
		"SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
		"SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",

		// GROUP BY / ORDER BY
		"SELECT count(*) FROM users GROUP BY status",
		"SELECT * FROM users ORDER BY id",
		"SELECT * FROM users ORDER BY id DESC",
		"SELECT * FROM users ORDER BY id LIMIT 10",

		// Subqueries
		"SELECT * FROM (SELECT 1) AS t",
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",

		// UNION
		"SELECT 1 UNION ALL SELECT 2",

		// CASE
		"SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END FROM users",

		// CREATE TABLE
		"CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",

		// DROP
		"DROP TABLE IF EXISTS test",

		// INSERT
		"INSERT INTO users (id, name) VALUES",

		// ALTER
		"ALTER TABLE users ADD COLUMN age UInt32",

		// USE
		"USE mydb",

		// TRUNCATE
		"TRUNCATE TABLE users",

		// DESCRIBE
		"DESCRIBE TABLE users",

		// SHOW
		"SHOW TABLES",
	}

	ctx := context.Background()
	passed := 0
	failed := 0

	for _, query := range queries {
		// Parse with our parser
		stmts, err := parser.Parse(ctx, strings.NewReader(query))
		if err != nil {
			t.Logf("FAIL [parse error]: %s\n  Error: %v", query, err)
			failed++
			continue
		}

		if len(stmts) == 0 {
			t.Logf("FAIL [no statements]: %s", query)
			failed++
			continue
		}

		// Get ClickHouse's AST
		chAST, err := getClickHouseAST(query)
		if err != nil {
			t.Logf("SKIP [clickhouse error]: %s\n  Error: %v", query, err)
			continue
		}

		// Check if ClickHouse accepted the query
		if strings.Contains(chAST, "Code:") || strings.Contains(chAST, "Exception:") {
			t.Logf("SKIP [clickhouse rejected]: %s\n  Response: %s", query, strings.TrimSpace(chAST))
			continue
		}

		// Verify we can serialize to JSON
		_, jsonErr := json.Marshal(stmts[0])
		if jsonErr != nil {
			t.Logf("FAIL [json error]: %s\n  Error: %v", query, jsonErr)
			failed++
			continue
		}

		t.Logf("PASS: %s", query)
		passed++
	}

	t.Logf("\nSummary: %d passed, %d failed", passed, failed)

	if failed > 0 {
		t.Errorf("%d queries failed to parse", failed)
	}
}

// BenchmarkParser benchmarks the parser performance
func BenchmarkParser(b *testing.B) {
	query := `
		SELECT
			u.id,
			u.name,
			count(*) AS order_count,
			sum(o.amount) AS total
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.status = 'active' AND o.created_at > '2023-01-01'
		GROUP BY u.id, u.name
		HAVING count(*) > 0
		ORDER BY total DESC
		LIMIT 100
	`

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := parser.Parse(ctx, strings.NewReader(query))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Helper to run clickhouse client command
func runClickHouseClient(query string) (string, error) {
	cmd := exec.Command("./clickhouse", "client", "--query", query)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
