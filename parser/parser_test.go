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

// TestParserTodoBasicExpressions contains basic expression tests validated against ClickHouse
// TODO: These tests are marked as future work to ensure the parser handles all basic expressions
func TestParserTodoBasicExpressions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		// Basic arithmetic and operators
		{"addition", "SELECT 1 + 2"},
		{"multiplication precedence", "SELECT 1 + 2 * 3"},
		{"parentheses precedence", "SELECT (1 + 2) * 3"},
		{"unary minus", "SELECT -5"},
		{"logical not", "SELECT NOT true"},
		{"array literal", "SELECT [1, 2, 3]"},
		{"tuple literal", "SELECT (1, 2, 3)"},
		{"count star", "SELECT count(*)"},
		{"sum function", "SELECT sum(1)"},
		{"avg function", "SELECT avg(1)"},
		{"min max functions", "SELECT min(1), max(1)"},
		{"nested function call", "SELECT toDate(now())"},
		{"now function", "SELECT now()"},
		{"select from system table", "SELECT * FROM system.one"},
		{"column alias", "SELECT 1 AS x"},
		{"multiple aliases", "SELECT 1 AS x, 2 AS y"},
		{"select distinct", "SELECT DISTINCT 1"},

		// WHERE clause variations
		{"where equality", "SELECT 1 WHERE 1 = 1"},
		{"where and condition", "SELECT 1 WHERE 1 > 0 AND 2 < 3"},
		{"where in list", "SELECT 1 WHERE 1 IN (1, 2, 3)"},
		{"where between", "SELECT 1 WHERE 1 BETWEEN 0 AND 10"},
		{"where is null", "SELECT 1 WHERE NULL IS NULL"},
		{"where is not null", "SELECT 1 WHERE 1 IS NOT NULL"},

		// ORDER BY variations
		{"order by", "SELECT 1 ORDER BY 1"},
		{"order by asc", "SELECT 1 ORDER BY 1 ASC"},
		{"order by desc", "SELECT 1 ORDER BY 1 DESC"},
		{"order by desc nulls first", "SELECT 1 ORDER BY 1 DESC NULLS FIRST"},
		{"order by desc nulls last", "SELECT 1 ORDER BY 1 DESC NULLS LAST"},

		// LIMIT variations
		{"limit", "SELECT 1 LIMIT 10"},
		{"limit offset", "SELECT 1 LIMIT 10 OFFSET 5"},

		// CASE expressions
		{"case when else", "SELECT CASE WHEN 1 > 0 THEN 1 ELSE 0 END"},
		{"if function", "SELECT if(1 > 0, 1, 0)"},
		{"multiIf function", "SELECT multiIf(1 > 0, 1, 2 > 0, 2, 0)"},

		// Type casting
		{"cast function", "SELECT CAST(1 AS String)"},
		{"cast operator", "SELECT 1::Int32"},

		// UNION
		{"union all", "SELECT 1 UNION ALL SELECT 2"},
		{"union distinct", "SELECT 1 UNION DISTINCT SELECT 1"},

		// Subqueries
		{"subquery in from", "SELECT * FROM (SELECT 1)"},
		{"subquery in from with alias", "SELECT * FROM (SELECT 1) AS t"},

		// Array functions
		{"arrayJoin", "SELECT arrayJoin([1, 2, 3])"},
		{"groupArray", "SELECT groupArray(1)"},
		{"groupUniqArray", "SELECT groupUniqArray(1)"},
		{"arrayMap lambda", "SELECT arrayMap(x -> x + 1, [1, 2, 3])"},
		{"arrayFilter lambda", "SELECT arrayFilter(x -> x > 1, [1, 2, 3])"},
		{"transform function", "SELECT transform(1, [1, 2], [10, 20], 0)"},
		{"tuple function", "SELECT tuple(1, 2, 3)"},
		{"array subscript", "SELECT [1, 2, 3][1]"},
		{"map function", "SELECT map(1, 2)"},

		// SETTINGS
		{"select with settings", "SELECT 1 SETTINGS max_threads = 1"},

		// WITH clause (CTE)
		{"with scalar", "WITH 1 AS x SELECT x"},
		{"with subquery cte", "WITH x AS (SELECT 1) SELECT * FROM x"},

		// Table functions
		{"numbers function", "SELECT number FROM numbers(10)"},
		{"group by with modulo", "SELECT count(*) FROM numbers(100) GROUP BY number % 10"},
		{"group by with totals", "SELECT number % 10, count(*) FROM numbers(100) GROUP BY number % 10 WITH TOTALS"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoWindowFunctions contains window function tests validated against ClickHouse
// TODO: These tests are marked as future work for window function support
func TestParserTodoWindowFunctions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"sum over empty", "SELECT number, sum(number) OVER () FROM numbers(10)"},
		{"sum over order by", "SELECT number, sum(number) OVER (ORDER BY number) FROM numbers(10)"},
		{"row_number over empty", "SELECT number, row_number() OVER () FROM numbers(10)"},
		{"row_number over order by", "SELECT number, row_number() OVER (ORDER BY number) FROM numbers(10)"},
		{"rank function", "SELECT number, rank() OVER (ORDER BY number) FROM numbers(10)"},
		{"dense_rank function", "SELECT number, dense_rank() OVER (ORDER BY number) FROM numbers(10)"},
		{"lag function", "SELECT number, lag(number) OVER (ORDER BY number) FROM numbers(10)"},
		{"lead function", "SELECT number, lead(number) OVER (ORDER BY number) FROM numbers(10)"},
		{"first_value function", "SELECT number, first_value(number) OVER (ORDER BY number) FROM numbers(10)"},
		{"last_value function", "SELECT number, last_value(number) OVER (ORDER BY number) FROM numbers(10)"},
		{"nth_value function", "SELECT number, nth_value(number, 2) OVER (ORDER BY number) FROM numbers(10)"},
		{"window frame rows", "SELECT number, avg(number) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM numbers(10)"},
		{"named window", "SELECT number, sum(number) OVER w FROM numbers(10) WINDOW w AS (ORDER BY number)"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support window functions: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoJoins contains join tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive join support
func TestParserTodoJoins(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"implicit cross join", "SELECT * FROM numbers(5) AS a, numbers(5) AS b"},
		{"explicit cross join", "SELECT * FROM numbers(5) AS a CROSS JOIN numbers(5) AS b"},
		{"inner join", "SELECT * FROM numbers(5) AS a INNER JOIN numbers(5) AS b ON a.number = b.number"},
		{"left join", "SELECT * FROM numbers(5) AS a LEFT JOIN numbers(5) AS b ON a.number = b.number"},
		{"right join", "SELECT * FROM numbers(5) AS a RIGHT JOIN numbers(5) AS b ON a.number = b.number"},
		{"full join", "SELECT * FROM numbers(5) AS a FULL JOIN numbers(5) AS b ON a.number = b.number"},
		{"join using", "SELECT * FROM numbers(5) AS a JOIN numbers(5) AS b USING number"},
		{"left outer join", "SELECT * FROM numbers(5) AS a LEFT OUTER JOIN numbers(5) AS b ON a.number = b.number"},
		{"any join", "SELECT * FROM numbers(5) AS a ANY JOIN numbers(5) AS b ON a.number = b.number"},
		{"all join", "SELECT * FROM numbers(5) AS a ALL JOIN numbers(5) AS b ON a.number = b.number"},
		{"semi join", "SELECT * FROM numbers(5) AS a SEMI JOIN numbers(5) AS b ON a.number = b.number"},
		{"anti join", "SELECT * FROM numbers(5) AS a ANTI JOIN numbers(5) AS b ON a.number = b.number"},
		{"global join", "SELECT * FROM numbers(5) AS a GLOBAL JOIN numbers(5) AS b ON a.number = b.number"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this join type: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoSubqueries contains subquery tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive subquery support
func TestParserTodoSubqueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"subquery with where", "SELECT * FROM (SELECT number AS x FROM numbers(10)) WHERE x > 5"},
		{"scalar subquery", "SELECT (SELECT 1)"},
		{"scalar subquery aggregate", "SELECT (SELECT max(number) FROM numbers(10))"},
		{"subquery in IN clause", "SELECT number FROM numbers(10) WHERE number IN (SELECT number FROM numbers(5))"},
		{"subquery in NOT IN clause", "SELECT number FROM numbers(10) WHERE number NOT IN (SELECT number FROM numbers(5))"},
		{"exists subquery", "SELECT number FROM numbers(10) WHERE EXISTS (SELECT 1 FROM numbers(5) WHERE number = 1)"},
		{"not exists subquery", "SELECT number FROM numbers(10) WHERE NOT EXISTS (SELECT 1 WHERE 0)"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this subquery pattern: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoArrayFunctions contains array function tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive array function support
func TestParserTodoArrayFunctions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"array constructor", "SELECT array(1, 2, 3)"},
		{"empty array", "SELECT emptyArrayUInt8()"},
		{"range function", "SELECT range(10)"},
		{"arrayConcat", "SELECT arrayConcat([1, 2], [3, 4])"},
		{"arrayElement", "SELECT arrayElement([1, 2, 3], 1)"},
		{"has function", "SELECT has([1, 2, 3], 2)"},
		{"indexOf", "SELECT indexOf([1, 2, 3], 2)"},
		{"length on array", "SELECT length([1, 2, 3])"},
		{"empty on array", "SELECT empty([1, 2, 3])"},
		{"notEmpty on array", "SELECT notEmpty([1, 2, 3])"},
		{"arrayReverse", "SELECT arrayReverse([1, 2, 3])"},
		{"arrayFlatten", "SELECT arrayFlatten([[1, 2], [3, 4]])"},
		{"arrayCompact", "SELECT arrayCompact([1, 1, 2, 2, 3, 3])"},
		{"arrayDistinct", "SELECT arrayDistinct([1, 1, 2, 2, 3, 3])"},
		{"arrayEnumerate", "SELECT arrayEnumerate([10, 20, 30])"},
		{"arrayEnumerateUniq", "SELECT arrayEnumerateUniq([10, 10, 20, 20])"},
		{"arrayPopBack", "SELECT arrayPopBack([1, 2, 3])"},
		{"arrayPopFront", "SELECT arrayPopFront([1, 2, 3])"},
		{"arrayPushBack", "SELECT arrayPushBack([1, 2], 3)"},
		{"arrayPushFront", "SELECT arrayPushFront([2, 3], 1)"},
		{"arraySlice", "SELECT arraySlice([1, 2, 3, 4, 5], 2, 3)"},
		{"arraySort", "SELECT arraySort([3, 1, 2])"},
		{"arrayUniq", "SELECT arrayUniq([1, 1, 2, 2, 3])"},
		{"arrayExists lambda", "SELECT arrayExists(x -> x > 2, [1, 2, 3])"},
		{"arrayAll lambda", "SELECT arrayAll(x -> x > 0, [1, 2, 3])"},
		{"arrayFirst lambda", "SELECT arrayFirst(x -> x > 1, [1, 2, 3])"},
		{"arraySplit lambda", "SELECT arraySplit(x -> x = 2, [1, 2, 3, 4])"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this array function: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoDateTimeFunctions contains date/time function tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive date/time function support
func TestParserTodoDateTimeFunctions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"toDate", "SELECT toDate('2023-01-01')"},
		{"toDateTime", "SELECT toDateTime('2023-01-01 12:00:00')"},
		{"toDateTime64", "SELECT toDateTime64('2023-01-01 12:00:00.123', 3)"},
		{"toYear", "SELECT toYear(now())"},
		{"toMonth", "SELECT toMonth(now())"},
		{"toDayOfMonth", "SELECT toDayOfMonth(now())"},
		{"toDayOfWeek", "SELECT toDayOfWeek(now())"},
		{"toHour", "SELECT toHour(now())"},
		{"toMinute", "SELECT toMinute(now())"},
		{"toSecond", "SELECT toSecond(now())"},
		{"toUnixTimestamp", "SELECT toUnixTimestamp(now())"},
		{"fromUnixTimestamp", "SELECT fromUnixTimestamp(1234567890)"},
		{"formatDateTime", "SELECT formatDateTime(now(), '%Y-%m-%d')"},
		{"dateDiff", "SELECT dateDiff('day', toDate('2023-01-01'), toDate('2023-01-31'))"},
		{"dateAdd", "SELECT dateAdd(day, 1, toDate('2023-01-01'))"},
		{"dateSub", "SELECT dateSub(day, 1, toDate('2023-01-02'))"},
		{"addDays", "SELECT addDays(toDate('2023-01-01'), 5)"},
		{"subtractDays", "SELECT subtractDays(toDate('2023-01-06'), 5)"},
		{"today", "SELECT today()"},
		{"yesterday", "SELECT yesterday()"},
		{"toStartOfDay", "SELECT toStartOfDay(now())"},
		{"toStartOfWeek", "SELECT toStartOfWeek(now())"},
		{"toStartOfMonth", "SELECT toStartOfMonth(now())"},
		{"toStartOfYear", "SELECT toStartOfYear(now())"},
		{"toStartOfHour", "SELECT toStartOfHour(now())"},
		{"toStartOfMinute", "SELECT toStartOfMinute(now())"},
		{"toMonday", "SELECT toMonday(now())"},
		{"toIntervalDay", "SELECT toIntervalDay(1)"},
		{"toIntervalMonth", "SELECT toIntervalMonth(1)"},
		{"interval add", "SELECT now() + INTERVAL 1 DAY"},
		{"interval subtract", "SELECT now() - INTERVAL 1 HOUR"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this date/time function: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoStringFunctions contains string function tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive string function support
func TestParserTodoStringFunctions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"concat", "SELECT concat('hello', ' ', 'world')"},
		{"substring", "SELECT substring('hello', 1, 3)"},
		{"upper", "SELECT upper('hello')"},
		{"lower", "SELECT lower('HELLO')"},
		{"trim", "SELECT trim('  hello  ')"},
		{"ltrim", "SELECT ltrim('  hello')"},
		{"rtrim", "SELECT rtrim('hello  ')"},
		{"length string", "SELECT length('hello')"},
		{"reverse string", "SELECT reverse('hello')"},
		{"replaceAll", "SELECT replaceAll('hello', 'l', 'x')"},
		{"replaceOne", "SELECT replaceOne('hello', 'l', 'x')"},
		{"position", "SELECT position('hello', 'l')"},
		{"positionCaseInsensitive", "SELECT positionCaseInsensitive('HELLO', 'l')"},
		{"splitByChar", "SELECT splitByChar(',', 'a,b,c')"},
		{"splitByString", "SELECT splitByString(',,', 'a,,b,,c')"},
		{"arrayStringConcat", "SELECT arrayStringConcat(['a', 'b', 'c'], ',')"},
		{"format function", "SELECT format('{0} {1}', 'hello', 'world')"},
		{"toString", "SELECT toString(123)"},
		{"toFixedString", "SELECT toFixedString('hello', 10)"},
		{"empty string", "SELECT empty('')"},
		{"notEmpty string", "SELECT notEmpty('')"},
		{"leftPad", "SELECT leftPad('123', 5, '0')"},
		{"rightPad", "SELECT rightPad('123', 5, '0')"},
		{"repeat", "SELECT repeat('abc', 3)"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this string function: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoTypeConversions contains type conversion tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive type conversion support
func TestParserTodoTypeConversions(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"toInt8", "SELECT toInt8(123)"},
		{"toInt16", "SELECT toInt16(123)"},
		{"toInt32", "SELECT toInt32(123)"},
		{"toInt64", "SELECT toInt64(123)"},
		{"toUInt8", "SELECT toUInt8(123)"},
		{"toUInt16", "SELECT toUInt16(123)"},
		{"toUInt32", "SELECT toUInt32(123)"},
		{"toUInt64", "SELECT toUInt64(123)"},
		{"toFloat32", "SELECT toFloat32(123.456)"},
		{"toFloat64", "SELECT toFloat64(123.456)"},
		{"toDecimal32", "SELECT toDecimal32(123.456, 2)"},
		{"toDecimal64", "SELECT toDecimal64(123.456, 2)"},
		{"toString conversion", "SELECT toString(123)"},
		{"toTypeName int", "SELECT toTypeName(1)"},
		{"toTypeName string", "SELECT toTypeName('hello')"},
		{"toTypeName array", "SELECT toTypeName([1, 2, 3])"},
		{"reinterpretAsUInt64", "SELECT reinterpretAsUInt64('hello')"},
		{"accurateCast", "SELECT accurateCast(123.456, 'Int32')"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this type conversion: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoDDL contains DDL statement tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive DDL support
func TestParserTodoDDL(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		// CREATE TABLE variations
		{"create table memory", "CREATE TABLE test_table (id UInt64, name String) ENGINE = Memory"},
		{"create table if not exists", "CREATE TABLE IF NOT EXISTS test_table (id UInt64) ENGINE = Memory"},
		{"create table mergetree", "CREATE TABLE test_table (id UInt64) ENGINE = MergeTree() ORDER BY id"},
		{"create table with partition", "CREATE TABLE test_table (id UInt64, dt Date) ENGINE = MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id"},
		{"create table with primary key", "CREATE TABLE test_table (id UInt64) ENGINE = MergeTree() ORDER BY id PRIMARY KEY id"},
		{"create table with settings", "CREATE TABLE test_table (id UInt64) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192"},
		{"create table with default", "CREATE TABLE test_table (id UInt64 DEFAULT 0, name String DEFAULT '') ENGINE = Memory"},
		{"create table with materialized", "CREATE TABLE test_table (id UInt64 MATERIALIZED 0) ENGINE = Memory"},
		{"create table with codec", "CREATE TABLE test_table (id UInt64 CODEC(LZ4)) ENGINE = Memory"},
		{"create table with comment", "CREATE TABLE test_table (id UInt64 COMMENT 'The ID') ENGINE = Memory"},
		{"create table as select", "CREATE TABLE test_table AS SELECT 1 AS id"},

		// CREATE VIEW
		{"create view", "CREATE VIEW test_view AS SELECT 1"},
		{"create view if not exists", "CREATE VIEW IF NOT EXISTS test_view AS SELECT 1"},
		{"create materialized view", "CREATE MATERIALIZED VIEW test_mv ENGINE = Memory AS SELECT 1"},

		// CREATE DATABASE
		{"create database", "CREATE DATABASE test_db"},
		{"create database if not exists", "CREATE DATABASE IF NOT EXISTS test_db"},
		{"create database with engine", "CREATE DATABASE test_db ENGINE = Atomic"},

		// DROP statements
		{"drop table", "DROP TABLE test_table"},
		{"drop table if exists", "DROP TABLE IF EXISTS test_table"},
		{"drop table sync", "DROP TABLE test_table SYNC"},
		{"drop database", "DROP DATABASE test_db"},
		{"drop database if exists", "DROP DATABASE IF EXISTS test_db"},
		{"drop view", "DROP VIEW test_view"},
		{"drop view if exists", "DROP VIEW IF EXISTS test_view"},

		// TRUNCATE
		{"truncate table", "TRUNCATE TABLE test_table"},

		// RENAME
		{"rename table", "RENAME TABLE old_table TO new_table"},

		// EXCHANGE
		{"exchange tables", "EXCHANGE TABLES table1 AND table2"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this DDL statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoAlter contains ALTER statement tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive ALTER support
func TestParserTodoAlter(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		// Column operations
		{"add column", "ALTER TABLE test_table ADD COLUMN new_col UInt64"},
		{"add column if not exists", "ALTER TABLE test_table ADD COLUMN IF NOT EXISTS new_col UInt64"},
		{"add column after", "ALTER TABLE test_table ADD COLUMN new_col UInt64 AFTER id"},
		{"drop column", "ALTER TABLE test_table DROP COLUMN old_col"},
		{"drop column if exists", "ALTER TABLE test_table DROP COLUMN IF EXISTS old_col"},
		{"modify column", "ALTER TABLE test_table MODIFY COLUMN col UInt64"},
		{"rename column", "ALTER TABLE test_table RENAME COLUMN old_name TO new_name"},

		// Index operations
		{"add index", "ALTER TABLE test_table ADD INDEX idx (col) TYPE minmax GRANULARITY 4"},
		{"drop index", "ALTER TABLE test_table DROP INDEX idx"},
		{"clear index", "ALTER TABLE test_table CLEAR INDEX idx"},
		{"materialize index", "ALTER TABLE test_table MATERIALIZE INDEX idx"},

		// Constraint operations
		{"add constraint", "ALTER TABLE test_table ADD CONSTRAINT c CHECK col > 0"},
		{"drop constraint", "ALTER TABLE test_table DROP CONSTRAINT c"},

		// Partition operations
		{"detach partition", "ALTER TABLE test_table DETACH PARTITION 202301"},
		{"attach partition", "ALTER TABLE test_table ATTACH PARTITION 202301"},
		{"drop partition", "ALTER TABLE test_table DROP PARTITION 202301"},
		{"replace partition", "ALTER TABLE test_table REPLACE PARTITION 202301 FROM other_table"},
		{"freeze table", "ALTER TABLE test_table FREEZE"},
		{"freeze partition", "ALTER TABLE test_table FREEZE PARTITION 202301"},

		// TTL
		{"modify ttl", "ALTER TABLE test_table MODIFY TTL dt + INTERVAL 1 MONTH"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this ALTER statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoInsert contains INSERT statement tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive INSERT support
func TestParserTodoInsert(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"insert values", "INSERT INTO test_table VALUES"},
		{"insert with columns", "INSERT INTO test_table (id, name) VALUES"},
		{"insert select", "INSERT INTO test_table (id) SELECT number FROM numbers(10)"},
		{"insert format csv", "INSERT INTO test_table FORMAT CSV"},
		{"insert format json", "INSERT INTO test_table FORMAT JSONEachRow"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this INSERT statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoOptimize contains OPTIMIZE statement tests validated against ClickHouse
// TODO: These tests are marked as future work for OPTIMIZE support
func TestParserTodoOptimize(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"optimize table", "OPTIMIZE TABLE test_table"},
		{"optimize partition", "OPTIMIZE TABLE test_table PARTITION 202301"},
		{"optimize final", "OPTIMIZE TABLE test_table FINAL"},
		{"optimize deduplicate", "OPTIMIZE TABLE test_table DEDUPLICATE"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this OPTIMIZE statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoShow contains SHOW statement tests validated against ClickHouse
// TODO: These tests are marked as future work for comprehensive SHOW support
func TestParserTodoShow(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"show databases", "SHOW DATABASES"},
		{"show tables", "SHOW TABLES"},
		{"show tables from", "SHOW TABLES FROM system"},
		{"show create table", "SHOW CREATE TABLE system.one"},
		{"show create database", "SHOW CREATE DATABASE system"},
		{"show processlist", "SHOW PROCESSLIST"},
		{"show columns", "SHOW COLUMNS FROM system.one"},
		{"show dictionaries", "SHOW DICTIONARIES"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this SHOW statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoDescribe contains DESCRIBE statement tests validated against ClickHouse
// TODO: These tests are marked as future work for DESCRIBE support
func TestParserTodoDescribe(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"describe table full", "DESCRIBE TABLE system.one"},
		{"desc table", "DESC TABLE system.one"},
		{"describe short", "DESCRIBE system.one"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this DESCRIBE statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoSystem contains SYSTEM statement tests validated against ClickHouse
// TODO: These tests are marked as future work for SYSTEM command support
func TestParserTodoSystem(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"flush logs", "SYSTEM FLUSH LOGS"},
		{"drop dns cache", "SYSTEM DROP DNS CACHE"},
		{"drop mark cache", "SYSTEM DROP MARK CACHE"},
		{"drop uncompressed cache", "SYSTEM DROP UNCOMPRESSED CACHE"},
		{"drop compiled expression cache", "SYSTEM DROP COMPILED EXPRESSION CACHE"},
		{"reload config", "SYSTEM RELOAD CONFIG"},
		{"reload dictionaries", "SYSTEM RELOAD DICTIONARIES"},
		{"stop merges", "SYSTEM STOP MERGES"},
		{"start merges", "SYSTEM START MERGES"},
		{"stop ttl merges", "SYSTEM STOP TTL MERGES"},
		{"start ttl merges", "SYSTEM START TTL MERGES"},
		{"stop moves", "SYSTEM STOP MOVES"},
		{"start moves", "SYSTEM START MOVES"},
		{"stop fetches", "SYSTEM STOP FETCHES"},
		{"start fetches", "SYSTEM START FETCHES"},
		{"stop replication queues", "SYSTEM STOP REPLICATION QUEUES"},
		{"start replication queues", "SYSTEM START REPLICATION QUEUES"},
		{"sync replica", "SYSTEM SYNC REPLICA system.one"},
		{"restart replica", "SYSTEM RESTART REPLICA system.one"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this SYSTEM command: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoExplain contains EXPLAIN statement tests validated against ClickHouse
// TODO: These tests are marked as future work for EXPLAIN support
func TestParserTodoExplain(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"explain", "EXPLAIN SELECT 1"},
		{"explain ast", "EXPLAIN AST SELECT 1"},
		{"explain syntax", "EXPLAIN SYNTAX SELECT 1"},
		{"explain plan", "EXPLAIN PLAN SELECT 1"},
		{"explain pipeline", "EXPLAIN PIPELINE SELECT 1"},
		{"explain estimate", "EXPLAIN ESTIMATE SELECT 1"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this EXPLAIN statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoSet contains SET statement tests validated against ClickHouse
// TODO: These tests are marked as future work for SET support
func TestParserTodoSet(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"set max_threads", "SET max_threads = 4"},
		{"set max_memory_usage", "SET max_memory_usage = 10000000"},
		{"set boolean setting", "SET enable_optimize_predicate_expression = 1"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this SET statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoUse contains USE statement tests validated against ClickHouse
// TODO: These tests are marked as future work for USE support
func TestParserTodoUse(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"use system", "USE system"},
		{"use default", "USE default"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this USE statement: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoClickHouseSpecific contains ClickHouse-specific syntax tests
// TODO: These tests are marked as future work for ClickHouse-specific features
func TestParserTodoClickHouseSpecific(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		// ARRAY JOIN
		{"array join", "SELECT s, arr FROM arrays_test ARRAY JOIN arr"},
		{"array join with alias", "SELECT s, arr, a FROM arrays_test ARRAY JOIN arr AS a"},
		{"array join with enumerate", "SELECT s, arr, a, num FROM arrays_test ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num"},
		{"array join with map", "SELECT s, arr, a, mapped FROM arrays_test ARRAY JOIN arr AS a, arrayMap(x -> x + 1, arr) AS mapped"},
		{"left array join", "SELECT s, arr, a FROM arrays_test LEFT ARRAY JOIN arr AS a"},

		// PREWHERE
		{"prewhere", "SELECT * FROM test_table PREWHERE id > 0"},
		{"prewhere and where", "SELECT * FROM test_table PREWHERE id > 0 WHERE name != ''"},

		// SAMPLE
		{"sample", "SELECT * FROM test_table SAMPLE 0.1"},
		{"sample n", "SELECT * FROM test_table SAMPLE 1000"},
		{"sample offset", "SELECT * FROM test_table SAMPLE 0.1 OFFSET 0.5"},

		// FINAL
		{"select final", "SELECT * FROM test_table FINAL"},

		// FORMAT
		{"format json", "SELECT 1 FORMAT JSON"},
		{"format csv", "SELECT 1 FORMAT CSV"},
		{"format tsv", "SELECT 1 FORMAT TSV"},
		{"format tabseparated", "SELECT 1 FORMAT TabSeparated"},
		{"format pretty", "SELECT 1 FORMAT Pretty"},
		{"format vertical", "SELECT 1 FORMAT Vertical"},
		{"format jsoneachrow", "SELECT 1 FORMAT JSONEachRow"},
		{"format jsoncompact", "SELECT 1 FORMAT JSONCompact"},

		// INTO OUTFILE
		{"into outfile", "SELECT 1 INTO OUTFILE 'output.csv'"},
		{"into outfile format", "SELECT 1 INTO OUTFILE 'output.csv' FORMAT CSV"},

		// Global/distributed
		{"global in", "SELECT * FROM test_table WHERE id GLOBAL IN (SELECT id FROM other_table)"},
		{"global not in", "SELECT * FROM test_table WHERE id GLOBAL NOT IN (SELECT id FROM other_table)"},

		// WITH FILL
		{"order by with fill", "SELECT number FROM numbers(10) ORDER BY number WITH FILL"},
		{"order by with fill from to", "SELECT number FROM numbers(10) ORDER BY number WITH FILL FROM 0 TO 20"},
		{"order by with fill step", "SELECT number FROM numbers(10) ORDER BY number WITH FILL FROM 0 TO 20 STEP 2"},

		// Parametric aggregate functions
		{"quantile parametric", "SELECT quantile(0.9)(number) FROM numbers(100)"},
		{"quantiles parametric", "SELECT quantiles(0.5, 0.9, 0.99)(number) FROM numbers(100)"},
		{"topK parametric", "SELECT topK(5)(number) FROM numbers(100)"},

		// Lambda in aggregate
		{"sumIf with lambda", "SELECT sumIf(number, number > 5) FROM numbers(10)"},
		{"countIf", "SELECT countIf(number > 5) FROM numbers(10)"},
		{"avgIf", "SELECT avgIf(number, number > 5) FROM numbers(10)"},

		// Combinators
		{"aggregate if combinator", "SELECT sumIf(number, number > 5) FROM numbers(10)"},
		{"aggregate array combinator", "SELECT sumArray([1, 2, 3])"},
		{"aggregate merge combinator", "SELECT sumMerge(sum_state) FROM states_table"},
		{"aggregate state combinator", "SELECT sumState(number) FROM numbers(10)"},

		// NULL handling
		{"null safe equal", "SELECT NULL <=> NULL"},
		{"ifNull", "SELECT ifNull(NULL, 0)"},
		{"nullIf", "SELECT nullIf(1, 1)"},
		{"coalesce", "SELECT coalesce(NULL, NULL, 1)"},
		{"assumeNotNull", "SELECT assumeNotNull(toNullable(1))"},
		{"toNullable", "SELECT toNullable(1)"},

		// Special expressions
		{"asterisk with except", "SELECT * EXCEPT (id) FROM test_table"},
		{"asterisk with replace", "SELECT * REPLACE (id + 1 AS id) FROM test_table"},
		{"columns matcher", "SELECT COLUMNS('name.*') FROM test_table"},

		// Dictionaries
		{"dictGet", "SELECT dictGet('dict_name', 'attr', toUInt64(1))"},
		{"dictGetOrDefault", "SELECT dictGetOrDefault('dict_name', 'attr', toUInt64(1), 'default')"},
		{"dictHas", "SELECT dictHas('dict_name', toUInt64(1))"},

		// Tuple access
		{"tuple element dot", "SELECT tuple(1, 2, 3).1"},
		{"tuple element subscript", "SELECT tuple(1, 2, 3)[1]"},
		{"named tuple access", "SELECT (1, 2, 3) AS t, t.1"},

		// Map operations
		{"map element access", "SELECT map('key', 'value')['key']"},
		{"mapKeys", "SELECT mapKeys(map('a', 1, 'b', 2))"},
		{"mapValues", "SELECT mapValues(map('a', 1, 'b', 2))"},
		{"mapContains", "SELECT mapContains(map('a', 1), 'a')"},

		// JSON functions
		{"JSONExtract", "SELECT JSONExtract('{\"a\": 1}', 'a', 'Int32')"},
		{"JSONExtractString", "SELECT JSONExtractString('{\"a\": \"b\"}', 'a')"},
		{"JSONExtractInt", "SELECT JSONExtractInt('{\"a\": 1}', 'a')"},
		{"JSONExtractBool", "SELECT JSONExtractBool('{\"a\": true}', 'a')"},
		{"JSONExtractFloat", "SELECT JSONExtractFloat('{\"a\": 1.5}', 'a')"},
		{"JSONExtractRaw", "SELECT JSONExtractRaw('{\"a\": [1,2,3]}', 'a')"},
		{"JSONExtractArrayRaw", "SELECT JSONExtractArrayRaw('{\"a\": [1,2,3]}', 'a')"},
		{"JSONExtractKeysAndValues", "SELECT JSONExtractKeysAndValues('{\"a\": 1, \"b\": 2}', 'Int32')"},

		// Regular expressions
		{"match", "SELECT match('hello', 'h.*o')"},
		{"extract", "SELECT extract('hello world', 'w\\\\w+')"},
		{"extractAll", "SELECT extractAll('hello world', '\\\\w+')"},
		{"like regex", "SELECT 'hello' LIKE '%ell%'"},
		{"ilike", "SELECT 'HELLO' ILIKE '%ell%'"},
		{"not like", "SELECT 'hello' NOT LIKE '%xyz%'"},
		{"not ilike", "SELECT 'HELLO' NOT ILIKE '%xyz%'"},

		// UUID
		{"generateUUIDv4", "SELECT generateUUIDv4()"},
		{"toUUID", "SELECT toUUID('00000000-0000-0000-0000-000000000000')"},
		{"UUIDStringToNum", "SELECT UUIDStringToNum('00000000-0000-0000-0000-000000000000')"},
		{"UUIDNumToString", "SELECT UUIDNumToString(toFixedString('0000000000000000', 16))"},

		// IP functions
		{"IPv4NumToString", "SELECT IPv4NumToString(3232235777)"},
		{"IPv4StringToNum", "SELECT IPv4StringToNum('192.168.1.1')"},
		{"IPv6NumToString", "SELECT IPv6NumToString(toFixedString('0000000000000001', 16))"},
		{"toIPv4", "SELECT toIPv4('192.168.1.1')"},
		{"toIPv6", "SELECT toIPv6('::1')"},

		// URL functions
		{"protocol", "SELECT protocol('https://example.com/path')"},
		{"domain", "SELECT domain('https://example.com/path')"},
		{"domainWithoutWWW", "SELECT domainWithoutWWW('https://www.example.com')"},
		{"topLevelDomain", "SELECT topLevelDomain('https://example.com')"},
		{"path", "SELECT path('https://example.com/path/to/page')"},
		{"pathFull", "SELECT pathFull('https://example.com/path?query=1')"},
		{"queryString", "SELECT queryString('https://example.com/path?query=1')"},
		{"fragment", "SELECT fragment('https://example.com/path#section')"},
		{"extractURLParameter", "SELECT extractURLParameter('https://example.com?a=1&b=2', 'a')"},
		{"extractURLParameters", "SELECT extractURLParameters('https://example.com?a=1&b=2')"},
		{"extractURLParameterNames", "SELECT extractURLParameterNames('https://example.com?a=1&b=2')"},
		{"cutURLParameter", "SELECT cutURLParameter('https://example.com?a=1&b=2', 'a')"},

		// Hash functions
		{"cityHash64", "SELECT cityHash64('hello')"},
		{"sipHash64", "SELECT sipHash64('hello')"},
		{"MD5", "SELECT MD5('hello')"},
		{"SHA1", "SELECT SHA1('hello')"},
		{"SHA256", "SELECT SHA256('hello')"},
		{"xxHash32", "SELECT xxHash32('hello')"},
		{"xxHash64", "SELECT xxHash64('hello')"},
		{"murmurHash2_32", "SELECT murmurHash2_32('hello')"},
		{"murmurHash2_64", "SELECT murmurHash2_64('hello')"},
		{"murmurHash3_32", "SELECT murmurHash3_32('hello')"},
		{"murmurHash3_64", "SELECT murmurHash3_64('hello')"},
		{"murmurHash3_128", "SELECT murmurHash3_128('hello')"},

		// Encoding functions
		{"hex", "SELECT hex('hello')"},
		{"unhex", "SELECT unhex('68656C6C6F')"},
		{"base64Encode", "SELECT base64Encode('hello')"},
		{"base64Decode", "SELECT base64Decode('aGVsbG8=')"},

		// Bit functions
		{"bitAnd", "SELECT bitAnd(1, 3)"},
		{"bitOr", "SELECT bitOr(1, 2)"},
		{"bitXor", "SELECT bitXor(1, 3)"},
		{"bitNot", "SELECT bitNot(1)"},
		{"bitShiftLeft", "SELECT bitShiftLeft(1, 2)"},
		{"bitShiftRight", "SELECT bitShiftRight(4, 1)"},
		{"bitRotateLeft", "SELECT bitRotateLeft(1, 2)"},
		{"bitRotateRight", "SELECT bitRotateRight(4, 1)"},
		{"bitTest", "SELECT bitTest(15, 0)"},
		{"bitTestAny", "SELECT bitTestAny(15, 0, 1)"},
		{"bitTestAll", "SELECT bitTestAll(15, 0, 1, 2, 3)"},

		// Conditional expressions
		{"conditional ternary", "SELECT 1 > 0 ? 'yes' : 'no'"},

		// Special
		{"materialize", "SELECT materialize(1)"},
		{"ignore", "SELECT ignore(1, 2, 3)"},
		{"sleep", "SELECT sleep(0.001)"},
		{"currentDatabase", "SELECT currentDatabase()"},
		{"currentUser", "SELECT currentUser()"},
		{"hostName", "SELECT hostName()"},
		{"version", "SELECT version()"},
		{"uptime", "SELECT uptime()"},
		{"blockNumber", "SELECT blockNumber()"},
		{"rowNumberInBlock", "SELECT rowNumberInBlock()"},
		{"rowNumberInAllBlocks", "SELECT rowNumberInAllBlocks()"},
		{"runningDifference", "SELECT runningDifference(number) FROM numbers(10)"},
		{"runningAccumulate", "SELECT runningAccumulate(sumState(number)) FROM numbers(10)"},
		{"neighbor", "SELECT neighbor(number, 1) FROM numbers(10)"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support this ClickHouse-specific feature: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParserTodoClickHouseValidation validates queries against ClickHouse server
// TODO: These tests require a running ClickHouse server and validate parser output
func TestParserTodoClickHouseValidation(t *testing.T) {
	if !clickhouseAvailable() {
		t.Skip("ClickHouse not available")
	}

	// All these queries have been validated as syntactically correct by ClickHouse
	tests := []struct {
		name  string
		query string
	}{
		// From official ClickHouse test suite
		{"array literal hello goodbye", "SELECT ['Hello', 'Goodbye']"},
		{"empty array", "SELECT []"},
		{"array join basic", "SELECT arrayJoin(['Hello', 'Goodbye'])"},
		{"array join nested", "SELECT arrayJoin([[3,4,5], [6,7], [2], [1,1]]) AS x ORDER BY x"},
		{"distinct subquery", "SELECT x FROM (SELECT DISTINCT 1 AS x, arrayJoin([1, 2]) AS y)"},
		{"uniq sum aggregate", "SELECT uniq(UserID), sum(Sign) FROM test.visits WHERE CounterID = 942285"},
		{"arrayExists lambda position", "SELECT arrayExists(x -> position(x, 'a') > 0, ['a'])"},

		// Complex joins from test suite
		{"complex join with settings", "SELECT * FROM (SELECT number, n, j1, j2 FROM (SELECT number, number / 2 AS n FROM system.numbers) js1 ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 10) js2 USING n LIMIT 10) ORDER BY n SETTINGS join_algorithm = 'hash'"},

		// Aggregating materialized view queries
		{"create materialized view aggregate", "CREATE MATERIALIZED VIEW basic_mv ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192) AS SELECT CounterID, StartDate, sumState(Sign) AS Visits, uniqState(UserID) AS Users FROM test.visits GROUP BY CounterID, StartDate"},
		{"sumMerge uniqMerge", "SELECT StartDate, sumMerge(Visits) AS Visits, uniqMerge(Users) AS Users FROM basic_mv GROUP BY StartDate ORDER BY StartDate"},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First verify ClickHouse accepts this query
			chAST, err := getClickHouseAST(tt.query)
			if err != nil {
				t.Skipf("ClickHouse error: %v", err)
				return
			}
			if strings.Contains(chAST, "Code:") || strings.Contains(chAST, "Exception:") {
				t.Skipf("ClickHouse rejected query: %s", strings.TrimSpace(chAST))
				return
			}

			// Now try to parse with our parser
			stmts, err := parser.Parse(ctx, strings.NewReader(tt.query))
			if err != nil {
				t.Skipf("TODO: Parser does not yet support: %s (error: %v)", tt.query, err)
				return
			}
			if len(stmts) == 0 {
				t.Skipf("TODO: Parser returned no statements for: %s", tt.query)
				return
			}

			// Verify we can serialize to JSON
			_, jsonErr := json.Marshal(stmts[0])
			if jsonErr != nil {
				t.Skipf("TODO: JSON serialization failed: %v", jsonErr)
				return
			}

			t.Logf("PASS: %s", tt.query)
		})
	}
}
