# TODO: Remaining Parser and Explain Issues

## Current State

- **Tests passing:** ~6,066 (88.9%)
- **Tests skipped:** ~758 (11.1%)
- **Parser errors:** 7 remaining

## Recently Fixed (Latest Session)

### Lexer Improvements
- ✅ Dollar-quoted strings (`$$...$$`)
- ✅ Hex P notation floats (`0x123p4`, `-0x1P1023`)
- ✅ Backtick escaping (`` `ta``ble` ``)
- ✅ BOM (byte order mark) character handling
- ✅ Dollar signs in identifiers (`$alias$name$`)

### Parser Improvements
- ✅ SYSTEM DROP FORMAT SCHEMA CACHE
- ✅ EXPLAIN AST options (`EXPLAIN AST optimize=0 SELECT ...`)
- ✅ WITH scalar expression without alias (`WITH 1 SELECT 1`)
- ✅ DROP USER with @ hostname (`test_user@localhost`, `test_user@'192.168.23.15'`)
- ✅ KEY keyword as implicit alias (`view(select 'foo.com' key)`)
- ✅ Complex UNION with parentheses (`((SELECT 1) UNION ALL SELECT 2)`)

## Previously Fixed (parser layer)

- ✅ SELECT ALL syntax (`SELECT ALL 'a'`)
- ✅ FILTER clause on aggregate functions (`argMax() FILTER(WHERE ...)`)
- ✅ DROP SETTINGS PROFILE (`DROP SETTINGS PROFILE IF EXISTS ...`)
- ✅ CREATE NAMED COLLECTION (`CREATE NAMED COLLECTION ... AS ...`)
- ✅ WITH column AS alias syntax (`WITH number AS k SELECT k`)
- ✅ SHOW TABLES NOT LIKE (`SHOW TABLES NOT LIKE '%'`)
- ✅ SHOW CREATE QUOTA (`SHOW CREATE QUOTA default`)
- ✅ LIMIT BY with second LIMIT (`LIMIT 1 BY * LIMIT 1`)
- ✅ WITH TOTALS HAVING clause (`SELECT count() WITH TOTALS HAVING x != 0`)
- ✅ COLLATE in column definitions (`varchar(255) COLLATE binary NOT NULL`)
- ✅ SETTINGS with keyword assignments (`SETTINGS limit=5`)
- ✅ TTL GROUP BY SET clause (`TTL d + interval 1 second GROUP BY x SET y = max(y)`)
- ✅ DROP ROW POLICY ON wildcard (`DROP ROW POLICY ... ON default.*`)
- ✅ INSERT FROM INFILE COMPRESSION (`FROM INFILE '...' COMPRESSION 'gz'`)
- ✅ FROM before SELECT syntax (`FROM numbers(1) SELECT number`)
- ✅ Parenthesized SELECT at statement level (`(SELECT 1)`)
- ✅ EXISTS table syntax (`EXISTS db.table`)
- ✅ DROP TABLE FORMAT (`DROP TABLE IF EXISTS t FORMAT Null`)

## Previously Fixed (explain layer)

- ✅ TableJoin output - removed join type keywords
- ✅ Table function aliases (e.g., `remote('127.1') AS t1`)
- ✅ Table identifier aliases (e.g., `system.one AS xxx`)
- ✅ Array/tuple cast formatting for `::` syntax
- ✅ SETTINGS placement with FORMAT clause
- ✅ Concat operator `||` flattening into single `concat` function
- ✅ Window function (OVER clause) support
- ✅ Float literal formatting
- ✅ Aliased expression handling for binary/unary/function/identifier
- ✅ PARTITION BY support in CREATE TABLE
- ✅ Server error message stripping from expected output
- ✅ DROP TABLE with multiple tables (e.g., `DROP TABLE t1, t2, t3`)
- ✅ Negative integer/float literals (e.g., `-1` → `Literal Int64_-1`)
- ✅ Empty tuple in ORDER BY (e.g., `ORDER BY ()` → `Function tuple` with empty `ExpressionList`)
- ✅ String escape handling (lexer now unescapes `\'`, `\\`, `\n`, `\t`, `\0`, etc.)

## Remaining Parser Issues (7 total)

### Multi-line SQL (Test Framework Limitation)
These are valid SQL split across multiple lines. Our test framework only reads the first line:
- Incomplete CASE expressions (`SELECT CASE number`, `SELECT CASE`, `SELECT "number", CASE "number"`)

### QUALIFY Clause with ^ Operator
Window function filtering with caret operator:
```sql
SELECT '{}'::JSON x QUALIFY x.^c0 = 1;
```

### Parenthesized ALTER
Multiple ALTER operations in parentheses:
```sql
ALTER TABLE t22 (DELETE WHERE ...), (MODIFY SETTING ...), (UPDATE ... WHERE ...);
```

### INSERT with JSON Data
JSON data after FORMAT clause:
```sql
INSERT INTO FUNCTION null() SELECT * FROM input('x Int') ... FORMAT JSONEachRow {"x": 1};
```

### EXCEPT in Nested Expressions
`* EXCEPT` within nested function calls:
```sql
SELECT untuple((expr, * EXCEPT b));
```

## Parser Issues (High Priority)

### CREATE TABLE with INDEX Clause
```sql
CREATE TABLE t (x Array(String), INDEX idx1 x TYPE bloom_filter(0.025)) ENGINE=MergeTree;
```

### SETTINGS Inside Function Arguments
```sql
SELECT * FROM icebergS3(s3_conn, filename='test', SETTINGS key='value');
```

### CREATE TABLE with Column TTL
```sql
CREATE TABLE t (c Int TTL expr()) ENGINE=MergeTree;
```

## Parser Issues (Medium Priority)

### CREATE DICTIONARY
```sql
CREATE DICTIONARY d0 (c1 UInt64) PRIMARY KEY c1 LAYOUT(FLAT()) SOURCE(...);
```

### QUALIFY Clause
```sql
SELECT x QUALIFY row_number() OVER () = 1;
```

### GROUPING SETS
```sql
SELECT ... GROUP BY GROUPING SETS ((a), (b));
```

### CREATE TABLE ... AS SELECT
```sql
CREATE TABLE src ENGINE=Memory AS SELECT 1;
```

## Testing Notes

Run tests with timeout:
```bash
go test ./parser -timeout 5s -v
```

Count test results:
```bash
go test ./parser -v 2>&1 | grep -E 'PASS:|SKIP:' | wc -l
```

View parser failures:
```bash
go test ./parser -v 2>&1 | grep "TODO: Parser does not yet support" | head -20
```
