# TODO: Remaining Parser and Explain Issues

## Current State

- **Tests passing:** 6,006 (88.0%)
- **Tests skipped:** 819 (12.0%)

## Recently Fixed (explain layer)

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

## Parser Issues (High Priority)

These require changes to `parser/parser.go`:

### CREATE TABLE with INDEX Clause
INDEX definitions in CREATE TABLE are not captured:
```sql
CREATE TABLE t (x Array(String), INDEX idx1 x TYPE bloom_filter(0.025)) ENGINE=MergeTree;
```

### SETTINGS Inside Function Arguments
SETTINGS clause within function calls is not parsed:
```sql
SELECT * FROM icebergS3(s3_conn, filename='test', SETTINGS key='value');
-- The SETTINGS should become a Set child of the function
```

### CREATE TABLE with Column TTL
TTL expressions on columns are not captured:
```sql
CREATE TABLE t (c Int TTL expr()) ENGINE=MergeTree;
-- Expected: ColumnDeclaration with 2 children (type + TTL function)
```

## Parser Issues (Medium Priority)

### CREATE DICTIONARY
Dictionary definitions are not supported:
```sql
CREATE DICTIONARY d0 (c1 UInt64) PRIMARY KEY c1 LAYOUT(FLAT()) SOURCE(...);
```

### CREATE USER / CREATE FUNCTION
User and function definitions are not supported:
```sql
CREATE USER test_user GRANTEES ...;
CREATE OR REPLACE FUNCTION myFunc AS ...;
```

### QUALIFY Clause
Window function filtering clause:
```sql
SELECT x QUALIFY row_number() OVER () = 1;
```

### INTO OUTFILE with TRUNCATE
Extended INTO OUTFILE syntax:
```sql
SELECT 1, 2 INTO OUTFILE '/dev/null' TRUNCATE FORMAT Npy;
```

### GROUPING SETS
Advanced grouping syntax:
```sql
SELECT ... GROUP BY GROUPING SETS ((a), (b));
```

### view() Table Function
The view() table function in FROM:
```sql
SELECT * FROM view(SELECT 1 as id);
```

### CREATE TABLE ... AS SELECT
CREATE TABLE with inline SELECT:
```sql
CREATE TABLE src ENGINE=Memory AS SELECT 1;
```

### Variant() Type with PRIMARY KEY
Complex column definitions:
```sql
CREATE TABLE t (c Variant() PRIMARY KEY) ENGINE=Redis(...);
```

## Parser Issues (Lower Priority)

### INTERVAL with Dynamic Type
INTERVAL with type cast:
```sql
SELECT INTERVAL 1 MINUTE AS c0, INTERVAL c0::Dynamic DAY;
```

### ALTER TABLE with Multiple Operations
Multiple ALTER operations in parentheses:
```sql
ALTER TABLE t (DELETE WHERE ...), (MODIFY SETTING ...), (UPDATE ... WHERE ...);
```

### Tuple Type in Column with Subfield Access
Tuple type with engine using subfield:
```sql
CREATE TABLE t (t Tuple(a Int32)) ENGINE=EmbeddedRocksDB() PRIMARY KEY (t.a);
```

### insert() Function with input()
INSERT using input() function:
```sql
INSERT INTO FUNCTION null() SELECT * FROM input('x Int') ...;
```

## Explain Issues (Remaining)

### Scientific Notation for Floats
Very small/large floats should use scientific notation:
```sql
SELECT 2.2250738585072014e-308;
-- Expected: Float64_2.2250738585072014e-308
-- Got: Float64_0.0000...22250738585072014
```

### Array Literals with Negative Numbers
Arrays with negative integers may still expand to Function instead of Literal in some cases:
```sql
SELECT [-10000, 5750];
-- Some cases now work correctly with Literal Int64_-10000
-- Complex nested arrays may still require additional work
```

### WithElement for CTE Subqueries
Some CTE subqueries should use WithElement wrapper:
```sql
WITH sub AS (SELECT ...) SELECT ...;
-- Expected: WithElement (children 1) > Subquery > SelectWithUnionQuery
```

## Testing Notes

Run tests with timeout to catch infinite loops:
```bash
go test ./parser -timeout 5s -v
```

Count test results:
```bash
go test ./parser -v 2>&1 | grep -E 'PASS:|SKIP:' | wc -l
```

View explain mismatches:
```bash
go test ./parser -v 2>&1 | grep -A 30 "TODO: Explain output mismatch" | head -100
```

View parser failures:
```bash
go test ./parser -v 2>&1 | grep "TODO: Parser does not yet support" | head -20
```
