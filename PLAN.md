# Comprehensive Plan: Fix Remaining Tests

## Current Status
- **Tests passing:** 6,005 (88.0%)
- **Tests skipped:** 819 (12.0%)
  - Parser failures: 173 tests
  - Explain mismatches: 331 tests
  - Other (metadata skip/explain=false): ~315 tests

## Phase 1: Parser Fixes (High Impact)

### 1.1 `view()` Table Function (~50 tests)
**Problem:** The `view(SELECT ...)` table function with inline subquery fails to parse.
```sql
SELECT * FROM view(SELECT 1 as id);
```
**Files:** `parser/parser.go` (parseTableExpression, parseFunctionCall)
**Solution:** When parsing a function call and the function name is `view`, check if the first argument starts with SELECT/WITH and parse it as a subquery instead of expression list.

### 1.2 Complex Type Casts with Named Parameters (~30 tests)
**Problem:** `::Tuple(a UInt32, b String)` with named fields fails
```sql
SELECT tuple(42, 42)::Tuple(a UInt32, b UInt32);
```
**Files:** `parser/expression.go` (parseDataType)
**Solution:** Extend parseDataType to handle named parameters in type constructors like `Tuple(name Type, ...)`.

### 1.3 DESCRIBE on Table Functions (~20 tests)
**Problem:** `desc format()`, `desc url()`, `desc s3Cluster()` fail
```sql
desc format(CSV, '"value"');
```
**Files:** `parser/parser.go` (parseDescribe)
**Solution:** Handle table function after DESC/DESCRIBE by calling parseTableExpression.

### 1.4 INSERT INTO FUNCTION (~15 tests)
**Problem:** INSERT INTO FUNCTION with file paths and settings fails
```sql
insert into function file(02458_data.jsonl) select * settings engine_file_truncate_on_insert=1;
```
**Files:** `parser/parser.go` (parseInsert)
**Solution:** Handle TABLE FUNCTION keyword and parse function call with settings.

### 1.5 CREATE USER / FUNCTION / DICTIONARY (~10 tests)
**Problem:** These CREATE variants are not supported
```sql
CREATE USER test_user GRANTEES ...;
CREATE DICTIONARY d0 (c1 UInt64) PRIMARY KEY c1;
```
**Files:** `parser/parser.go` (parseCreate)
**Solution:** Add cases for USER, FUNCTION, DICTIONARY in parseCreate switch.

### 1.6 SHOW SETTINGS (~5 tests)
**Problem:** SHOW SETTINGS LIKE syntax not supported
```sql
show settings like 'send_timeout';
```
**Files:** `parser/parser.go` (parseShow)
**Solution:** Handle SETTINGS keyword after SHOW.

### 1.7 PASTE JOIN (~3 tests)
**Problem:** PASTE JOIN is not recognized
```sql
SELECT * FROM t1 PASTE JOIN t2;
```
**Files:** `parser/parser.go` (parseTableExpression or join parsing)
**Solution:** Add PASTE as a valid join type.

### 1.8 `any()` Subquery Syntax (~2 tests)
**Problem:** `== any (SELECT ...)` syntax not supported
```sql
select 1 == any (select number from numbers(10));
```
**Files:** `parser/expression.go`
**Solution:** Handle `any(subquery)` as a special expression form after comparison operators.

---

## Phase 2: Explain Layer Fixes (Medium Impact)

### 2.1 INDEX Clause in CREATE TABLE (~50 tests)
**Problem:** INDEX definitions are skipped but should produce explain output
```sql
CREATE TABLE t (x UInt8, INDEX i x TYPE hypothesis GRANULARITY 100);
```
**Files:** `parser/parser.go` (parseCreateTable), `internal/explain/statements.go`
**Solution:**
1. Parse INDEX into an ast.IndexDefinition struct
2. Add explain output for index definitions

### 2.2 SETTINGS Inside Function Arguments (~40 tests)
**Problem:** SETTINGS in table functions should create a Set child
```sql
SELECT * FROM icebergS3(s3_conn, SETTINGS key='value');
```
**Files:** `parser/expression.go` (parseFunctionCall), `internal/explain/functions.go`
**Solution:** Capture SETTINGS as a Set node attached to the function call, output in explain.

### 2.3 WITH FILL Clause (~30 tests)
**Problem:** ORDER BY ... WITH FILL is not captured
```sql
SELECT nan ORDER BY 1 WITH FILL;
```
**Files:** `parser/parser.go` (parseOrderByItem), `internal/explain/select.go`
**Solution:** Add WithFill field to OrderItem, parse WITH FILL, output in explain.

### 2.4 Column CODEC Clause (~20 tests)
**Problem:** CODEC(GCD, LZ4) in columns not captured
```sql
CREATE TABLE t (col UInt32 CODEC(GCD, LZ4));
```
**Files:** `parser/parser.go` (parseColumnDeclaration), `internal/explain/statements.go`
**Solution:** Parse CODEC clause into ColumnDeclaration, output in explain.

### 2.5 Column EPHEMERAL Modifier (~15 tests)
**Problem:** EPHEMERAL keyword not captured
```sql
CREATE TABLE t (a Int EPHEMERAL);
```
**Files:** `parser/parser.go` (parseColumnDeclaration)
**Solution:** Add Ephemeral field to ColumnDeclaration, parse and explain.

### 2.6 CREATE TABLE ... AS function() (~15 tests)
**Problem:** CREATE TABLE AS s3Cluster(...) should have Function child
```sql
CREATE TABLE test AS s3Cluster('cluster', 'url');
```
**Files:** `parser/parser.go` (parseCreateTable), `internal/explain/statements.go`
**Solution:** Parse AS clause when followed by function call, store as TableFunction field.

### 2.7 WithElement Wrapper for CTEs (~20 tests)
**Problem:** Some CTEs need WithElement wrapper in output
```sql
WITH sub AS (SELECT ...) SELECT ...;
```
**Files:** `internal/explain/select.go`
**Solution:** Output WithElement wrapper when appropriate for CTE definitions.

### 2.8 Float Scientific Notation (~15 tests)
**Problem:** Very small/large floats should use scientific notation
```sql
SELECT 2.2250738585072014e-308;
```
**Files:** `internal/explain/format.go`
**Solution:** Format floats using scientific notation when appropriate.

### 2.9 Negative Literals in Arrays (~10 tests)
**Problem:** Arrays with negatives may output Function instead of Literal
```sql
SELECT [-10000, 5750];
```
**Files:** `internal/explain/expressions.go`
**Solution:** Properly detect and format negative integer literals in arrays.

### 2.10 Parameterized View Placeholders (~10 tests)
**Problem:** `{name:Type}` parameters in views
```sql
create view v as select number where number%2={parity:Int8};
```
**Files:** `internal/explain/expressions.go`
**Solution:** Output Parameter nodes correctly with type info.

### 2.11 Column TTL (~10 tests)
**Problem:** TTL expression on columns not captured
```sql
CREATE TABLE t (c Int TTL expr());
```
**Files:** `parser/parser.go` (parseColumnDeclaration)
**Solution:** Parse TTL clause into ColumnDeclaration.

---

## Phase 3: Lower Priority Fixes

### 3.1 GROUPING SETS (~5 tests)
```sql
SELECT ... GROUP BY GROUPING SETS ((a), (b));
```

### 3.2 QUALIFY Clause (~5 tests)
```sql
SELECT x QUALIFY row_number() OVER () = 1;
```

### 3.3 INTO OUTFILE TRUNCATE (~3 tests)
```sql
SELECT 1 INTO OUTFILE '/dev/null' TRUNCATE FORMAT Npy;
```

### 3.4 INTERVAL with Dynamic Type (~3 tests)
```sql
SELECT INTERVAL c0::Dynamic DAY;
```

### 3.5 ALTER TABLE with Multiple Operations (~3 tests)
```sql
ALTER TABLE t (DELETE WHERE ...), (UPDATE ... WHERE ...);
```

### 3.6 EXPLAIN SYNTAX for SYSTEM commands (~2 tests)
```sql
explain syntax system drop schema cache for hdfs;
```

---

## Implementation Order (Recommended)

1. **Week 1: Parser Fundamentals**
   - 1.2 Complex Type Casts (unlocks many tests)
   - 1.1 view() Table Function (high impact)
   - 1.3 DESCRIBE on Table Functions

2. **Week 2: Parser Completeness**
   - 1.4 INSERT INTO FUNCTION
   - 1.5 CREATE USER/FUNCTION/DICTIONARY
   - 1.6 SHOW SETTINGS
   - 1.7 PASTE JOIN
   - 1.8 any() Subquery

3. **Week 3: Explain Layer - CREATE TABLE**
   - 2.1 INDEX Clause
   - 2.4 CODEC Clause
   - 2.5 EPHEMERAL Modifier
   - 2.6 CREATE TABLE AS function()
   - 2.11 Column TTL

4. **Week 4: Explain Layer - SELECT**
   - 2.2 SETTINGS in Functions
   - 2.3 WITH FILL
   - 2.7 WithElement for CTEs
   - 2.10 Parameterized View Placeholders

5. **Week 5: Explain Layer - Formatting**
   - 2.8 Float Scientific Notation
   - 2.9 Negative Literals in Arrays

6. **Week 6: Remaining Items**
   - Phase 3 lower priority items

---

## Estimated Impact

| Phase | Tests Fixed | New Pass Rate |
|-------|-------------|---------------|
| 1.1-1.4 | ~115 | ~90% |
| 1.5-1.8 | ~20 | ~90.5% |
| 2.1-2.6 | ~140 | ~93% |
| 2.7-2.11 | ~65 | ~94% |
| Phase 3 | ~20 | ~94.5% |

---

## Files to Modify

### Parser Layer
- `parser/parser.go` - Main parser (CREATE, INSERT, DESCRIBE, SHOW, joins)
- `parser/expression.go` - Expression parsing (type casts, functions, special syntax)
- `ast/ast.go` - AST node definitions (IndexDefinition, new fields)

### Explain Layer
- `internal/explain/statements.go` - CREATE TABLE explain
- `internal/explain/select.go` - SELECT explain (WITH FILL, CTEs)
- `internal/explain/functions.go` - Function explain (SETTINGS)
- `internal/explain/expressions.go` - Expression explain (literals, parameters)
- `internal/explain/format.go` - Output formatting (scientific notation)

---

## Testing Strategy

1. Run tests frequently: `go test ./parser -timeout 5s`
2. After each fix, verify no regressions: compare PASS count
3. Check specific test cases: `go test ./parser -v -run "TestParser/test_name"`
4. Monitor for infinite loops (timeout protection already in place)
