# TODO: Remaining Parser and Explain Issues

## Current State

- **Tests passing:** 5,197 (76.2%)
- **Tests skipped:** 1,627 (23.8%)
  - Parser issues: ~675
  - Explain mismatches: ~637

## Parser Issues

These require changes to `parser/parser.go`:

### Table/Database Names Starting with Numbers
Tables and databases with names starting with digits fail to parse:
```sql
DROP TABLE IF EXISTS 03657_gby_overflow;
DROP DATABASE IF EXISTS 03710_database;
```

### FORMAT Null
The `FORMAT Null` clause is not recognized:
```sql
SELECT ... FORMAT Null;
```

### FETCH FIRST ... ROW ONLY
SQL standard fetch syntax is not supported:
```sql
SELECT ... FETCH FIRST 1 ROW ONLY;
```

### INSERT INTO FUNCTION
Function-based inserts are not supported:
```sql
INSERT INTO FUNCTION file('file.parquet') SELECT ...;
```

### WITH ... AS Subquery Aliases
Subquery aliases in FROM clauses with keyword `AS`:
```sql
SELECT * FROM (SELECT 1 x) AS alias;
```

### String Concatenation Operator ||
The `||` operator in some contexts:
```sql
SELECT currentDatabase() || '_test' AS key;
```

### MOD/DIV Operators
The MOD and DIV keywords as operators:
```sql
SELECT number MOD 3, number DIV 3 FROM ...;
```

### Reserved Keyword Handling
Keywords like `LEFT`, `RIGHT` used as table aliases:
```sql
SELECT * FROM numbers(10) AS left RIGHT JOIN ...;
```

### Parameterized Settings
Settings with `$` parameters:
```sql
SET param_$1 = 'Hello';
```

### Incomplete CASE Expression
CASE without END:
```sql
SELECT CASE number  -- missing END
```

## Explain Output Issues

These require changes to `internal/explain/`:

### Double Equals (==) Operator
The `==` operator creates extra nested equals/tuple nodes:
```sql
SELECT value == '127.0.0.1:9181'
```
Expected: `Function equals` with `Identifier` and `Literal`
Got: Nested `Function equals` with extra `Function tuple`

### CreateQuery Spacing
Some ClickHouse versions output extra space before `(children`:
```
CreateQuery d1  (children 1)  -- two spaces
CreateQuery d1 (children 1)   -- one space (our output)
```

### Server Error Messages in Expected Output
Some test expected outputs include trailing messages:
```
The query succeeded but the server error '42' was expected
```
These are not part of the actual EXPLAIN output.

## Lower Priority

### DateTime64 with Timezone
Type parameters with string timezone:
```sql
DateTime64(3,'UTC')
```

### Complex Type Expressions
Nested type expressions in column definitions:
```sql
CREATE TABLE t (c LowCardinality(UUID));
```

### Parameterized Views
View definitions with parameters:
```sql
CREATE VIEW v AS SELECT ... WHERE x={parity:Int8};
```

## Testing Notes

Run tests with timeout to catch infinite loops:
```bash
go test ./parser -timeout 5s -v
```

Count test results:
```bash
go test ./parser -timeout 5s -v 2>&1 | grep -E 'PASS:|SKIP:' | cut -d':' -f1 | sort | uniq -c
```

View explain mismatches:
```bash
go test ./parser -timeout 5s -v 2>&1 | grep -A 30 "TODO: Explain output mismatch" | head -100
```
