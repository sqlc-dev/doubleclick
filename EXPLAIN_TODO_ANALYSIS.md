# EXPLAIN Test Analysis

## Summary

After extensive analysis of the 2446 explain_todo tests, I found that the test data contains fundamental inconsistencies that prevent systematic fixes.

## Key Findings

### 1. LIMIT 0 and FORMAT Stripping

The expected output varies between tests for identical patterns:
- **02998_system_dns_cache_table**: Expects LIMIT 0 and FORMAT to be STRIPPED
- **03031_table_function_fuzzquery**: Expects LIMIT 0 and FORMAT to be KEPT

Both tests have `LIMIT 0 FORMAT TSVWithNamesAndTypes;` but expect different outputs.

### 2. SETTINGS Position

The position of `Set` in the output varies:
- **01293_external_sorting_limit_bug** (explain_todo): Expects Set at SelectQuery level
- **01104_distributed_numbers_test** (passing): Expects Set at SelectWithUnionQuery level

Changing the logic to fix one breaks the other.

### 3. AND/OR Flattening

Some tests expect flattened boolean operations, others expect nested:
- **00824_filesystem** (explain_todo): Expects `Function and` with 3 children (flattened)
- **03653_keeper_histogram_metrics** (passing): Expects nested `Function and` (2 children each)

Implementing flattening broke 173 passing tests.

## Root Cause

The `explain.txt` files were generated from different ClickHouse versions or configurations, leading to inconsistent expected outputs. Without regenerating test data with a consistent ClickHouse version, these inconsistencies cannot be resolved.

## Statistics

- Total tests with explain_todo: 2446
- Tests with stmt1 in explain_todo: 142
- Tests currently passing from explain_todo: 0

## Recommendations

1. **Regenerate test data**: Run all tests against a single ClickHouse version to get consistent expected output
2. **Version-specific logic**: If supporting multiple ClickHouse versions, implement version detection
3. **Focus on specific patterns**: Fix individual tests rather than broad changes when required
