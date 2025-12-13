#!/bin/bash
# Generate explain.txt for all test queries in batches

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TESTDATA_DIR="$PROJECT_DIR/parser/testdata"
CLICKHOUSE_BIN="$PROJECT_DIR/clickhouse"

BATCH_SIZE=${1:-100}
START_BATCH=${2:-0}

# Get all test directories sorted
mapfile -t TEST_DIRS < <(find "$TESTDATA_DIR" -type d -mindepth 1 | sort)
TOTAL=${#TEST_DIRS[@]}

echo "Total test directories: $TOTAL"
echo "Batch size: $BATCH_SIZE"
echo "Starting from batch: $START_BATCH"

SUCCESS=0
FAILED=0
SKIPPED=0

START_IDX=$((START_BATCH * BATCH_SIZE))
END_IDX=$((START_IDX + BATCH_SIZE))
if [ $END_IDX -gt $TOTAL ]; then
    END_IDX=$TOTAL
fi

echo "Processing indices $START_IDX to $((END_IDX - 1))"

for ((i=START_IDX; i<END_IDX; i++)); do
    dir="${TEST_DIRS[$i]}"
    name=$(basename "$dir")
    query_file="$dir/query.sql"
    explain_file="$dir/explain.txt"
    metadata_file="$dir/metadata.json"

    if [ ! -f "$query_file" ]; then
        echo "[$i] SKIP $name: no query.sql"
        ((SKIPPED++))
        continue
    fi

    # Read first non-comment line
    query=""
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip empty lines and comments
        trimmed="${line#"${line%%[![:space:]]*}"}"
        if [ -z "$trimmed" ] || [[ "$trimmed" == --* ]]; then
            continue
        fi
        query="$trimmed"
        break
    done < "$query_file"

    if [ -z "$query" ]; then
        echo "[$i] SKIP $name: empty query"
        ((SKIPPED++))
        continue
    fi

    # Run EXPLAIN AST
    result=$("$CLICKHOUSE_BIN" client --query "EXPLAIN AST $query" 2>&1)
    exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "$result" > "$explain_file"
        echo "[$i] OK $name"
        ((SUCCESS++))
    else
        # Update metadata.json with explain: false
        if [ -f "$metadata_file" ]; then
            # Read existing metadata and merge with explain: false
            existing=$(cat "$metadata_file" | tr -d '\n')
            if [[ "$existing" == "{}"* ]]; then
                echo '{"explain":false}' > "$metadata_file"
            elif [[ "$existing" == "{"* ]]; then
                # Remove leading { and prepend with {"explain":false,
                rest="${existing#\{}"
                echo "{\"explain\":false,$rest" > "$metadata_file"
            else
                echo '{"explain":false}' > "$metadata_file"
            fi
        else
            echo '{"explain":false}' > "$metadata_file"
        fi
        echo "[$i] FAIL $name"
        ((FAILED++))
    fi
done

echo ""
echo "Batch complete: Success=$SUCCESS, Failed=$FAILED, Skipped=$SKIPPED"
