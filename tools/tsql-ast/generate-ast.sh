#!/bin/bash
# Generate ast.json files for T-SQL test cases
#
# Usage: ./generate-ast.sh [test-dir]
#   test-dir: Optional specific test directory to process
#             If not provided, processes all directories in parser/testdata/

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/TsqlAstParser"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TESTDATA_DIR="$REPO_ROOT/parser/testdata"

# Build the project if needed
if [ ! -f "$PROJECT_DIR/bin/Release/net8.0/TsqlAstParser.dll" ]; then
    echo "Building TsqlAstParser..."
    cd "$PROJECT_DIR"
    dotnet build -c Release
fi

# Function to process a single test directory
process_test() {
    local dir="$1"
    local query_file="$dir/query.sql"
    local ast_file="$dir/ast.json"

    if [ ! -f "$query_file" ]; then
        echo "Skipping $dir: no query.sql found"
        return
    fi

    echo "Processing: $dir"
    dotnet run --project "$PROJECT_DIR" -c Release -- "$query_file" "$ast_file" 2>/dev/null || {
        echo "  Error parsing $query_file"
        return 1
    }
}

# Process directories
if [ -n "$1" ]; then
    # Process specific directory
    if [ -d "$1" ]; then
        process_test "$1"
    else
        echo "Error: Directory not found: $1"
        exit 1
    fi
else
    # Process all test directories
    echo "Generating ast.json for all test cases in $TESTDATA_DIR"

    count=0
    errors=0

    for dir in "$TESTDATA_DIR"/*/; do
        if process_test "$dir"; then
            ((count++))
        else
            ((errors++))
        fi
    done

    echo ""
    echo "Done: $count files generated, $errors errors"
fi
