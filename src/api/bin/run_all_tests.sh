#!/bin/bash

# Run all API tests
# 
# This script runs the test suite for all API modules and non-deprecated subapis
# in the project. It executes each API/subapi's run_tests.sh script and reports results.
# Deprecated subapis (prefixed with _) are automatically skipped.
# Compatible with Bash 3.2+ (macOS default)
#
# Usage:
#   ./run_all_tests.sh                    # Stop on first failure (default)
#   ./run_all_tests.sh --continue         # Continue running all tests even if some fail
#   ./run_all_tests.sh --start-from news  # Skip tests until API/subapi name contains "news"
#   ./run_all_tests.sh --only news        # Run ONLY APIs/subapis whose name contains "news"
#   ./run_all_tests.sh --only spotify     # Run ONLY subapis matching "spotify"
#   ./run_all_tests.sh --start-from news --continue  # Combine options

# Color output (defined early for use in error messages)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Parse arguments
CONTINUE_ON_FAILURE=false
START_FROM=""
ONLY_MATCH=""
CLEAR_CACHE=""  # Empty means prompt, "yes" means clear, "no" means don't clear
VERBOSE=false
TEST_PATTERN=""  # Pattern to match test function names
while [[ $# -gt 0 ]]; do
    case $1 in
        --continue)
            CONTINUE_ON_FAILURE=true
            shift
            ;;
        --start-from)
            START_FROM="$2"
            shift 2
            ;;
        --only)
            ONLY_MATCH="$2"
            shift 2
            ;;
        --pattern)
            TEST_PATTERN="$2"
            shift 2
            ;;
        --y)
            CLEAR_CACHE="yes"
            shift
            ;;
        --n)
            CLEAR_CACHE="no"
            shift
            ;;
        -v|--verbose|--v)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            echo "Usage: ./run_all_tests.sh [OPTIONS]"
            echo ""
            echo "This script runs linting checks (ruff, mypy, biome) and then executes"
            echo "test suites for API modules and non-deprecated subapis. When --only is"
            echo "used, linting runs only for the specified API/subapi directory."
            echo ""
            echo "Options:"
            echo "  --continue              Continue running all tests even if some fail"
            echo "  --start-from STRING     Skip tests until API/subapi name contains STRING"
            echo "  --only STRING           Run ONLY APIs/subapis whose name contains STRING"
            echo "                         (searches both APIs and subapis, skips deprecated)"
            echo "                         (linting checks also run only for this API/subapi)"
            echo "  --pattern STRING        Run only test functions matching STRING pattern (pytest -k)"
            echo "                         (use with --only to run specific tests in specific API)"
            echo "  --y                     Clear cache and run tests (non-interactive, cache disabled)"
            echo "  --n                     Don't clear cache and run tests (non-interactive, cache enabled)"
            echo "  -v, --verbose, --v      Enable verbose output (adds -s flag to pytest for uncaptured output)"
            echo "  -h, --help              Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run_all_tests.sh"
            echo "  ./run_all_tests.sh --continue"
            echo "  ./run_all_tests.sh --start-from news"
            echo "  ./run_all_tests.sh --only comscore"
            echo "  ./run_all_tests.sh --only spotify"
            echo "  ./run_all_tests.sh --only news --continue"
            echo "  ./run_all_tests.sh --start-from lastfm --continue"
            echo "  ./run_all_tests.sh --y                  # Clear cache and run"
            echo "  ./run_all_tests.sh --n                  # Don't clear cache and run"
            echo "  ./run_all_tests.sh --verbose             # Show all output (pytest -s)"
            echo "  ./run_all_tests.sh --v --only lastfm     # Verbose mode for specific API"
            echo "  ./run_all_tests.sh --only newsai --pattern debug  # Run only tests with 'debug' in name"
            echo "  ./run_all_tests.sh --only newsai --pattern 'test_get_trending' --v  # Specific test with verbose"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Validate mutually exclusive options
if [ -n "$START_FROM" ] && [ -n "$ONLY_MATCH" ]; then
    echo -e "${RED}Error: --start-from and --only cannot be used together${NC}"
    echo "Use -h or --help for usage information"
    exit 1
fi

# Set verbose flag for pytest if requested
if [ "$VERBOSE" = true ]; then
    export PYTEST_VERBOSE="-s"
    echo -e "${CYAN}Verbose mode enabled (pytest -s)${NC}"
    echo ""
else
    export PYTEST_VERBOSE=""
fi

# Set test pattern for pytest if requested
if [ -n "$TEST_PATTERN" ]; then
    export PYTEST_PATTERN="-k $TEST_PATTERN"
    echo -e "${CYAN}Test pattern filter enabled: '$TEST_PATTERN' (pytest -k)${NC}"
    echo ""
else
    export PYTEST_PATTERN=""
fi

# Get the directory where this script is located
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
API_DIR=$(dirname "$SCRIPT_DIR")
PYTHON_FUNCTIONS_DIR=$(dirname "$API_DIR")
PROJECT_ROOT=$(cd "$PYTHON_FUNCTIONS_DIR/../.." && pwd)

clear_cache() {
    local target_dir="${1:-}"
    # Handle cache clearing
    if [ -z "$CLEAR_CACHE" ]; then
        # Interactive mode: prompt the user
        echo -e "${YELLOW}Do you want to clean cache? (y/n)${NC}"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            CLEAR_CACHE="yes"
        else
            CLEAR_CACHE="no"
        fi
    fi

    if [ "$CLEAR_CACHE" = "yes" ]; then
        if [ -n "$target_dir" ]; then
            echo -e "${CYAN}Clearing cache at /tmp/cache/$target_dir${NC}"
            rm -rf /tmp/cache/"$target_dir"*
        else
            echo -e "${CYAN}Clearing all cache at /tmp/cache${NC}"
            rm -rf /tmp/cache
        fi
        echo -e "${GREEN}✓ Cache cleared${NC}"
        echo ""
        # Disable cache for this run since we cleared it
        export ENABLE_CACHE_FOR_TESTS=""
    elif [ "$CLEAR_CACHE" = "no" ]; then
        echo -e "${CYAN}Keeping existing cache (cache will be enabled for integration tests)${NC}"
        echo ""
        # Enable cache for integration tests when --n is used
        export ENABLE_CACHE_FOR_TESTS="1"
    fi
}

# Activate virtual environment if it exists
if [ -d "$API_DIR/../venv" ]; then
    echo -e "${CYAN}Activating virtual environment...${NC}"
    source "$API_DIR/../venv/bin/activate"
    echo ""
fi

# Determine the target directory for cache clearing based on --only flag
CACHE_TARGET_DIR=""
if [ -n "$ONLY_MATCH" ]; then
    # Find the matching API name (check both APIs and subapis)
    # Check APIs first
    for api_path in "$API_DIR"/*/bin/run_tests.sh; do
        if [ -f "$api_path" ]; then
            api_name=$(basename $(dirname $(dirname "$api_path")))
            if [[ "$api_name" == *"$ONLY_MATCH"* ]]; then
                CACHE_TARGET_DIR="$api_name"
                break
            fi
        fi
    done
    
    # Check subapis (skip deprecated ones with _ prefix)
    if [ -z "$CACHE_TARGET_DIR" ]; then
        for api_path in "$API_DIR/subapi"/*/bin/run_tests.sh; do
            if [ -f "$api_path" ]; then
                api_name=$(basename $(dirname $(dirname "$api_path")))
                # Skip deprecated APIs (those starting with _)
                if [[ "$api_name" == _* ]]; then
                    continue
                fi
                if [[ "$api_name" == *"$ONLY_MATCH"* ]]; then
                    CACHE_TARGET_DIR="$api_name"
                    break
                fi
            fi
        done
    fi
fi

# Clear cache with the appropriate target directory
clear_cache "$CACHE_TARGET_DIR"

# Set Firebase emulator environment variables to block GCS during tests
# This ensures that tests don't attempt to use Google Cloud Storage
if [ -z "$FIREBASE_AUTH_EMULATOR_HOST" ]; then
    export FIREBASE_AUTH_EMULATOR_HOST="localhost:9099"
fi
if [ -z "$FIRESTORE_EMULATOR_HOST" ]; then
    export FIRESTORE_EMULATOR_HOST="localhost:8080"
fi

# Function to run Python linting checks (ruff + mypy)
# Usage: run_python_linting [api_directory]
# If api_directory is provided, only checks that API directory
run_python_linting() {
    local target_dir="${1:-}"
    local check_dir
    
    if [ -n "$target_dir" ]; then
        check_dir="$API_DIR/$target_dir"
        if [ ! -d "$check_dir" ]; then
            echo -e "${RED}Error: API directory not found: $check_dir${NC}"
            return 1
        fi
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo -e "${BOLD}${BLUE}🔍 Running Python Linting Checks: $target_dir${NC}"
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo ""
    else
        check_dir="$PYTHON_FUNCTIONS_DIR"
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo -e "${BOLD}${BLUE}🔍 Running Python Linting Checks${NC}"
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo ""
    fi
    
    cd "$PYTHON_FUNCTIONS_DIR"
    
    # Check if required packages are installed
    if ! python -c "import ruff" 2>/dev/null; then
        echo -e "${YELLOW}Installing development dependencies...${NC}"
        pip install -q -r requirements-dev.txt 2>/dev/null || {
            echo -e "${RED}Failed to install development dependencies${NC}"
            return 1
        }
    fi
    
    # Check if mypy is available
    if ! command -v mypy &> /dev/null && ! python -c "import mypy" 2>/dev/null; then
        echo -e "${YELLOW}mypy not found. Installing...${NC}"
        pip install -q mypy 2>/dev/null || {
            echo -e "${RED}Failed to install mypy${NC}"
            return 1
        }
    fi
    
    local lint_errors=0
    
    # Determine the path to check relative to PYTHON_FUNCTIONS_DIR
    local check_path
    if [ -n "$target_dir" ]; then
        check_path="api/$target_dir"
    else
        check_path="."
    fi
    
    # Run Ruff checks with auto-fix attempts (up to 3 iterations)
    local max_attempts=3
    local attempt=1
    local ruff_check_passed=false
    local ruff_format_passed=false
    
    while [ $attempt -le $max_attempts ]; do
        # Run Ruff formatter check and fix
        echo -e "${CYAN}Running Ruff formatter check (attempt $attempt/$max_attempts)${NC}"
        if [ -n "$target_dir" ]; then
            echo -e "${CYAN}  Checking: $check_path${NC}"
        fi
        if ruff format --check "$check_path" --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache > /dev/null 2>&1; then
            ruff_format_passed=true
            echo -e "${GREEN}✓ Ruff formatting passed${NC}"
        else
            echo -e "${YELLOW}⚠️  Formatting issues detected, attempting auto-fix...${NC}"
            ruff format "$check_path" --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}✓ Formatting auto-fixed${NC}"
            else
                echo -e "${RED}✗ Failed to auto-fix formatting${NC}"
            fi
        fi
        echo ""
        
        # Run Ruff linter check and fix
        echo -e "${CYAN}Running Ruff linter check (attempt $attempt/$max_attempts)${NC}"
        if [ -n "$target_dir" ]; then
            echo -e "${CYAN}  Checking: $check_path${NC}"
        fi
        if ruff check "$check_path" --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache > /dev/null 2>&1; then
            ruff_check_passed=true
            echo -e "${GREEN}✓ Ruff linting passed${NC}"
        else
            echo -e "${YELLOW}⚠️  Linting issues detected, attempting auto-fix...${NC}"
            ruff check --fix "$check_path" --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}✓ Linting auto-fixed${NC}"
            else
                echo -e "${RED}✗ Failed to auto-fix linting${NC}"
            fi
        fi
        echo ""
        
        # If both checks pass, break out of the loop
        if [ "$ruff_check_passed" = true ] && [ "$ruff_format_passed" = true ]; then
            break
        fi
        
        # If this wasn't the last attempt, try again
        if [ $attempt -lt $max_attempts ]; then
            echo -e "${YELLOW}Re-running checks after auto-fix...${NC}"
            echo ""
            ruff_check_passed=false
            ruff_format_passed=false
        fi
        
        attempt=$((attempt + 1))
    done
    
    # Final check - report failures if still present
    if [ "$ruff_check_passed" != true ]; then
        echo -e "${RED}✗ Ruff linting failed after $max_attempts attempts${NC}"
        # Show the actual errors
        ruff check "$check_path" --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache
        lint_errors=$((lint_errors + 1))
    fi
    
    if [ "$ruff_format_passed" != true ]; then
        echo -e "${RED}✗ Ruff formatting failed after $max_attempts attempts${NC}"
        # Show what would be reformatted
        ruff format --check "$check_path" --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache
        lint_errors=$((lint_errors + 1))
    fi
    echo ""
    
    # Run mypy type checker
    echo -e "${CYAN}Running mypy type checker${NC}"
    if [ -n "$target_dir" ]; then
        echo -e "${CYAN}  Checking: $check_path${NC}"
    fi
    if mypy "$check_path" --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache; then
        echo -e "${GREEN}✓ Type checking passed${NC}"
    else
        echo -e "${RED}✗ Type checking failed${NC}"
        lint_errors=$((lint_errors + 1))
    fi
    echo ""
    
    if [ $lint_errors -eq 0 ]; then
        echo -e "${GREEN}✅ All Python linting checks passed${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}❌ Python linting checks failed ($lint_errors error(s))${NC}"
        echo ""
        return 1
    fi
}

# Function to run Biome linting checks
# Usage: run_biome_linting [api_directory]
# If api_directory is provided, only checks that API directory (if TypeScript files exist)
run_biome_linting() {
    local target_dir="${1:-}"
    
    if [ -n "$target_dir" ]; then
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo -e "${BOLD}${BLUE}🔍 Running Biome Linting Checks: $target_dir${NC}"
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo ""
        
        # Check if there are TypeScript/JavaScript files in the API directory
        local api_path="$API_DIR/$target_dir"
        if [ ! -d "$api_path" ]; then
            echo -e "${YELLOW}⚠️  API directory not found: $api_path. Skipping Biome checks.${NC}"
            echo ""
            return 0
        fi
        
        # Check if there are any TS/JS files in this directory
        if ! find "$api_path" -type f \( -name "*.ts" -o -name "*.tsx" -o -name "*.js" -o -name "*.jsx" \) 2>/dev/null | grep -q .; then
            echo -e "${YELLOW}⚠️  No TypeScript/JavaScript files found in $target_dir. Skipping Biome checks.${NC}"
            echo ""
            return 0
        fi
    else
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo -e "${BOLD}${BLUE}🔍 Running Biome Linting Checks${NC}"
        echo -e "${BOLD}${BLUE}========================================${NC}"
        echo ""
    fi
    
    cd "$PROJECT_ROOT"
    
    # Check if biome is available
    if ! command -v biome &> /dev/null && ! command -v yarn &> /dev/null; then
        echo -e "${YELLOW}⚠️  Biome not found. Skipping Biome checks.${NC}"
        echo ""
        return 0
    fi
    
    # Determine what to check
    local check_target="."
    if [ -n "$target_dir" ]; then
        # Check the specific API directory relative to project root
        check_target="firebase/python_functions/api/$target_dir"
    fi
    
    # Try to run biome via yarn if available, otherwise direct command
    if command -v yarn &> /dev/null; then
        echo -e "${CYAN}Running Biome check via yarn${NC}"
        if [ -n "$target_dir" ]; then
            echo -e "${CYAN}  Checking: $check_target${NC}"
        fi
        if yarn biome check "$check_target"; then
            echo -e "${GREEN}✓ Biome linting passed${NC}"
            echo ""
            return 0
        else
            echo -e "${RED}✗ Biome linting failed${NC}"
            echo ""
            return 1
        fi
    elif command -v biome &> /dev/null; then
        echo -e "${CYAN}Running Biome check${NC}"
        if [ -n "$target_dir" ]; then
            echo -e "${CYAN}  Checking: $check_target${NC}"
        fi
        if biome check "$check_target"; then
            echo -e "${GREEN}✓ Biome linting passed${NC}"
            echo ""
            return 0
        else
            echo -e "${RED}✗ Biome linting failed${NC}"
            echo ""
            return 1
        fi
    else
        echo -e "${YELLOW}⚠️  Biome not found. Skipping Biome checks.${NC}"
        echo ""
        return 0
    fi
}

# Arrays to track results (using parallel arrays instead of associative array)
api_names=()
api_results=()

# Run linting checks before tests
LINT_ERRORS=0

# Determine if we should run linting for a specific API or the whole project
if [ -n "$ONLY_MATCH" ]; then
    # Use the CACHE_TARGET_DIR we already determined
    MATCHING_API=""
    if [ -n "$CACHE_TARGET_DIR" ]; then
        # Check if it's a subapi by looking for the directory structure
        if [ -d "$API_DIR/subapi/$CACHE_TARGET_DIR" ]; then
            MATCHING_API="subapi/$CACHE_TARGET_DIR"
        else
            MATCHING_API="$CACHE_TARGET_DIR"
        fi
    fi
    
    if [ -n "$MATCHING_API" ]; then
        # Run linting checks for the specific API
        echo -e "${CYAN}Running linting checks for API: $MATCHING_API${NC}"
        echo ""
        
        # Run Python linting checks for this API
        if ! run_python_linting "$MATCHING_API"; then
            LINT_ERRORS=$((LINT_ERRORS + 1))
        fi
        
        # Run Biome linting checks for this API
        if ! run_biome_linting "$MATCHING_API"; then
            LINT_ERRORS=$((LINT_ERRORS + 1))
        fi
    else
        echo -e "${YELLOW}⚠️  No matching API found for '$ONLY_MATCH'. Skipping linting checks.${NC}"
        echo ""
    fi
else
    # Run linting checks for the whole project
    # Run Python linting checks
    if ! run_python_linting; then
        LINT_ERRORS=$((LINT_ERRORS + 1))
    fi
    
    # Run Biome linting checks
    if ! run_biome_linting; then
        LINT_ERRORS=$((LINT_ERRORS + 1))
    fi
fi

# Exit early if linting failed (unless --continue is set)
if [ $LINT_ERRORS -gt 0 ] && [ "$CONTINUE_ON_FAILURE" = false ]; then
    echo -e "${BOLD}${RED}========================================${NC}"
    echo -e "${BOLD}${RED}Linting checks failed. Stopping test run.${NC}"
    echo -e "${BOLD}${RED}Use --continue flag to run tests anyway${NC}"
    echo -e "${BOLD}${RED}========================================${NC}"
    exit 1
fi

# Set exit on error for test execution
set -e  # Exit on first error

# Find all API directories with run_tests.sh scripts
echo -e "${BOLD}${BLUE}========================================${NC}"
echo -e "${BOLD}${BLUE}🧪 Running All API Test Suites${NC}"
echo -e "${BOLD}${BLUE}========================================${NC}"
echo ""

# Discover all APIs with test scripts (both APIs and non-deprecated subapis)
for api_path in "$API_DIR"/*/bin/run_tests.sh; do
    if [ -f "$api_path" ]; then
        api_name=$(basename $(dirname $(dirname "$api_path")))
        api_names+=("$api_name")
    fi
done

# Discover non-deprecated subapis (skip those starting with _)
for api_path in "$API_DIR/subapi"/*/bin/run_tests.sh; do
    if [ -f "$api_path" ]; then
        api_name=$(basename $(dirname $(dirname "$api_path")))
        # Skip deprecated APIs (those starting with _)
        if [[ "$api_name" != _* ]]; then
            api_names+=("subapi/$api_name")
        fi
    fi
done

# Sort API names alphabetically
IFS=$'\n' api_names=($(sort <<<"${api_names[*]}"))
unset IFS

echo -e "${CYAN}Found ${#api_names[@]} API modules with test suites:${NC}"
for api_name in "${api_names[@]}"; do
    echo -e "  • $api_name"
done
echo ""

# If --start-from is specified, show which API we'll start from
if [ -n "$START_FROM" ]; then
    echo -e "${YELLOW}⏩ Will skip tests until API name contains: '$START_FROM'${NC}"
    echo ""
fi

# If --only is specified, show which APIs will be run
if [ -n "$ONLY_MATCH" ]; then
    echo -e "${CYAN}🎯 Will run ONLY APIs whose name contains: '$ONLY_MATCH'${NC}"
    echo ""
fi

# Counter for summary
total_apis=${#api_names[@]}
passed_count=0
failed_count=0
skipped_count=0
start_time=$(date +%s)
found_start=false

# If no --start-from specified, start immediately
if [ -z "$START_FROM" ]; then
    found_start=true
fi

# Run tests for each API
for api_name in "${api_names[@]}"; do
    # If --only is specified, skip APIs that don't match
    if [ -n "$ONLY_MATCH" ]; then
        # Extract the base name for matching (remove "subapi/" prefix if present)
        if [[ "$api_name" == subapi/* ]]; then
            base_name="${api_name#subapi/}"
        else
            base_name="$api_name"
        fi
        
        if [[ "$base_name" != *"$ONLY_MATCH"* ]]; then
            echo -e "${YELLOW}⏩ Skipping: $api_name (does not match '$ONLY_MATCH')${NC}"
            api_results+=("SKIPPED")
            ((skipped_count++))
            continue
        fi
    fi
    
    # Check if we should start testing from this API
    if [ "$found_start" = false ]; then
        # Extract the base name for matching (remove "subapi/" prefix if present)
        if [[ "$api_name" == subapi/* ]]; then
            base_name="${api_name#subapi/}"
        else
            base_name="$api_name"
        fi
        
        if [[ "$base_name" == *"$START_FROM"* ]]; then
            found_start=true
            echo -e "${GREEN}✓ Found '$START_FROM' in '$api_name' - starting tests from here${NC}"
            echo ""
        else
            echo -e "${YELLOW}⏩ Skipping: $api_name${NC}"
            api_results+=("SKIPPED")
            ((skipped_count++))
            continue
        fi
    fi
    echo ""
    echo -e "${BOLD}${BLUE}========================================${NC}"
    echo -e "${BOLD}${CYAN}Testing: $api_name${NC}"
    echo -e "${BOLD}${BLUE}========================================${NC}"
    echo ""
    
    # Build test script path (works for both APIs and subapis since api_name includes "subapi/" prefix when needed)
    test_script="$API_DIR/$api_name/bin/run_tests.sh"
    
    if [ ! -f "$test_script" ]; then
        echo -e "${RED}❌ Test script not found: $test_script${NC}"
        api_results+=("NOT_FOUND")
        ((failed_count++))
        continue
    fi
    
    # Make sure the script is executable
    chmod +x "$test_script"
    
    # Run the test script
    if bash "$test_script"; then
        api_results+=("PASSED")
        ((passed_count++))
        echo ""
        echo -e "${GREEN}✅ $api_name tests PASSED${NC}"
    else
        api_results+=("FAILED")
        ((failed_count++))
        echo ""
        echo -e "${RED}❌ $api_name tests FAILED${NC}"
        
        # Exit immediately unless --continue flag is set
        if [ "$CONTINUE_ON_FAILURE" = false ]; then
            echo ""
            echo -e "${BOLD}${RED}========================================${NC}"
            echo -e "${BOLD}${RED}Test run stopped due to failure${NC}"
            echo -e "${BOLD}${RED}Use --continue flag to run all tests${NC}"
            echo -e "${BOLD}${RED}========================================${NC}"
            exit 1
        fi
    fi
done

end_time=$(date +%s)
duration=$((end_time - start_time))

# Print summary
echo ""
echo ""
echo -e "${BOLD}${BLUE}========================================${NC}"
echo -e "${BOLD}${BLUE}📊 Test Summary${NC}"
echo -e "${BOLD}${BLUE}========================================${NC}"
echo ""

# Display results using parallel array indices
for i in "${!api_names[@]}"; do
    api_name="${api_names[$i]}"
    result="${api_results[$i]}"
    
    if [ "$result" == "PASSED" ]; then
        echo -e "  ${GREEN}✅ $api_name${NC}"
    elif [ "$result" == "FAILED" ]; then
        echo -e "  ${RED}❌ $api_name${NC}"
    elif [ "$result" == "SKIPPED" ]; then
        echo -e "  ${YELLOW}⏩ $api_name (skipped)${NC}"
    else
        echo -e "  ${YELLOW}⚠️  $api_name (not found)${NC}"
    fi
done

echo ""
echo -e "${BOLD}Results:${NC}"
if [ $LINT_ERRORS -gt 0 ]; then
    echo -e "  ${RED}Linting Errors: $LINT_ERRORS${NC}"
fi
echo -e "  Total APIs: $total_apis"
if [ $skipped_count -gt 0 ]; then
    echo -e "  ${YELLOW}Skipped: $skipped_count${NC}"
fi
echo -e "  ${GREEN}Passed: $passed_count${NC}"
echo -e "  ${RED}Failed: $failed_count${NC}"
echo -e "  Duration: ${duration}s"
echo ""

# Exit with error if linting failed or tests failed
if [ $LINT_ERRORS -gt 0 ] || [ $failed_count -gt 0 ]; then
    echo -e "${BOLD}${RED}========================================${NC}"
    if [ $LINT_ERRORS -gt 0 ] && [ $failed_count -gt 0 ]; then
        echo -e "${BOLD}${RED}❌ Linting checks and API tests failed${NC}"
    elif [ $LINT_ERRORS -gt 0 ]; then
        echo -e "${BOLD}${RED}❌ Linting checks failed${NC}"
    else
        echo -e "${BOLD}${RED}❌ Some API tests failed${NC}"
    fi
    echo -e "${BOLD}${RED}========================================${NC}"
    exit 1
else
    echo -e "${BOLD}${GREEN}========================================${NC}"
    echo -e "${BOLD}${GREEN}🎉 All linting checks and API tests passed!${NC}"
    echo -e "${BOLD}${GREEN}========================================${NC}"
    exit 0
fi
