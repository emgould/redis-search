#!/bin/bash
#
# Seed Mock Data - Generate test fixtures from real API responses
#
# This script runs the generate_mock_data.py script to fetch real API data
# and save as JSON fixtures for testing.
#
# Usage:
#   ./seed_mock_data.sh
#
# Requirements:
#   Environment variables must be set (or in .env file):
#   - WATCHMODE_API_KEY (for Watchmode)
#   - TMDB_READ_TOKEN (for TMDB, used in whats_new wrapper)
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../../.. && pwd)"

# Function to print colored output
print_header() {
    echo -e "\n${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# Function to check if environment variable is set
check_env_var() {
    local var_name=$1
    if [ -z "${!var_name}" ]; then
        return 1
    fi
    return 0
}

# Function to load environment from .env file
load_env_from_file() {
    # Look for .env file in multiple locations
    local env_files=(
        "$SCRIPT_DIR/../../.env"
        "$SCRIPT_DIR/../.env"
        "$SCRIPT_DIR/.env"
        "$SCRIPT_DIR/../../.secret.local"
        "$SCRIPT_DIR/../.secret.local"
    )

    for env_file in "${env_files[@]}"; do
        if [ -f "$env_file" ]; then
            print_info "Loading environment from: $env_file"
            
            # Export variables from file
            while IFS= read -r line || [[ -n "$line" ]]; do
                # Skip empty lines and comments
                if [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]]; then
                    continue
                fi

                # Export variable if it contains =
                if [[ "$line" =~ = && ! "$line" =~ ^# ]]; then
                    export "$line"
                fi
            done < "$env_file"

            print_success "Environment loaded from $env_file"
            return 0
        fi
    done

    print_warning "No .env or .secret.local file found"
    print_info "Checking for environment variables..."
    return 1
}

# Function to seed Watchmode fixtures
seed_watchmode_fixtures() {
    print_header "Seeding Watchmode Mock Data"

    # Check for Watchmode API key
    if ! check_env_var "WATCHMODE_API_KEY"; then
        print_error "WATCHMODE_API_KEY environment variable not set"
        print_info "Get your key from: https://api.watchmode.com/"
        print_info "Then add to .env file or run: export WATCHMODE_API_KEY='your_key_here'"
        return 1
    fi

    print_success "WATCHMODE_API_KEY is set"

    # Activate virtual environment if it exists
    local python_functions_dir="$SCRIPT_DIR/../.."
    if [ -d "$python_functions_dir/venv" ]; then
        print_info "Activating virtual environment..."
        source "$python_functions_dir/venv/bin/activate"
        print_success "Virtual environment activated"
    elif [ -d "$python_functions_dir/.venv" ]; then
        print_info "Activating virtual environment..."
        source "$python_functions_dir/.venv/bin/activate"
        print_success "Virtual environment activated"
    else
        print_warning "No virtual environment found (venv or .venv)"
        print_info "Continuing without venv activation..."
    fi

    # Navigate to Watchmode tests directory
    local watchmode_dir="$SCRIPT_DIR/watchmode/tests"
    
    if [ ! -d "$watchmode_dir" ]; then
        print_error "Watchmode tests directory not found: $watchmode_dir"
        return 1
    fi

    cd "$watchmode_dir"

    # Check if generate_mock_data.py exists
    if [ ! -f "fixtures/generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $watchmode_dir"
        return 1
    fi

    # Run generator for API endpoints
    print_info "Fetching real data from Watchmode API (endpoints)..."
    print_info "This may take 10-30 seconds..."
    
    if python fixtures/generate_mock_data.py --endpoints; then
        print_success "Watchmode API endpoint mock data seeded successfully"
        
        # Show what was generated
        if [ -d "fixtures/make_requests" ]; then
            local fixture_count=$(ls -1 fixtures/make_requests/*.json 2>/dev/null | wc -l)
            print_success "Generated $fixture_count API endpoint files in make_requests/"
        fi
    else
        print_error "Failed to seed Watchmode API endpoint mock data"
        return 1
    fi
    
    # Run generator for core methods
    print_info "Generating core method results..."
    
    if python fixtures/generate_mock_data.py --core; then
        print_success "Watchmode core method mock data seeded successfully"
        
        # Show what was generated
        if [ -d "fixtures/core" ]; then
            local fixture_count=$(ls -1 fixtures/core/*.json 2>/dev/null | wc -l)
            print_success "Generated $fixture_count core method files in core/"
        fi
        
        return 0
    else
        print_error "Failed to seed Watchmode core method mock data"
        return 1
    fi
}

# Function to show environment status
show_env_status() {
    print_header "Environment Variables Status"

    echo "Watchmode:"
    if check_env_var "WATCHMODE_API_KEY"; then
        print_success "WATCHMODE_API_KEY is set"
    else
        print_error "WATCHMODE_API_KEY is NOT set"
    fi

    echo -e "\nTMDB (optional, for wrapper functions):"
    if check_env_var "TMDB_READ_TOKEN"; then
        print_success "TMDB_READ_TOKEN is set"
    else
        print_warning "TMDB_READ_TOKEN is NOT set (needed for whats_new wrapper)"
    fi
}

# Main script
main() {
    print_header "Seed Mock Data - Generate Test Fixtures"

    # Try to load environment from .env file
    load_env_from_file

    # Show environment status
    show_env_status

    # Seed Watchmode fixtures
    seed_watchmode_fixtures
    exit_code=$?

    # Summary
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_header "✓ Mock Data Seeding Complete"
        print_success "All fixtures generated successfully!"
        print_info "Fixtures saved in services/watchmode/tests/fixtures/"
        echo ""
        print_info "Next steps:"
        echo "  1. Review generated fixtures"
        echo "  2. Run tests: cd ../.. && ./bin/run_tests.sh"
        echo "  3. Regenerate anytime by running this script again"
    else
        print_header "✗ Mock Data Seeding Failed"
        print_error "Some fixtures failed to generate"
        echo ""
        print_info "Troubleshooting:"
        echo "  1. Check environment variables are set correctly"
        echo "  2. Verify you have internet connection"
        echo "  3. Ensure API credentials are valid"
        echo "  4. Check APIs are accessible (not rate-limited)"
    fi

    return $exit_code
}

# Run main function
main "$@"
