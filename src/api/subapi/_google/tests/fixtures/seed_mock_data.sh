#!/bin/bash
#
# Seed Mock Data - Generate test fixtures from real API responses
#
# This script runs the generate_mock_data.py script to fetch real Google Books API
# data and save as JSON fixtures for testing.
#
# Usage:
#   ./seed_mock_data.sh [type]
#
# Arguments:
#   type  Optional. Specify 'endpoints', 'core', 'search', or 'all'.
#         If omitted, generates search fixtures (default).
#
# Requirements:
#   Environment variable (optional but recommended):
#   - GOOGLE_BOOK_API_KEY
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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTS_DIR="$(dirname "$SCRIPT_DIR")"

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
        "$SCRIPT_DIR/../../../../../.env"
        "$SCRIPT_DIR/../../../../.env"
        "$SCRIPT_DIR/../../../.env"
        "$SCRIPT_DIR/../../.env"
        "$SCRIPT_DIR/.env"
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

    print_warning "No .env file found"
    print_info "Checking for environment variables..."
    return 1
}

# Function to seed Google Books fixtures
seed_google_books_fixtures() {
    local type="${1:-search}"
    
    print_header "Seeding Google Books Mock Data ($type)"

    # Check for API key (optional)
    if ! check_env_var "GOOGLE_BOOK_API_KEY"; then
        print_warning "GOOGLE_BOOK_API_KEY environment variable not set"
        print_info "API requests may be rate limited without a key"
        print_info "Get your key from: https://console.cloud.google.com/apis/credentials"
        print_info "Then add to .env file or run: export GOOGLE_BOOK_API_KEY='your_key_here'"
        print_info "Continuing without API key..."
    else
        print_success "GOOGLE_BOOK_API_KEY is set"
    fi

    # Navigate to tests directory
    cd "$TESTS_DIR"

    # Check if generate_mock_data.py exists
    if [ ! -f "fixtures/generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $TESTS_DIR/fixtures"
        return 1
    fi

    # Run generator based on type
    case "$type" in
        endpoints)
            print_info "Fetching real data from Google Books API (endpoints)..."
            print_info "This may take 10-30 seconds..."
            
            if python fixtures/generate_mock_data.py --endpoints; then
                print_success "Google Books API endpoint mock data seeded successfully"
                
                # Show what was generated
                if [ -d "fixtures/make_requests" ]; then
                    local fixture_count=$(ls -1 fixtures/make_requests/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count API endpoint files in make_requests/"
                fi
                return 0
            else
                print_error "Failed to seed Google Books API endpoint mock data"
                return 1
            fi
            ;;
        
        core)
            print_info "Generating core method results..."
            
            if python fixtures/generate_mock_data.py --core; then
                print_success "Google Books core method mock data seeded successfully"
                
                # Show what was generated
                if [ -d "fixtures/core" ]; then
                    local fixture_count=$(ls -1 fixtures/core/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count core method files in core/"
                fi
                return 0
            else
                print_error "Failed to seed Google Books core method mock data"
                return 1
            fi
            ;;
        
        search)
            print_info "Generating search method results..."
            
            if python fixtures/generate_mock_data.py --search; then
                print_success "Google Books search method mock data seeded successfully"
                
                # Show what was generated
                if [ -d "fixtures/search" ]; then
                    local fixture_count=$(ls -1 fixtures/search/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count search method files in search/"
                fi
                return 0
            else
                print_error "Failed to seed Google Books search method mock data"
                return 1
            fi
            ;;
        
        all)
            print_info "Generating all mock data (endpoints, core, search)..."
            print_info "This may take 30-60 seconds..."
            
            if python fixtures/generate_mock_data.py --all; then
                print_success "All Google Books mock data seeded successfully"
                
                # Show what was generated
                local total_count=0
                if [ -d "fixtures/make_requests" ]; then
                    local count=$(ls -1 fixtures/make_requests/*.json 2>/dev/null | wc -l)
                    print_success "Generated $count API endpoint files in make_requests/"
                    total_count=$((total_count + count))
                fi
                if [ -d "fixtures/core" ]; then
                    local count=$(ls -1 fixtures/core/*.json 2>/dev/null | wc -l)
                    print_success "Generated $count core method files in core/"
                    total_count=$((total_count + count))
                fi
                if [ -d "fixtures/search" ]; then
                    local count=$(ls -1 fixtures/search/*.json 2>/dev/null | wc -l)
                    print_success "Generated $count search method files in search/"
                    total_count=$((total_count + count))
                fi
                print_success "Total: $total_count fixture files generated"
                return 0
            else
                print_error "Failed to seed Google Books mock data"
                return 1
            fi
            ;;
        
        *)
            print_error "Unknown type: $type"
            print_info "Usage: $0 [endpoints|core|search|all]"
            return 1
            ;;
    esac
}

# Function to show environment status
show_env_status() {
    print_header "Environment Variables Status"

    echo "Google Books:"
    if check_env_var "GOOGLE_BOOK_API_KEY"; then
        print_success "GOOGLE_BOOK_API_KEY is set"
    else
        print_warning "GOOGLE_BOOK_API_KEY is NOT set (optional)"
    fi
}

# Main script
main() {
    print_header "Seed Mock Data - Generate Test Fixtures"

    # Try to load environment from .env file
    load_env_from_file

    # Show environment status
    show_env_status

    # Determine which type to seed
    local type="${1:-search}"

    seed_google_books_fixtures "$type"
    exit_code=$?

    # Summary
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_header "✓ Mock Data Seeding Complete"
        print_success "All fixtures generated successfully!"
        print_info "Fixtures saved in fixtures/"
        echo ""
        print_info "Next steps:"
        echo "  1. Review generated fixtures"
        echo "  2. Run tests: cd .. && pytest -v"
        echo "  3. Regenerate anytime by running this script again"
    else
        print_header "✗ Mock Data Seeding Failed"
        print_error "Some fixtures failed to generate"
        echo ""
        print_info "Troubleshooting:"
        echo "  1. Check environment variables (API key is optional)"
        echo "  2. Verify you have internet connection"
        echo "  3. Ensure API credentials are valid (if using API key)"
        echo "  4. Check API is accessible (not rate-limited)"
    fi

    return $exit_code
}

# Run main function
main "$@"

