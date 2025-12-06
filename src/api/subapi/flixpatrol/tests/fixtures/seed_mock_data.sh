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
#   - TMDB_TOKEN (optional, for enriched data)
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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../../../ && pwd)"
echo "SCRIPT_DIR: $SCRIPT_DIR"

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

# Function to seed FlixPatrol fixtures
seed_flixpatrol_fixtures() {
    print_header "Seeding FlixPatrol Mock Data"

    # No API keys required for FlixPatrol scraping
    print_success "No API keys required"

    # Navigate to python_functions directory first to activate venv
    local python_functions_dir="$SCRIPT_DIR/.."
    cd "$python_functions_dir"
    
    # Activate virtual environment
    if [ -d "venv" ]; then
        print_info "Activating virtual environment..."
        source venv/bin/activate
    elif [ -d ".venv" ]; then
        print_info "Activating virtual environment..."
        source .venv/bin/activate
    else
        print_error "No virtual environment found (venv or .venv)"
        return 1
    fi
    
    # Navigate to FlixPatrol tests directory
    local flixpatrol_dir="$SCRIPT_DIR/flixpatrol/tests"
    
    if [ ! -d "$flixpatrol_dir" ]; then
        print_error "FlixPatrol tests directory not found: $flixpatrol_dir"
        return 1
    fi

    cd "$flixpatrol_dir"

    # Check if generate_mock_data.py exists
    if [ ! -f "fixtures/generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $flixpatrol_dir"
        return 1
    fi

    # Run generator for all fixture types
    print_info "Fetching real data from FlixPatrol..."
    print_info "This may take 30-60 seconds..."
    
    local all_success=true
    
    # Generate API endpoint responses
    print_info "Generating API endpoint responses (make_requests/)..."
    if python fixtures/generate_mock_data.py --endpoints; then
        print_success "API endpoint responses generated"
    else
        print_error "Failed to generate API endpoint responses"
        all_success=false
    fi
    
    # Generate core method results
    print_info "Generating core method results (core/)..."
    if python fixtures/generate_mock_data.py --core; then
        print_success "Core method results generated"
    else
        print_error "Failed to generate core method results"
        all_success=false
    fi
    
    if [ "$all_success" = true ]; then
        print_success "FlixPatrol mock data seeded successfully"
        
        # Show what was generated
        print_info "Generated fixtures in:"
        for dir in fixtures/make_requests fixtures/core; do
            if [ -d "$dir" ]; then
                local count=$(ls -1 "$dir"/*.json 2>/dev/null | wc -l | tr -d ' ')
                if [ "$count" -gt 0 ]; then
                    echo "  - $dir: $count files"
                fi
            fi
        done
        
        return 0
    else
        print_error "Failed to seed some FlixPatrol mock data"
        return 1
    fi
}

# Function to show environment status
show_env_status() {
    print_header "Environment Variables Status"

    echo "FlixPatrol:"
    print_info "No API key required for FlixPatrol (scrapes public data)"
    print_success "Ready to generate fixtures"
}

# Main script
main() {
    print_header "Seed Mock Data - Generate Test Fixtures"

    # Try to load environment from .env file
    load_env_from_file

    # Show environment status
    show_env_status

    # Seed FlixPatrol fixtures
    seed_flixpatrol_fixtures
    exit_code=$?

    # Summary
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_header "✓ Mock Data Seeding Complete"
        print_success "All fixtures generated successfully!"
        print_info "Fixtures saved in services/flixpatrol/tests/fixtures/"
        echo ""
        print_info "Next steps:"
        echo "  1. Review generated fixtures"
        echo "  2. Run tests: cd .. && pytest services/flixpatrol/tests/ -v"
        echo "  3. Regenerate anytime by running this script again"
    else
        print_header "✗ Mock Data Seeding Failed"
        print_error "Some fixtures failed to generate"
        echo ""
        print_info "Troubleshooting:"
        echo "  1. Check environment variables are set correctly"
        echo "  2. Verify you have internet connection"
        echo "  3. Ensure FlixPatrol website is accessible"
        echo "  4. Check TMDB API credentials are valid (if using enrichment)"
    fi

    return $exit_code
}

# Run main function
main "$@"

