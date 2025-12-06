#!/bin/bash
#
# Seed Mock Data - Generate test fixtures from real API responses
#
# This script runs the generate_mock_data.py scripts for all services
# to fetch real API data and save as JSON fixtures for testing.
#
# Usage:
#   ./seed_mock_data.sh [service]
#
# Arguments:
#   service  Optional. Specify 'tmdb' or 'lastfm' to generate only that service.
#            If omitted, generates all api.
#
# Requirements:
#   Environment variables must be set (or in .env file):
#   - TMDB_READ_TOKEN (for TMDB)
#   - LASTFM_API_KEY (for LastFM)
#   - SPOTIFY_CLIENT_ID (for LastFM/Spotify integration)
#   - SPOTIFY_CLIENT_SECRET (for LastFM/Spotify integration)
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

# Function to seed TMDB fixtures
seed_tmdb_fixtures() {
    print_header "Seeding TMDB Mock Data"

    # Check for TMDB token
    if ! check_env_var "TMDB_READ_TOKEN"; then
        print_error "TMDB_READ_TOKEN environment variable not set"
        print_info "Get your token from: https://www.themoviedb.org/settings/api"
        print_info "Then add to .env file or run: export TMDB_READ_TOKEN='your_token_here'"
        return 1
    fi

    print_success "TMDB_READ_TOKEN is set"

    # Navigate to TMDB tests directory
    local tmdb_dir="$SCRIPT_DIR/tmdb/tests"
    
    if [ ! -d "$tmdb_dir" ]; then
        print_error "TMDB tests directory not found: $tmdb_dir"
        return 1
    fi

    cd "$tmdb_dir"

    # Check if generate_mock_data.py exists
    if [ ! -f "fixtures/generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $tmdb_dir"
        return 1
    fi

    # Run generator for API endpoints
    print_info "Fetching real data from TMDB API (endpoints)..."
    print_info "This may take 10-30 seconds..."
    
    if python fixtures/generate_mock_data.py --search; then
        print_success "TMDB API endpoint mock data seeded successfully"
        
        # Show what was generated
        if [ -d "fixtures/make_requests" ]; then
            local fixture_count=$(ls -1 fixtures/make_requests/*.json 2>/dev/null | wc -l)
            print_success "Generated $fixture_count API endpoint files in make_requests/"
        fi
    else
        print_error "Failed to seed TMDB API endpoint mock data"
        return 1
    fi
    
    # Run generator for core methods
    print_info "Generating core method results..."
    
    if python fixtures/generate_mock_data.py --core; then
        print_success "TMDB core method mock data seeded successfully"
        
        # Show what was generated
        if [ -d "fixtures/core" ]; then
            local fixture_count=$(ls -1 fixtures/core/*.json 2>/dev/null | wc -l)
            print_success "Generated $fixture_count core method files in core/"
        fi
    else
        print_error "Failed to seed TMDB core method mock data"
        return 1
    fi
    
    # Run generator for person methods
    print_info "Generating person method results..."
    
    if python fixtures/generate_mock_data.py --all; then
        print_success "TMDB person method mock data seeded successfully"
        
        # Show what was generated
        if [ -d "fixtures/person" ]; then
            local fixture_count=$(ls -1 fixtures/person/*.json 2>/dev/null | wc -l)
            print_success "Generated $fixture_count person method files in person/"
        fi
        
        return 0
    else
        print_error "Failed to seed TMDB person method mock data"
        return 1
    fi
}

# Function to show environment status
show_env_status() {
    print_header "Environment Variables Status"

    echo "TMDB:"
    if check_env_var "TMDB_READ_TOKEN"; then
        print_success "TMDB_READ_TOKEN is set"
    else
        print_error "TMDB_READ_TOKEN is NOT set"
    fi

    echo -e "\nLastFM & Spotify:"
    if check_env_var "LASTFM_API_KEY"; then
        print_success "LASTFM_API_KEY is set"
    else
        print_error "LASTFM_API_KEY is NOT set"
    fi

    if check_env_var "SPOTIFY_CLIENT_ID"; then
        print_success "SPOTIFY_CLIENT_ID is set"
    else
        print_error "SPOTIFY_CLIENT_ID is NOT set"
    fi

    if check_env_var "SPOTIFY_CLIENT_SECRET"; then
        print_success "SPOTIFY_CLIENT_SECRET is set"
    else
        print_error "SPOTIFY_CLIENT_SECRET is NOT set"
    fi
}

# Main script
main() {
    print_header "Seed Mock Data - Generate Test Fixtures"

    # Try to load environment from .env file
    load_env_from_file

    # Show environment status
    show_env_status

    # Determine which service to seed
    local service="${1:-all}"

    case "$service" in
        tmdb)
            seed_tmdb_fixtures
            exit_code=$?
            ;;        
        all)
            tmdb_result=0
            
            seed_tmdb_fixtures || tmdb_result=$?
            
            if [ $tmdb_result -eq 0 ]; then
                exit_code=0
            else
                exit_code=1
            fi
            ;;
        *)
            print_error "Unknown service: $service"
            print_info "Usage: $0 [tmdb|all]"
            exit 1
            ;;
    esac

    # Summary
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_header "✓ Mock Data Seeding Complete"
        print_success "All fixtures generated successfully!"
        print_info "Fixtures saved in services/*/tests/fixtures/"
        echo ""
        print_info "Next steps:"
        echo "  1. Review generated fixtures"
        echo "  2. Run tests: cd .. && pytest services/tmdb/tests/ -v"
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
        echo ""
        print_info "For help, see: QUICKSTART.txt"
    fi

    return $exit_code
}

# Run main function
main "$@"

