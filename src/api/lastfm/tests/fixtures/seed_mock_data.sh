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

# Function to seed LastFM fixtures
seed_lastfm_fixtures() {
    print_header "Seeding LastFM Mock Data"

    # Check for required environment variables
    local missing_vars=()

    if ! check_env_var "LASTFM_API_KEY"; then
        missing_vars+=("LASTFM_API_KEY")
    fi

    if ! check_env_var "SPOTIFY_CLIENT_ID"; then
        missing_vars+=("SPOTIFY_CLIENT_ID")
    fi

    if ! check_env_var "SPOTIFY_CLIENT_SECRET"; then
        missing_vars+=("SPOTIFY_CLIENT_SECRET")
    fi

    if [ ${#missing_vars[@]} -gt 0 ]; then
        print_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        print_info "Get Last.fm API key from: https://www.last.fm/api/account/create"
        print_info "Get Spotify credentials from: https://developer.spotify.com/dashboard"
        print_info "Then add to .env file or run:"
        echo "  export LASTFM_API_KEY='your_key_here'"
        echo "  export SPOTIFY_CLIENT_ID='your_id_here'"
        echo "  export SPOTIFY_CLIENT_SECRET='your_secret_here'"
        return 1
    fi

    print_success "All required environment variables are set"

    # Navigate to LastFM tests directory

    local lastfm_dir="$SCRIPT_DIR/lastfm/tests"
    
    if [ ! -d "$lastfm_dir" ]; then
        print_error "LastFM tests directory not found: $lastfm_dir"
        return 1
    fi

    cd "$lastfm_dir"

    # Check if generate_mock_data.py exists
    if [ ! -f "fixtures/generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $lastfm_dir"
        return 1
    fi

    # Run generator for all fixture types
    print_info "Fetching real data from Last.fm and Spotify APIs..."
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
    
    # Generate enrichment method results
    print_info "Generating enrichment method results (enrichment/)..."
    if python fixtures/generate_mock_data.py --enrichment; then
        print_success "Enrichment method results generated"
    else
        print_error "Failed to generate enrichment method results"
        all_success=false
    fi
    
    # Generate search method results
    print_info "Generating search method results (search/)..."
    if python fixtures/generate_mock_data.py --search; then
        print_success "Search method results generated"
    else
        print_error "Failed to generate search method results"
        all_success=false
    fi
    
    # Generate model validation data
    print_info "Generating model validation data (models/)..."
    if python fixtures/generate_mock_data.py --models; then
        print_success "Model validation data generated"
    else
        print_error "Failed to generate model validation data"
        all_success=false
    fi
    
    if [ "$all_success" = true ]; then
        print_success "LastFM mock data seeded successfully"
        
        # Show what was generated
        print_info "Generated fixtures in:"
        for dir in fixtures/make_requests fixtures/core fixtures/enrichment fixtures/search fixtures/models; do
            if [ -d "$dir" ]; then
                local count=$(ls -1 "$dir"/*.json 2>/dev/null | wc -l | tr -d ' ')
                if [ "$count" -gt 0 ]; then
                    echo "  - $dir: $count files"
                fi
            fi
        done
        
        return 0
    else
        print_error "Failed to seed some LastFM mock data"
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
    seed_lastfm_fixtures
    exit_code=$?

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

