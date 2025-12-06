#!/bin/bash
#
# Seed Mock Data - Generate test fixtures from real PodcastIndex API responses
#
# This script runs the generate_mock_data.py script to fetch real API data
# and save as JSON fixtures for testing.
#
# Usage:
#   ./seed_mock_data.sh [--all|--search]
#
# Arguments:
#   --all     Generate all mock data (default)
#   --search  Generate search method results only
#
# Requirements:
#   - Internet connection to access PodcastIndex API
#   - PODCASTINDEX_API_KEY environment variable must be set
#   - PODCASTINDEX_API_SECRET environment variable must be set
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
        "$SCRIPT_DIR/../../../../../../.env"
        "$SCRIPT_DIR/../../../../../.env"
        "$SCRIPT_DIR/../../../../.env"
        "$SCRIPT_DIR/../../../.env"
        "$SCRIPT_DIR/../../.env"
        "$SCRIPT_DIR/.env"
        "$SCRIPT_DIR/../../../../../../.secret.local"
        "$SCRIPT_DIR/../../../../../.secret.local"
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
                    # Remove quotes and trim whitespace
                    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed 's/"\(.*\)"/\1/')
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

# Function to check API credentials
check_credentials() {
    local missing_vars=()

    if ! check_env_var "PODCASTINDEX_API_KEY"; then
        missing_vars+=("PODCASTINDEX_API_KEY")
    fi

    if ! check_env_var "PODCASTINDEX_API_SECRET"; then
        missing_vars+=("PODCASTINDEX_API_SECRET")
    fi

    if [ ${#missing_vars[@]} -gt 0 ]; then
        print_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        print_info "Get PodcastIndex API credentials from: https://api.podcastindex.org/"
        print_info "Then add to .env file or run:"
        echo "  export PODCASTINDEX_API_KEY='your_key_here'"
        echo "  export PODCASTINDEX_API_SECRET='your_secret_here'"
        return 1
    fi

    print_success "All required environment variables are set"
    return 0
}

# Function to seed Podcast fixtures
seed_podcast_fixtures() {
    local mode="${1:-all}"
    
    print_header "Seeding Podcast Mock Data"

    # Navigate to fixtures directory
    cd "$SCRIPT_DIR"

    # Check if generate_mock_data.py exists
    if [ ! -f "generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $SCRIPT_DIR"
        return 1
    fi

    # Check credentials
    if ! check_credentials; then
        return 1
    fi

    case "$mode" in
        search)
            print_info "Generating search method results..."
            print_info "This may take a few seconds..."
            
            if python generate_mock_data.py --search; then
                print_success "Podcast search method mock data seeded successfully"
                
                # Show what was generated
                if [ -d "search" ]; then
                    local fixture_count=$(ls -1 search/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count search method files in search/"
                fi
                return 0
            else
                print_error "Failed to seed Podcast search method mock data"
                return 1
            fi
            ;;
        
        all)
            print_info "Generating all mock data..."
            print_info "This may take a few seconds..."
            
            if python generate_mock_data.py --all; then
                print_success "All Podcast mock data seeded successfully"
                
                # Show what was generated
                echo ""
                if [ -d "search" ]; then
                    local fixture_count=$(ls -1 search/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count search method files in search/"
                fi
                
                return 0
            else
                print_error "Failed to seed Podcast mock data"
                return 1
            fi
            ;;
        
        *)
            print_error "Unknown mode: $mode"
            print_info "Usage: $0 [--all|--search]"
            return 1
            ;;
    esac
}

# Main script
main() {
    print_header "Seed Podcast Mock Data - Generate Test Fixtures"

    # Try to load environment from .env file
    load_env_from_file

    # Determine which mode to use
    local mode="all"
    
    if [ "$1" = "--search" ]; then
        mode="search"
    elif [ "$1" = "--all" ] || [ -z "$1" ]; then
        mode="all"
    else
        print_error "Unknown argument: $1"
        print_info "Usage: $0 [--all|--search]"
        exit 1
    fi

    # Seed fixtures
    if seed_podcast_fixtures "$mode"; then
        exit_code=0
    else
        exit_code=1
    fi

    # Summary
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_header "✓ Mock Data Seeding Complete"
        print_success "All fixtures generated successfully!"
        print_info "Fixtures saved in services/podcast/tests/fixtures/"
        echo ""
        print_info "Next steps:"
        echo "  1. Review generated fixtures"
        echo "  2. Run tests: cd ../.. && pytest tests/ -v"
        echo "  3. Regenerate anytime by running this script again"
    else
        print_header "✗ Mock Data Seeding Failed"
        print_error "Some fixtures failed to generate"
        echo ""
        print_info "Troubleshooting:"
        echo "  1. Verify you have internet connection"
        echo "  2. Check if PodcastIndex API is accessible"
        echo "  3. Verify PODCASTINDEX_API_KEY is set correctly"
        echo "  4. Verify PODCASTINDEX_API_SECRET is set correctly"
        echo ""
        print_info "Get API credentials at: https://api.podcastindex.org/"
    fi

    return $exit_code
}

# Run main function
main "$@"

