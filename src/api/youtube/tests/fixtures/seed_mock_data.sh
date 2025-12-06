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
#   - YOUTUBE_API_KEY (for YouTube Data API)
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

# Function to seed YouTube fixtures
seed_youtube_fixtures() {
    print_header "Seeding YouTube Mock Data"

    # Check for required environment variables
    local missing_vars=()

    if ! check_env_var "YOUTUBE_API_KEY"; then
        missing_vars+=("YOUTUBE_API_KEY")
    fi

    if [ ${#missing_vars[@]} -gt 0 ]; then
        print_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        print_info "Get YouTube API key from: https://console.cloud.google.com/apis/credentials"
        print_info "Then add to .env file or run:"
        echo "  export YOUTUBE_API_KEY='your_key_here'"
        return 1
    fi

    print_success "All required environment variables are set"

    # Navigate to YouTube tests directory
    local youtube_dir="$SCRIPT_DIR/youtube/tests"
    
    if [ ! -d "$youtube_dir" ]; then
        print_error "YouTube tests directory not found: $youtube_dir"
        return 1
    fi

    cd "$youtube_dir"

    # Check if generate_mock_data.py exists
    if [ ! -f "fixtures/generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $youtube_dir"
        return 1
    fi

    # Run generator for all fixture types
    print_info "Fetching real data from YouTube Data API..."
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
    
    # Generate model validation data
    print_info "Generating model validation data (models/)..."
    if python fixtures/generate_mock_data.py --models; then
        print_success "Model validation data generated"
    else
        print_error "Failed to generate model validation data"
        all_success=false
    fi
    
    if [ "$all_success" = true ]; then
        print_success "YouTube mock data seeded successfully"
        
        # Show what was generated
        print_info "Generated fixtures in:"
        for dir in fixtures/make_requests fixtures/core fixtures/models; do
            if [ -d "$dir" ]; then
                local count=$(ls -1 "$dir"/*.json 2>/dev/null | wc -l | tr -d ' ')
                if [ "$count" -gt 0 ]; then
                    echo "  - $dir: $count files"
                fi
            fi
        done
        
        return 0
    else
        print_error "Failed to seed some YouTube mock data"
        return 1
    fi
}

# Function to show environment status
show_env_status() {
    print_header "Environment Variables Status"

    echo "YouTube:"
    if check_env_var "YOUTUBE_API_KEY"; then
        print_success "YOUTUBE_API_KEY is set"
    else
        print_error "YOUTUBE_API_KEY is NOT set"
    fi
}

# Main script
main() {
    print_header "Seed Mock Data - Generate Test Fixtures"

    # Try to load environment from .env file
    load_env_from_file

    # Show environment status
    show_env_status

    # Seed YouTube fixtures
    seed_youtube_fixtures
    exit_code=$?

    # Summary
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_header "✓ Mock Data Seeding Complete"
        print_success "All fixtures generated successfully!"
        print_info "Fixtures saved in services/youtube/tests/fixtures/"
        echo ""
        print_info "Next steps:"
        echo "  1. Review generated fixtures"
        echo "  2. Run tests: cd .. && pytest services/youtube/tests/ -v"
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



