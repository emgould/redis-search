#!/bin/bash
#
# Seed Mock Data - Generate test fixtures from real Comscore API responses
#
# This script runs the generate_mock_data.py script to fetch real API data
# and save as JSON fixtures for testing.
#
# Usage:
#   ./seed_mock_data.sh [--all|--api|--core|--models]
#
# Arguments:
#   --all     Generate all mock data (default)
#   --api     Generate API response fixtures only
#   --core    Generate core method results only
#   --models  Generate model fixtures only
#
# Requirements:
#   - Internet connection to access Comscore API
#   - No API key required (Comscore API is public)
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

# Function to seed Comscore fixtures
seed_comscore_fixtures() {
    local mode="${1:-all}"
    
    print_header "Seeding Comscore Mock Data"

    # Navigate to fixtures directory
    cd "$SCRIPT_DIR"

    # Check if generate_mock_data.py exists
    if [ ! -f "generate_mock_data.py" ]; then
        print_error "generate_mock_data.py not found in $SCRIPT_DIR"
        return 1
    fi

    case "$mode" in
        api)
            print_info "Fetching real data from Comscore API..."
            print_info "This may take a few seconds..."
            
            if python generate_mock_data.py --api; then
                print_success "Comscore API response mock data seeded successfully"
                
                # Show what was generated
                if [ -d "make_requests" ]; then
                    local fixture_count=$(ls -1 make_requests/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count API response files in make_requests/"
                fi
                return 0
            else
                print_error "Failed to seed Comscore API response mock data"
                return 1
            fi
            ;;
        
        core)
            print_info "Generating core method results..."
            
            if python generate_mock_data.py --core; then
                print_success "Comscore core method mock data seeded successfully"
                
                # Show what was generated
                if [ -d "core" ]; then
                    local fixture_count=$(ls -1 core/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count core method files in core/"
                fi
                return 0
            else
                print_error "Failed to seed Comscore core method mock data"
                return 1
            fi
            ;;
        
        models)
            print_info "Generating model fixtures..."
            
            if python generate_mock_data.py --models; then
                print_success "Comscore model fixtures seeded successfully"
                
                # Show what was generated
                if [ -d "models" ]; then
                    local fixture_count=$(ls -1 models/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count model files in models/"
                fi
                return 0
            else
                print_error "Failed to seed Comscore model fixtures"
                return 1
            fi
            ;;
        
        all)
            print_info "Generating all mock data..."
            print_info "This may take a few seconds..."
            
            if python generate_mock_data.py --all; then
                print_success "All Comscore mock data seeded successfully"
                
                # Show what was generated
                echo ""
                if [ -d "make_requests" ]; then
                    local fixture_count=$(ls -1 make_requests/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count API response files in make_requests/"
                fi
                
                if [ -d "core" ]; then
                    local fixture_count=$(ls -1 core/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count core method files in core/"
                fi
                
                if [ -d "models" ]; then
                    local fixture_count=$(ls -1 models/*.json 2>/dev/null | wc -l)
                    print_success "Generated $fixture_count model files in models/"
                fi
                
                return 0
            else
                print_error "Failed to seed Comscore mock data"
                return 1
            fi
            ;;
        
        *)
            print_error "Unknown mode: $mode"
            print_info "Usage: $0 [--all|--api|--core|--models]"
            return 1
            ;;
    esac
}

# Main script
main() {
    print_header "Seed Comscore Mock Data - Generate Test Fixtures"

    # Determine which mode to use
    local mode="all"
    
    if [ "$1" = "--api" ]; then
        mode="api"
    elif [ "$1" = "--core" ]; then
        mode="core"
    elif [ "$1" = "--models" ]; then
        mode="models"
    elif [ "$1" = "--all" ] || [ -z "$1" ]; then
        mode="all"
    else
        print_error "Unknown argument: $1"
        print_info "Usage: $0 [--all|--api|--core|--models]"
        exit 1
    fi

    # Seed fixtures
    if seed_comscore_fixtures "$mode"; then
        exit_code=0
    else
        exit_code=1
    fi

    # Summary
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_header "✓ Mock Data Seeding Complete"
        print_success "All fixtures generated successfully!"
        print_info "Fixtures saved in services/comscore/tests/fixtures/"
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
        echo "  2. Check if Comscore API is accessible"
        echo "  3. Try running with --api flag first to test API access"
        echo ""
        print_info "Note: Comscore API is public and requires no authentication"
    fi

    return $exit_code
}

# Run main function
main "$@"

