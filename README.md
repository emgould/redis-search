# Redis Search

A high-performance search system built with Redis Stack, featuring autocomplete and full-text search capabilities for media content (movies and TV shows). The project includes a FastAPI-based search API, ETL pipeline for data ingestion, and a web-based testing interface.

## Features

- **Fast Autocomplete**: Real-time autocomplete search powered by Redis Search
- **Full-Text Search**: Fuzzy matching and full-text search capabilities
- **Flexible Deployment**: Run locally, in Docker, or deploy to Google Cloud Platform
- **ETL Pipeline**: Load metadata from Google Cloud Storage (TMDB data)
- **Web Interface**: Developer testing UI with autocomplete and management views
- **Secret Management**: Support for both local `.env` files and GCP Secret Manager

## Architecture

The project is organized into several key components:

- **`src/search_api/`** - FastAPI application exposing search endpoints
- **`src/etl/`** - ETL pipeline for data ingestion
- **`src/services/`** - Business logic for search and ETL operations
- **`src/adapters/`** - Redis client and repository interfaces
- **`src/core/`** - Core search query builders and domain models
- **`web/`** - Developer testing web interface
- **`scripts/`** - Setup, deployment, and utility scripts
- **`config/`** - Environment-specific configuration files

## Prerequisites

- Python 3.11+
- Redis Stack (local or Docker)
- Google Cloud credentials (for GCS data loading and GCP Secret Manager)

## Quick Start

### 1. Installation

```bash
make install
```

This creates a virtual environment and installs all dependencies.

### 2. Local Setup

```bash
make local-setup
```

This one-time command:
- Starts Redis in Docker
- Builds the Redis search index
- Seeds example data

### 3. Start the Web Interface

```bash
make web
```

The web interface will be available at http://localhost:9001

## Development Commands

### Setup & Installation

```bash
make help           # Show all available commands
make install        # Create venv + install dependencies
make local-setup    # One-time local Redis setup with index and seed data
```

### Running Services

```bash
make web           # Start web interface (port 9001)
make local-api     # Run Search API with local env secrets (port 8080)
make local-etl     # Run ETL with local env secrets
make api           # Run Search API (requires secrets already loaded)
make etl           # Run ETL (requires secrets already loaded)
```

### Data Loading

Load TMDB metadata from Google Cloud Storage into local Redis:

```bash
make local-gcs-load-movies  # Load movie metadata
make local-gcs-load-tv      # Load TV metadata
make local-gcs-load-all     # Load all metadata
```

### Infrastructure

```bash
make redis-mac     # Install/start Redis using Homebrew (macOS)
make redis-docker  # Run Redis Stack in Docker
make index         # Build Redis search index
make seed          # Seed example data
```

### Docker

```bash
make docker-api    # Run API in Docker
make docker-etl    # Run ETL in Docker
make docker-down   # Stop all Docker containers
```

### Testing & Quality

```bash
make lint          # Run ruff and mypy
make test          # Run pytest suite
```

## Configuration

### Local Development

Configuration files are in the `config/` directory:

- `local.env` - Local development (used by web interface)
- `dev.env` - Development environment
- `prod.env` - Production environment

### Environment Variables

Key variables:

- `REDIS_HOST` - Redis server hostname
- `REDIS_PORT` - Redis server port
- `REDIS_PASSWORD` - Redis password (if required)
- `GCP_PROJECT_ID` - Google Cloud project ID
- `LOCAL_DEV` - Set to `true` to use local `.env` files instead of GCP Secret Manager
- `ENVIRONMENT` - Environment name (`local`, `dev`, or `prod`)

### Secret Management

For local development, secrets are stored in `config/*.env` files.

For GCP deployment, upload secrets to Secret Manager:

```bash
GCP_PROJECT_ID=your-project make secrets-setup ENV=dev
```

## API Endpoints

### Search API (port 8080)

- `GET /autocomplete?q=<query>` - Autocomplete search
- `GET /search?q=<query>` - Full-text search
- `GET /health` - Health check

### Web Interface (port 9001)

- `/` - Home page
- `/autocomplete_test?q=<query>` - Autocomplete test page
- `/management` - View Redis statistics
- `/admin/index_info` - View Redis index information

## Project Structure

```
.
├── config/              # Environment configuration files
├── docker/              # Docker Compose and Dockerfiles
├── scripts/             # Setup and utility scripts
├── src/
│   ├── adapters/        # Redis client and repository
│   ├── api/             # Shared API contracts
│   ├── core/            # Search queries and domain models
│   ├── etl/             # ETL pipeline
│   ├── search_api/      # FastAPI search service
│   └── services/        # Business logic
├── tests/               # Test suite
├── web/                 # Web interface
│   └── templates/       # HTML templates
├── Makefile             # Development commands
└── requirements.txt     # Python dependencies
```

## Development Workflow

1. **Start Redis**: `make redis-docker` or `make redis-mac`
2. **Build Index**: `make index`
3. **Load Data**: `make seed` or `make local-gcs-load-all`
4. **Run Services**: `make web` and/or `make local-api`
5. **Test**: `make test` and `make lint`

## Testing

Run the test suite:

```bash
make test
```

Run linting and type checking:

```bash
make lint
```

## License

MIT
