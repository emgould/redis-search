# Set PYTHONPATH globally to include src/ directory for all make commands
export PYTHONPATH := src:$(PYTHONPATH)

.PHONY: help install etl redis-mac redis-docker test web-local web-docker web-docker-down redis-docker-down docker-down-all lint local-dev local-etl local-setup secrets-setup local-gcs-load-movies local-gcs-load-tv local-gcs-load-all deploy deploy-api deploy-etl deploy-vm deploy-vm-all setup-etl-schedule create-redis-vm local tunnel etl-docker etl-docker-build etl-docker-tv etl-docker-movie etl-docker-person etl-docker-test etl-docker-cron etl-docker-cron-stop cache-version-get cache-version-set cache-version-list cache-version-seed

help:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  MEDIA CIRCLE - Redis Search Service"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "  A unified search and autocomplete service for media metadata."
	@echo "  Indexes movies, TV shows, people (TMDB), books, and authors (OpenLibrary)"
	@echo "  into Redis Search for fast full-text search and autocomplete."
	@echo ""
	@echo "  INFRASTRUCTURE:"
	@echo "    • Redis Stack VM    - GCE (redis-stack-vm, us-central1-a)"
	@echo "    • Web/API Service   - Cloud Run (media-circle-search)"
	@echo "    • ETL Service       - GCE VM (etl-vm, runs nightly at 2 AM UTC)"
	@echo "    • Data Storage      - GCS (gs://media-circle-metadata/)"
	@echo ""
	@echo "  DATA SOURCES:"
	@echo "    • TMDB API          - Movies, TV shows, people"
	@echo "    • OpenLibrary       - Books and authors (via Wikidata + OL dumps)"
	@echo ""
	@echo "  LOCAL DEVELOPMENT:"
	@echo "    • Redis connects via IAP tunnel (localhost:6381 → redis-stack-vm:6379)"
	@echo "    • Web app runs on http://localhost:9001"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "COMMANDS:"
	@echo ""
	@echo "  Setup:"
	@echo "    make install       - Create venv + install dependencies"
	@echo "    make local-setup   - Start Redis, build index, seed data (one-time local setup)"
	@echo "    make local-dev     - Print instructions to load local env secrets"
	@echo "    make secrets-setup - Upload secrets to GCP Secret Manager (requires GCP_PROJECT_ID)"
	@echo ""
	@echo "  Local Development:"
	@echo "    make local         - Start Redis, API & Web (if not running), then load all GCS metadata"
	@echo "    make local-etl     - Run ETL with local env secrets"
	@echo "    make etl           - Run ETL (requires secrets already loaded)"
	@echo ""
	@echo ""
	@echo "  Infrastructure:"
	@echo "    make redis-mac     - Install/start Redis using Homebrew"
	@echo "    make redis-docker  - Run Redis Stack in Docker"
	@echo "    make index         - Build Redis search index"
	@echo "    make seed          - Seed example data"
	@echo ""
	@echo "  Web App:"
	@echo "    make web-local        - Start web app locally on port 9001"
	@echo "    make web-docker       - Start web app in Docker on port 9001 (auto-starts Redis if needed)"
	@echo "    make web-docker-down  - Stop web container only (Redis keeps running)"
	@echo "    make redis-docker-down - Stop Redis container"
	@echo "    make docker-down-all  - Stop all Docker containers"
	@echo ""
	@echo "  ETL (Docker):"
	@echo "    make etl-docker       - Run full ETL in Docker (auto-starts Redis if needed)"
	@echo "    make etl-docker-tv    - Run TV ETL only in Docker"
	@echo "    make etl-docker-movie - Run Movie ETL only in Docker"
	@echo "    make etl-docker-person - Run Person ETL only in Docker"
	@echo "    make etl-docker-build - Build ETL Docker image"
	@echo "    make etl-docker-test  - Test ETL configuration and environment (dry-run)"
	@echo "    make etl-docker-cron  - Start ETL with cron scheduler (3 AM UTC daily)"
	@echo "    make etl-docker-cron-stop - Stop cron scheduler container"
	@echo ""
	@echo "  Deployment:"
	@echo "    make deploy-web       - Deploy Search Web App(autocomplete service) to Cloud Run"
	@echo "    make deploy-etl       - Deploy ETL service to Dedicated ETL VM"
	@echo "    make setup-etl-schedule - Setup daily ETL schedule (2 AM UTC, auto-shutdown)"
	@echo "    make create-redis-vm  - Create Redis Stack VM on GCE (one-time)"
	@echo "    make tunnel           - Create IAP tunnel to public Redis VM (localhost:6381)"
	@echo ""
	@echo "  Cache Version Management (REDIS=local|public required):"
	@echo "    make cache-version-get PREFIX=<prefix> REDIS=local  - Get version for a cache prefix"
	@echo "    make cache-version-set PREFIX=<prefix> VERSION=<ver> REDIS=local - Set version"
	@echo "    make cache-version-list REDIS=local                 - List all cache prefix versions"
	@echo "    make cache-version-seed REDIS=local                 - Seed all cache versions into Redis"
	@echo ""
	@echo "  Testing:"
	@echo "    make lint          - Run linting and type checking"
	@echo "    make test          - Run pytest suite"

install:
	bash scripts/python_setup.sh

lint:
	bash scripts/lint_check.sh

# One-time local setup: start Redis, build index, seed data
local-setup:
	@echo "🚀 Setting up local development environment..."
	@echo ""
	@echo "1️⃣  Starting Redis..."
	@cd docker && docker compose up -d redis
	@sleep 2
	@echo ""
	@echo "2️⃣  Building search index..."
	@. venv/bin/activate && python scripts/build_redis_index.py
	@echo ""
	@echo "3️⃣  Seeding example data..."
	@. venv/bin/activate && python scripts/seed_example_data.py
	@echo ""
	@echo "✅ Local setup complete! Run 'make web' to start the web app."

# Local development with secrets from config/*.env files
local-dev:
	@echo "To load local dev secrets into your current shell, run:"
	@echo ""
	@echo "  source venv/bin/activate"
	@echo "  LOCAL_DEV=true source scripts/load_secrets.sh dev etl"
	@echo ""
	@echo "Or use these convenience commands:"
	@echo "  make local-etl   - Run ETL with dev secrets"
	@echo "  make web-local   - Run web app with dev secrets"

local-etl:
	@bash -c 'source venv/bin/activate && source scripts/load_secrets.sh local etl && python -m src.etl.bulk_loader'

# GCP Secret Manager setup (one-time per environment)
secrets-setup:	
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) bash scripts/setup_gcp_secrets.sh $(ENV)

etl:
	. venv/bin/activate && python -m src.etl.bulk_loader

redis-mac:
	bash scripts/install_redis_mac.sh

redis-docker:
	cd docker && docker-compose up -d redis

redis-status:
	@gcloud compute instances describe redis-stack-vm --zone=us-central1-a --format='value(status)'
	@gcloud compute instances describe redis-stack-vm --zone=us-central1-a --format='value(networkInterfaces[0].networkIP)'

test:
	. venv/bin/activate && pytest -q

web-local:
	@echo "🐳 Starting local environment..."
	@echo ""
	@echo "1️⃣  Checking IAP tunnel to public Redis..."
	@if ! lsof -ti:6381 > /dev/null 2>&1; then \
		echo "   Tunnel not running, starting it in background..."; \
		(nohup gcloud compute start-iap-tunnel redis-stack-vm 6379 \
			--local-host-port=localhost:6381 \
			--zone=us-central1-a \
			--project=media-circle > /tmp/iap-tunnel.log 2>&1 &) && \
		echo "   Tunnel started (logs: /tmp/iap-tunnel.log)"; \
		sleep 3; \
	else \
		echo "   ✅ Tunnel already running on port 6381"; \
	fi
	@bash -c 'source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh local api && uvicorn web.app:app --reload --port 9001'

web-docker:
	@echo "🐳 Starting Docker environment..."
	@echo ""
	@echo "1️⃣  Checking IAP tunnel to public Redis..."
	@if ! lsof -ti:6381 > /dev/null 2>&1; then \
		echo "   Tunnel not running, starting it in background..."; \
		(nohup gcloud compute start-iap-tunnel redis-stack-vm 6379 \
			--local-host-port=localhost:6381 \
			--zone=us-central1-a \
			--project=media-circle > /tmp/iap-tunnel.log 2>&1 &) && \
		echo "   Tunnel started (logs: /tmp/iap-tunnel.log)"; \
		sleep 3; \
	else \
		echo "   ✅ Tunnel already running on port 6381"; \
	fi
	@echo ""
	@echo "2️⃣  Checking local Redis container..."
	@if ! docker ps --format '{{.Names}}' | grep -q '^redis-search-redis-1$$' 2>/dev/null; then \
		echo "   Redis not running, starting it..."; \
		cd docker && docker-compose up -d redis; \
		sleep 3; \
	else \
		echo "   ✅ Redis already running"; \
	fi
	@echo ""
	@echo "3️⃣  Starting web container..."
	cd docker && docker-compose up --build web

web-docker-down:
	@echo "🛑 Stopping web container (Redis will keep running)..."
	cd docker && docker-compose stop web
	cd docker && docker-compose rm -f web

redis-docker-down:
	@echo "🛑 Stopping Redis container..."
	cd docker && docker-compose stop redis

docker-down-all:
	@echo "🛑 Stopping all Docker containers..."
	cd docker && docker-compose down

# Build ETL Docker image
etl-docker-build:
	@echo "📦 Building ETL Docker image..."
	docker build -f docker/etl.Dockerfile -t redis-search-etl .

# Run ETL in Docker (requires Redis to be running)
etl-docker:
	@echo "🔄 Running ETL in Docker..."
	@# Ensure Redis is running
	@if ! docker ps | grep -q redis-search-redis; then \
		echo "Starting Redis first..."; \
		cd docker && docker-compose up -d redis; \
		sleep 3; \
	fi
	cd docker && docker-compose --profile etl run --rm etl

# Run ETL in Docker with specific job
etl-docker-tv:
	cd docker && docker-compose --profile etl run --rm etl run --job tv

etl-docker-movie:
	cd docker && docker-compose --profile etl run --rm etl run --job movie

etl-docker-person:
	cd docker && docker-compose --profile etl run --rm etl run --job person

# Test cron setup without actually running ETL (dry-run)
etl-docker-test:
	cd docker && docker-compose --profile etl run --rm etl test

# Start ETL container with cron daemon (runs at 3 AM UTC)
etl-docker-cron:
	@echo "🕐 Starting ETL container with cron scheduler..."
	@echo "   ETL will run daily at 3 AM UTC"
	@echo "   View logs: docker logs -f redis-search-etl-1"
	@echo "   Run manually: docker exec redis-search-etl-1 python -m etl.run_nightly_etl"
	cd docker && docker-compose --profile etl run -d --name redis-search-etl-cron etl cron

# Stop ETL cron container
etl-docker-cron-stop:
	docker rm -f redis-search-etl-cron 2>/dev/null || true

index:
	. venv/bin/activate && python scripts/build_redis_index.py

seed:
	. venv/bin/activate && python scripts/seed_example_data.py

# GCS metadata loading - loads TMDB data from Google Cloud Storage into local Redis
local-gcs-load-movies:
	@bash -c 'source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh local etl && python scripts/load_gcs_metadata.py --type movie'

local-gcs-load-tv:
	@bash -c 'source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh local etl && python scripts/load_gcs_metadata.py --type tv'

local-gcs-load-all:
	@bash -c 'source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh local etl && python scripts/load_gcs_metadata.py --type all'

# Start Redis & API if not running, then load all GCS metadata
local:
	@echo "🚀 Starting local development environment..."
	@echo ""
	@echo "1️⃣  Checking Redis..."
	@if ! docker ps --format '{{.Names}}' | grep -q '^docker-redis-1$$' 2>/dev/null; then \
		echo "   Redis not running, starting it..."; \
		$(MAKE) redis-docker; \
		sleep 3; \
	else \
		echo "   ✅ Redis is already running"; \
	fi
	@echo ""
	@echo "2️⃣  Checking API..."
	@if ! lsof -ti:8080 > /dev/null 2>&1; then \
		echo "   API not running, starting it in background..."; \
		(nohup bash -c 'cd $(CURDIR) && source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh local api && uvicorn src.search_api.main:app --reload --port 8080' > /tmp/api.log 2>&1 &) && \
		echo "   API started in background (logs: /tmp/api.log)"; \
		sleep 3; \
	else \
		echo "   ✅ API is already running on port 8080"; \
	fi
	@echo ""
	@echo "3️⃣  Checking Web..."
	@if ! lsof -ti:9001 > /dev/null 2>&1; then \
		echo "   Web not running, starting it in background..."; \
		(nohup bash -c 'cd $(CURDIR) && source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh local api && uvicorn web.app:app --reload --port 9001' > /tmp/web.log 2>&1 &) && \
		echo "   Web started in background (logs: /tmp/web.log)"; \
		sleep 3; \
	else \
		echo "   ✅ Web is already running on port 9001"; \
	fi
	@echo ""
	@echo "4️⃣  Building Redis search index..."
	@. venv/bin/activate && python scripts/build_redis_index.py
	@echo ""
	@echo "5️⃣  Loading all GCS metadata..."
	@$(MAKE) local-gcs-load-all
	@echo ""
	@echo "✅ Local environment ready!"
	@echo ""
	@echo "🌐 Opening browser..."
	@open http://localhost:9001 2>/dev/null || xdg-open http://localhost:9001 2>/dev/null || echo "   Please open http://localhost:9001 in your browser"

# Cloud Run deployment
create-redis-vm:
	./scripts/create_redis_vm.sh

# Deploy to Cloud Run (dev environment)
# Deploy Search API to Cloud Run (for autocomplete/search endpoints)
deploy-web:
	./scripts/deploy_web_cr.sh

# Deploy ETL service to VM (ETL container only, Redis unchanged)
deploy-etl:
	./scripts/deploy_etl_vm.sh

# Setup scheduled ETL (2 AM UTC daily, auto-shutdown after completion)
setup-etl-schedule:
	./scripts/setup_etl_schedule.sh

# Legacy alias
deploy: deploy-api

# IAP tunnel to Redis VM - forwards localhost:6381 to Redis VM port 6379
# Use PUBLIC_REDIS_PORT=6381 in local.env to connect through tunnel
# Test public Redis connectivity (requires `make tunnel` in another terminal)
test-redis-public:
	@if ! lsof -ti:6381 > /dev/null 2>&1; then \
		echo "❌ No tunnel on port 6381. Start it first:"; \
		echo "   make tunnel"; \
		echo ""; \
		exit 1; \
	fi
	@echo "🧪 Testing Redis connectivity via IAP tunnel (localhost:6381)..."
	@. venv/bin/activate 2>/dev/null || . .venv/bin/activate; \
	REDIS_HOST=localhost REDIS_PORT=6381 REDIS_PASSWORD=rCrwd3xMFhfoKhUF9by9 python scripts/test_redis_connectivity.py

tunnel:
	@echo "🔐 Creating IAP tunnel to Redis VM..."
	@echo "   Local port:  localhost:6381"
	@echo "   Remote:      redis-stack-vm:6379"
	@echo ""
	@echo "   To use: set PUBLIC_REDIS_PORT=6381 in config/local.env"
	@echo "   Press Ctrl+C to close the tunnel"
	@echo ""
	gcloud compute start-iap-tunnel redis-stack-vm 6379 \
		--local-host-port=localhost:6381 \
		--zone=us-central1-a \
		--project=media-circle

# Cache version management — shared registry in Redis
# REDIS=local|public is REQUIRED for all cache-version-* targets
#   local  → config/local.env
#   public → config/etl.dev.env
define CACHE_REDIS_ENV
source venv/bin/activate && \
if [ "$(REDIS)" = "local" ]; then source config/local.env; \
elif [ "$(REDIS)" = "public" ]; then source config/etl.dev.env; \
else echo "ERROR: REDIS=local|public is required"; exit 1; fi
endef

cache-version-get:
	@bash -c '$(CACHE_REDIS_ENV) && python -c "from utils.redis_cache import get_cache_version; print(get_cache_version(\"$(PREFIX)\"))"'

cache-version-set:
	@bash -c '$(CACHE_REDIS_ENV) && python -c "from utils.redis_cache import set_cache_version; set_cache_version(\"$(PREFIX)\", \"$(VERSION)\"); print(\"$(PREFIX) -> $(VERSION)\")"'

cache-version-list:
	@bash -c '$(CACHE_REDIS_ENV) && python -c "\
from utils.redis_cache import get_all_cache_versions; \
versions = get_all_cache_versions(); \
[print(f\"  {k:30s} {v}\") for k, v in sorted(versions.items())] if versions else print(\"  (no versions registered)\"); \
"'

cache-version-seed:
	@bash -c '$(CACHE_REDIS_ENV) && python scripts/seed_cache_versions.py'
