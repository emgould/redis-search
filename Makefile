# Set PYTHONPATH globally to include src/ directory for all make commands
export PYTHONPATH := src:$(PYTHONPATH)

.PHONY: help install api etl redis-mac redis-docker test web-local web-docker web-docker-down redis-docker-down docker-down-all lint local-dev local-api local-etl local-setup secrets-setup local-gcs-load-movies local-gcs-load-tv local-gcs-load-all deploy create-redis-vm local tunnel

help:
	@echo "Available make commands:"
	@echo ""
	@echo "  Setup:"
	@echo "    make install       - Create venv + install dependencies"
	@echo "    make local-setup   - Start Redis, build index, seed data (one-time local setup)"
	@echo "    make local-dev     - Print instructions to load local env secrets"
	@echo "    make secrets-setup - Upload secrets to GCP Secret Manager (requires GCP_PROJECT_ID)"
	@echo ""
	@echo "  Local Development:"
	@echo "    make local         - Start Redis, API & Web (if not running), then load all GCS metadata"
	@echo "    make local-api     - Run Search API with local env secrets"
	@echo "    make local-etl     - Run ETL with local env secrets"
	@echo "    make api           - Run Search API (requires secrets already loaded)"
	@echo "    make etl           - Run ETL (requires secrets already loaded)"
	@echo ""
	@echo "  Data Loading (local Redis):"
	@echo "    make local-gcs-load-movies - Load movie metadata from GCS into local Redis"
	@echo "    make local-gcs-load-tv     - Load TV metadata from GCS into local Redis"
	@echo "    make local-gcs-load-all    - Load all TMDB metadata from GCS into local Redis"
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
	@echo "  Cloud Run Deployment:"
	@echo "    make deploy SERVICE=api ENV=dev  - Deploy Search API to Cloud Run (dev)"
	@echo "    make deploy SERVICE=etl ENV=dev  - Deploy ETL job to Cloud Run (dev)"
	@echo "    make create-redis-vm             - Create Redis Stack VM on GCE (one-time)"
	@echo "    make tunnel                      - Create IAP tunnel to public Redis VM (localhost:6381)"
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
	@echo "  make local-api   - Run Search API with dev secrets"
	@echo "  make local-etl   - Run ETL with dev secrets"

local-api:
	@bash -c 'source venv/bin/activate && source scripts/load_secrets.sh local api && uvicorn src.search_api.main:app --reload --port 8080'

local-etl:
	@bash -c 'source venv/bin/activate && source scripts/load_secrets.sh local etl && python -m src.etl.run_etl'

# GCP Secret Manager setup (one-time per environment)
secrets-setup:	
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) bash scripts/setup_gcp_secrets.sh $(ENV)

api:
	. venv/bin/activate && uvicorn src.search_api.main:app --reload --port 8080

etl:
	. venv/bin/activate && python -m src.etl.run_etl

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
deploy:
	./scripts/deploy_cloud_run.sh

# IAP tunnel to Redis VM - forwards localhost:6381 to Redis VM port 6379
# Use PUBLIC_REDIS_PORT=6381 in local.env to connect through tunnel
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
