
.PHONY: help install api etl redis-mac redis-docker test docker-api docker-etl docker-down web lint local-dev local-api local-etl local-setup secrets-setup local-gcs-load-movies local-gcs-load-tv local-gcs-load-all deploy-api deploy-etl create-redis-vm

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
	@echo "    make web           - Start developer test website on port 9001"
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
	@echo "  Docker:"
	@echo "    make docker-api    - Run API in Docker (uses LOCAL_DEV=true)"
	@echo "    make docker-etl    - Run ETL in Docker (uses LOCAL_DEV=true)"
	@echo "    make docker-down   - Stop all Docker containers"
	@echo ""
	@echo "  Cloud Run Deployment:"
	@echo "    make create-redis-vm - Create Redis Stack VM on GCE (one-time)"
	@echo "    make deploy-api      - Deploy Search API to Cloud Run"
	@echo "    make deploy-etl      - Deploy ETL job to Cloud Run"
	@echo "    (Set REDIS_HOST=<vm-ip> before deploy commands)"
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
	@bash -c 'source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh dev search_api && uvicorn src.search_api.main:app --reload --port 8080'

local-etl:
	@bash -c 'source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh dev etl && python -m src.etl.run_etl'

# GCP Secret Manager setup (one-time per environment)
secrets-setup:
	@if [ -z "$(GCP_PROJECT_ID)" ]; then \
		echo "Error: GCP_PROJECT_ID is required"; \
		echo "Usage: GCP_PROJECT_ID=your-project make secrets-setup ENV=dev"; \
		exit 1; \
	fi
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) bash scripts/setup_gcp_secrets.sh $(ENV)

api:
	. venv/bin/activate && uvicorn src.search_api.main:app --reload --port 8080

etl:
	. venv/bin/activate && python -m src.etl.run_etl

redis-mac:
	bash scripts/install_redis_mac.sh

redis-docker:
	cd docker && docker-compose up -d redis

test:
	. venv/bin/activate && pytest -q

web:
	@bash -c 'source venv/bin/activate && LOCAL_DEV=true source scripts/load_secrets.sh local search_api && uvicorn web.app:app --reload --port 9001'

docker-api:
	cd docker && docker-compose up search_api

docker-etl:
	cd docker && docker-compose run etl

docker-down:
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

# Cloud Run deployment
create-redis-vm:
	./scripts/create_redis_vm.sh

deploy-api:
	./scripts/deploy_cloud_run.sh api prod

deploy-etl:
	./scripts/deploy_cloud_run.sh etl prod
