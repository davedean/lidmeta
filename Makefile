PY ?= poetry

.PHONY: install lint test run docker-dev docker-build docker-clean

install:
	$(PY) install

lint:
	$(PY) run black --check .
	$(PY) run isort --check .
	$(PY) run flake8 .

format:
	$(PY) run black .
	$(PY) run isort .

test:
	$(PY) run pytest -q

run:
	$(PY) run uvicorn lidarr_metadata_server.main:app --reload --port 8080

install-hooks:
	pre-commit install

# Docker development targets
# Metadata server only (fast dev)
docker-dev:
	DOCKER_BUILDKIT=1 docker-compose -f docker-compose.metadata-only.yml up --build

docker-build:
	DOCKER_BUILDKIT=1 docker-compose -f docker-compose.metadata-only.yml build

docker-build-no-cache:
	docker-compose -f docker-compose.metadata-only.yml build --no-cache

docker-clean:
	docker-compose -f docker-compose.metadata-only.yml down -v
	docker system prune -f

# Clean up old dump directories
cleanup-dumps:
	./scripts/cleanup-old-dumps.sh

# Quick rebuild without cache (for dependency changes)
docker-rebuild:
	docker-compose -f docker-compose.metadata-only.yml build --no-cache lidarr-metadata-server
	docker-compose -f docker-compose.metadata-only.yml up

# Optimized builds using build script
docker-build-optimized:
	./scripts/build-docker.sh development

docker-build-prod:
	./scripts/build-docker.sh production

# Dev mode with different sample sizes
docker-dev-small:
	LMS_SAMPLE_SIZE_MB=50 docker-compose -f docker-compose.metadata-only.yml up --build

docker-dev-medium:
	LMS_SAMPLE_SIZE_MB=200 docker-compose -f docker-compose.metadata-only.yml up --build

# Production mode (full extraction)
docker-prod:
	LMS_DEV_MODE=0 docker-compose -f docker-compose.metadata-only.yml up --build

# Full stack (with proxy and Lidarr)
docker-full:
	docker-compose -f deploy/docker-compose.dev.yml up --build

docker-full-clean:
	docker-compose -f deploy/docker-compose.dev.yml down -v
