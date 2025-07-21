FROM python:3.12-slim AS base

# System deps (build-essential for any future wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gnupg \
 && rm -rf /var/lib/apt/lists/*

# Import MusicBrainz GPG key for signature verification
RUN gpg --keyserver hkps://keys.openpgp.org --recv-key C777580F || \
    gpg --keyserver hkps://keyserver.ubuntu.com --recv-key C777580F || \
    gpg --keyserver hkps://pgp.mit.edu --recv-key C777580F || \
    echo "Warning: Could not import MusicBrainz GPG key"

# Create dump directory (will be mounted from host)
RUN mkdir -p /app/data/mbjson && chown -R 1000:1000 /app/data/mbjson

# Install Poetry
ENV POETRY_VERSION=1.7.1
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv
ENV PATH="/opt/poetry-venv/bin:$PATH"

# Install poetry in a virtual environment
RUN python3 -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}

WORKDIR /app

ARG BUILD_VERSION
ENV BUILD_VERSION=$BUILD_VERSION
LABEL org.opencontainers.image.revision=$BUILD_VERSION

# Copy only dependency descriptors first for cache efficiency
COPY pyproject.toml poetry.lock* /app/

# Install dependencies with cache mount for better performance
# This layer will be cached unless pyproject.toml or poetry.lock change
RUN --mount=type=cache,target=/root/.cache/pip \
    poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-root --no-dev

# Copy application source (exclude host virtualenv via .dockerignore)
COPY capture_proxy /app/capture_proxy
COPY data_processor /app/data_processor
COPY search_service /app/search_service
COPY README.md /app/

# Copy dump management scripts
COPY tools/download_mbjson_dump.py /opt/metadata-server/tools/
COPY tools/build_index.py /opt/metadata-server/tools/
COPY tools/inspect_index.py /opt/metadata-server/tools/

# (Optional) Make scripts executable
RUN chmod +x /opt/metadata-server/tools/*.py

EXPOSE 8080

CMD ["poetry", "run", "uvicorn", "lidarr_metadata_server.main:app", "--host", "0.0.0.0", "--port", "8080"]

# Development stage with dev dependencies
FROM base AS development

# Install dev dependencies for development
RUN --mount=type=cache,target=/root/.cache/pip \
    poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-root

# Production stage (default)
FROM base AS production
