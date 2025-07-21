#!/bin/bash

# down all containers
docker compose -f docker-compose.flat-file.yml down

# remove generated files and progress tracking files
rm -rf ./deploy/data/processed/artist ./deploy/data/processed/album ./deploy/data/processed/processing_progress.json ./deploy/data/processed/progress.json

# FULL CLEAN: uncomment the next line to clear ALL caches (preprocessing + indexes)
# rm -rf ./deploy/data/processed/artist.filtered ./deploy/data/processed/release-group.filtered ./deploy/data/processed/indexes ./deploy/data/processed/search ./deploy/data/processed/preprocessing_info.json

# INCREMENTAL: for fast development loops, use ./run_data-processor_incremental.sh instead



# run the data processsor
docker compose -f docker-compose.flat-file.yml up data-processor --build

# bring down all containers
docker compose -f docker-compose.flat-file.yml down
