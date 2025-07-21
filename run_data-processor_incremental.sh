#!/bin/bash

# Incremental data processor run - preserves cached files for fast development loops
# Only removes output files, keeps preprocessing and index caches

echo "🚀 Running Incremental Data Processing (preserving caches)"
echo "======================================================="

# down all containers
docker compose -f docker-compose.flat-file.yml down data-processor

# remove only output files, preserve cached preprocessing and indexes
echo "🧹 Moving old output files (preserving preprocessing and index caches)..."
#rm -rf ./deploy/data/processed/artist ./deploy/data/processed/album ./deploy/data/processed/processing_progress.json ./deploy/data/processed/progress.json
mv ./deploy/data/processed/artist ./deploy/data/processed/album ./deploy/data/processed/archive

echo "💾 Preserved cache files:"
if [ -f "./deploy/data/processed/artist.filtered" ]; then
    echo "  ✅ Artist filtered file"
else
    echo "  ❌ Artist filtered file (will be created)"
fi

if [ -f "./deploy/data/processed/release-group.filtered" ]; then
    echo "  ✅ Release-group filtered file"
else
    echo "  ❌ Release-group filtered file (will be created)"
fi

if [ -d "./deploy/data/processed/indexes" ]; then
    echo "  ✅ Index files"
else
    echo "  ❌ Index files (will be created)"
fi

echo ""
echo "🔄 Running optimized data processor..."

# run the data processor
docker compose -f docker-compose.flat-file.yml up data-processor --build

# bring down container
docker compose -f docker-compose.flat-file.yml down data-processor

echo ""
echo "✅ Incremental processing complete!"
echo ""
echo "For a completely clean rebuild, use: ./run_data-processor_clean.sh"
echo "For testing preprocessing only, use: ./run_preprocessing_only.sh"
echo ""
echo "Hit Enter to remove old processed files (takes longer than 10 minutes) or Ctrl-C to leave without cleaning up"

read

# remove old processed files (10+ minutes)
rm -rf ./deploy/data/processed/archive/artist ./deploy/data/processed/archive/album

