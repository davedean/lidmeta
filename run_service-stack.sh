docker compose -f docker-compose.flat-file.yml down search-service caddy proxy lidarr

docker compose -f docker-compose.flat-file.yml up search-service caddy proxy lidarr --build
