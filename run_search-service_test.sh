#!/bin/bash

docker compose -f docker-compose.flat-file.yml down

sleep 1

docker compose -f docker-compose.flat-file.yml up -d search-service caddy proxy lidarr --build

echo "Waiting for containers to start .. "
sleep 5

echo ''
echo ''
echo "##### Testing Search Service (should succeed).."
docker exec -it search-service bash -c 'curl -vvv http://search-service:8001/search/artists?q=underworld'

echo ''
echo ''
echo "Installing curl to the Caddy container.."
docker exec -it caddy sh -c 'apk update > /dev/null && apk add curl > /dev/null'

echo ''
echo ''
echo "Testing Search Service from inside the Caddy container.."
docker exec -it caddy sh -c 'curl -vvv "http://search-service:8001/search/artists?q=underworld"'

echo ''
echo ''
echo "Testing Caddy from inside the Caddy container.."
docker exec -it caddy sh -c 'curl -vvv "http://caddy/api/v1/search?type=all&query=radiohead"'


echo ''
echo ''
echo "##### Testing Caddy from the proxy container (should succeed).."
docker exec -it skyhook-proxy bash -c '/usr/bin/curl -vvv "http://caddy/api/v1/search?type=all&query=madonna"'

echo "Complete"
sleep 1

#docker compose -f docker-compose.flat-file.yml down
