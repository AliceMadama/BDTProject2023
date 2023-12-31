#!/bin/bash

# Colors
GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${GREEN} == Stopping application stack == ${NC}"


docker-compose -f "docker-files/docker-compose-mongo.yaml" stop 
docker-compose -f "docker-files/docker-compose-redis.yaml" stop 
docker-compose -f "docker-files/docker-compose-multinode.yaml" stop 
docker-compose -f "docker-files/docker-compose-openroute.yaml" stop

#docker-compose -f "docker-files/docker-compose-flink.yaml" stop

docker-compose -f "docker-files/docker-compose-mongo.yaml" down
docker-compose -f "docker-files/docker-compose-redis.yaml" down
docker-compose -f "docker-files/docker-compose-multinode.yaml" down
docker-compose -f "docker-files/docker-compose-openroute.yaml" down

#docker-compose -f "docker-files/docker-compose-flink.yaml" down
echo -e "${GREEN} == Done == ${NC}"