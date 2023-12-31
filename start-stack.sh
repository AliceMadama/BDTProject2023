#!/bin/bash

# Colors
GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${GREEN} == Starting application stack == ${NC}"

docker-compose -f ./docker-files/docker-compose-mongo.yaml stop 
docker-compose -f ./docker-files/docker-compose-redis.yaml stop 
docker-compose -f ./docker-files/docker-compose-multinode.yaml stop 
docker-compose -f ./docker-files/docker-compose-openroute.yaml stop

docker-compose -f ./docker-files/docker-compose-mongo.yaml up -d 
docker-compose -f ./docker-files/docker-compose-redis.yaml up -d 
docker-compose -f ./docker-files/docker-compose-multinode.yaml up -d 
docker-compose -f ./docker-files/docker-compose-openroute.yaml up -d

echo -e "${GREEN} == Done == ${NC}"
