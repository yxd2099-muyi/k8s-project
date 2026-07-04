#!/bin/bash

set -e

# 启动容器
echo "启动Docker容器..."

docker compose -f docker-compose.yml down -v
docker compose -f docker-compose.yml up -d
# 等待容器启动
echo "等待容器启动..."
sleep 5

