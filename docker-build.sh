#!/bin/bash
set -e

IMAGE_NAME="qactpmdgateway"
TAG="${1:-latest}"

echo "构建 Docker 镜像: ${IMAGE_NAME}:${TAG}"
docker build -t ${IMAGE_NAME}:${TAG} .

echo "构建完成！"
echo ""
echo "运行: docker-compose up -d"
echo "或: docker run -d -p 7899:7899 --name qactpmdgateway ${IMAGE_NAME}:${TAG}"
