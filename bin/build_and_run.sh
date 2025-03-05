#!/bin/bash
# build_and_run.sh
# This script builds and runs the Docker image. Useful for local testing.

# Exit immediately if a command exits with a non-zero status.
set -e

IMAGE_NAME="pr2-transform-app"
PORT=8080

# Build the Docker image.
docker build -t ${IMAGE_NAME} .

# Run the container.
docker run -d -e PORT=${PORT} -p ${PORT}:${PORT} --name ${IMAGE_NAME} ${IMAGE_NAME}

echo "Container started on port ${PORT}"
