#!/bin/bash
# stop_and_remove.sh
# This script stops and removes the container. Useful for local testing.

CONTAINER_NAME="pr2-transform-app"

# Check if the container is running and stop it
if [ "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
    echo "Stopping container ${CONTAINER_NAME}..."
    docker stop ${CONTAINER_NAME}
else
    echo "Container ${CONTAINER_NAME} is not running."
fi

# Check if the container exists and remove it
if [ "$(docker ps -aq -f name=${CONTAINER_NAME})" ]; then
    echo "Removing container ${CONTAINER_NAME}..."
    docker rm ${CONTAINER_NAME}
else
    echo "Container ${CONTAINER_NAME} does not exist."
fi

echo "Container ${CONTAINER_NAME} has been stopped and removed."
