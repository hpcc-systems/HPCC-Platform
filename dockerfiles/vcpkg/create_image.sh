#!/bin/bash
set -e

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

export $(grep -v '^#' $SCRIPT_DIR/../../.env | xargs -d '\r' | xargs -d '\n') > /dev/null

GITHUB_ACTOR="${GITHUB_ACTOR:-hpcc-systems}"
GITHUB_TOKEN="${GITHUB_TOKEN:-none}"
GITHUB_REF=$(git rev-parse --short=8 HEAD)
cd vcpkg
VCPKG_REF=$(git rev-parse --short=8 HEAD)
cd ..
DOCKER_USERNAME="${DOCKER_USERNAME:-hpccbuilds}"
DOCKER_PASSWORD="${DOCKER_PASSWORD:-none}"

echo "SCRIPT_DIR: $SCRIPT_DIR"
echo "GITHUB_ACTOR: $GITHUB_ACTOR"
echo "GITHUB_TOKEN: $GITHUB_TOKEN"
echo "GITHUB_REF: $GITHUB_REF"
echo "VCPKG_REF: $VCPKG_REF"
echo "DOCKER_USERNAME: $DOCKER_USERNAME"
echo "DOCKER_PASSWORD: $DOCKER_PASSWORD"
echo "USER: $USER"

# docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD

docker build --progress plain --rm -f "$SCRIPT_DIR/dev-core.dockerfile" \
    -t dev-core:latest \
    "$SCRIPT_DIR"

    # "$(pwd)/build/stage/opt/HPCCSystems" 

# docker run --rm -u 0 --mount source="$(pwd)/build",target=/hpcc-dev/build,type=bind,consistency=cached dev-core:latest \
# "cd /hpcc-dev/build"

# CONTAINER=$(docker create dev-core:latest)
# docker cp $(pwd)/build/stage/opt/HPCCSystems $CONTAINER:/opt/HPCCSystems
# docker commit $CONTAINER dev-core:final
