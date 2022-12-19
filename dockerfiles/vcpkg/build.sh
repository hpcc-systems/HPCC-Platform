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

docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD

function doBuild() {
    docker build --progress plain --pull --rm -f "$SCRIPT_DIR/$1.dockerfile" \
        -t build-$1:$GITHUB_REF \
        -t build-$1:latest \
        --build-arg VCPKG_REF=$VCPKG_REF \
        "$SCRIPT_DIR/." 

    docker run --rm --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached build-$1:$GITHUB_REF \
        "cmake -S /hpcc-dev/HPCC-Platform -B /hpcc-dev/HPCC-Platform/build-$1 \${CMAKE_OPTIONS}"

    docker run --rm --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached build-$1:$GITHUB_REF \
        "cmake --build \${BUILD_FOLDER} --parallel $(nproc)"

# docker run -it --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached build-ubuntu-22.04:latest bash
}

CMAKE_OPTIONS="-DCMAKE_BUILD_TYPE=RelWithDebInfo -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF -DINCLUDE_PLUGINS=ON -DSUPPRESS_REMBED=ON -DSUPPRESS_V8EMBED=ON"
doBuild ubuntu-22.04 
doBuild ubuntu-20.04
doBuild ubuntu-18.04
doBuild centos-8
doBuild centos-7

# docker build --progress plain --pull --rm -f "$SCRIPT_DIR/core.dockerfile" \
#     -t $DOCKER_USERNAME/core:$GITHUB_REF \
#     -t $DOCKER_USERNAME/core:latest \
#     "build-ubuntu-22.04" 
# docker push $DOCKER_USERNAME/core:$GITHUB_REF
# docker push $DOCKER_USERNAME/core:latest

# docker run -it -d -p 8010:8010 core:latest touch /var/log/HPCCSystems/myesp/esp.log && /etc/init.d/hpcc-init start && tail -f /var/log/HPCCSystems/myesp/esp.log
