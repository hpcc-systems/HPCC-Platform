#!/bin/bash
set -e

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
ROOT_DIR=$SCRIPT_DIR/../..

export $(grep -v '^#' $ROOT_DIR/.env | xargs -d '\r' | xargs -d '\n') > /dev/null

GITHUB_ACTOR="${GITHUB_ACTOR:-hpcc-systems}"
GITHUB_TOKEN="${GITHUB_TOKEN:-none}"
GITHUB_REF=$(git rev-parse --short=8 HEAD)
cd vcpkg
VCPKG_REF=$(git rev-parse --short=8 HEAD)
cd ..
DOCKER_USERNAME="${DOCKER_USERNAME:-hpccbuilds}"
DOCKER_PASSWORD="${DOCKER_PASSWORD:-none}"

echo "SCRIPT_DIR: $SCRIPT_DIR"
echo "ROOT_DIR: $ROOT_DIR"
echo "GITHUB_ACTOR: $GITHUB_ACTOR"
echo "GITHUB_TOKEN: $GITHUB_TOKEN"
echo "GITHUB_REF: $GITHUB_REF"
echo "VCPKG_REF: $VCPKG_REF"
echo "DOCKER_USERNAME: $DOCKER_USERNAME"
echo "DOCKER_PASSWORD: $DOCKER_PASSWORD"
echo "USER: $USER"

# docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD

CMAKE_OPTIONS="-G \"Ninja\" -DCMAKE_BUILD_TYPE=Debug -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF -DCONTAINERIZED=ON -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON"

function doBuild() {
    # Ensure build tools are up to date  ---
    docker build --rm -f "$SCRIPT_DIR/$1.dockerfile" \
        -t build-$1:$GITHUB_REF \
        -t build-$1:latest \
        --build-arg DOCKER_NAMESPACE=$DOCKER_USERNAME \
        --build-arg VCPKG_REF=$VCPKG_REF \
        "$SCRIPT_DIR/." 

    # rm $ROOT_DIR/vcpkg/vcpkg || true
     
    # Check if cmake config needs to be generated  ---
    if ! docker volume ls -q -f name=hpcc_build | grep -q hpcc_build; then
        docker run --rm \
            --mount source=hpcc_src,target=/hpcc-dev/HPCC-Platform,type=volume \
            --mount source="$(pwd)/.git",target=/hpcc-dev/HPCC-Platform/.git,type=bind,consistency=cached \
            --mount source=hpcc_build,target=/hpcc-dev/build,type=volume \
            build-$1:$GITHUB_REF /bin/bash -c \
            "git reset --hard && git submodule update --init --recursive && cmake -S /hpcc-dev/HPCC-Platform -B /hpcc-dev/build ${CMAKE_OPTIONS}"
        #   docker run --rm -it --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached --mount source=build-$1,target=/hpcc-dev/build,type=volume build-ubuntu-22.04:5918a7b8 /bin/bash
    fi


    # Build  (should also update existing config ---
    GIT_STASH=$(git stash create hpcc-docker)
    CONTAINER=$(docker create \
        --mount source=hpcc_src,target=/hpcc-dev/HPCC-Platform,type=volume \
        --mount source="$(pwd)/.git",target=/hpcc-dev/HPCC-Platform/.git,type=bind,consistency=cached \
        --mount source=hpcc_build,target=/hpcc-dev/build,type=volume \
        build-$1:$GITHUB_REF /bin/bash -c \
        "git stash apply $GIT_STASH && cmake --build /hpcc-dev/build --parallel --target install")

    # rm $ROOT_DIR/vcpkg/vcpkg || true

    docker start -a $CONTAINER
    docker commit $CONTAINER build-$1:$GITHUB_REF
    docker rm $CONTAINER

    docker build --rm -f "$SCRIPT_DIR/dev-core.dockerfile" \
        -t dev-core:latest \
        -t hpccsystems/platform-core:gordon \
        --build-arg BUILD_IMAGE=build-$1:$GITHUB_REF \
        "$SCRIPT_DIR"
}

# doBuild amazonlinux
doBuild "ubuntu-22.04" 
# doBuild ubuntu-20.04
# doBuild centos-8
# doBuild centos-7

# docker build --pull --rm -f "$SCRIPT_DIR/core.dockerfile" \
#     -t $DOCKER_USERNAME/core:$GITHUB_REF \
#     -t $DOCKER_USERNAME/core:latest \
#     "build-ubuntu-22.04" 
# docker push $DOCKER_USERNAME/core:$GITHUB_REF
# docker push $DOCKER_USERNAME/core:latest

# docker run -it -d -p 8010:8010 core:latest touch /var/log/HPCCSystems/myesp/esp.log && /etc/init.d/hpcc-init start && tail -f /var/log/HPCCSystems/myesp/esp.log
