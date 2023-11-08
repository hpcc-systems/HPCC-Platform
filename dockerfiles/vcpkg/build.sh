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
GITHUB_BRANCH=$(git log -50 --pretty=format:"%D" | tr ',' '\n' | grep 'upstream/' | awk 'NR==1 {sub("upstream/", ""); print}' | xargs)
DOCKER_USERNAME="${DOCKER_USERNAME:-hpccbuilds}"
DOCKER_PASSWORD="${DOCKER_PASSWORD:-none}"

echo "SCRIPT_DIR: $SCRIPT_DIR"
echo "GITHUB_ACTOR: $GITHUB_ACTOR"
echo "GITHUB_TOKEN: $GITHUB_TOKEN"
echo "GITHUB_REF: $GITHUB_REF"
echo "VCPKG_REF: $VCPKG_REF"
echo "GITHUB_BRANCH: $GITHUB_BRANCH"
echo "DOCKER_USERNAME: $DOCKER_USERNAME"
echo "DOCKER_PASSWORD: $DOCKER_PASSWORD"

docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD

CMAKE_OPTIONS="-G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON"

function doBuild() {
    # docker pull "hpccsystems/platform-build-base-$1:$VCPKG_REF" || true
    # docker pull "hpccsystems/platform-build-$1:$VCPKG_REF" || true
    # docker pull "hpccsystems/platform-build-$1:$GITHUB_BRANCH" || true

    docker build --progress plain --rm -f "$SCRIPT_DIR/$1.dockerfile" \
        --build-arg DOCKER_NAMESPACE=$DOCKER_USERNAME \
        --build-arg VCPKG_REF=$VCPKG_REF \
        --cache-from hpccsystems/platform-build-$1:$VCPKG_REF \
        --cache-from hpccsystems/platform-build-$1:$GITHUB_BRANCH \
        -t hpccsystems/platform-build-$1:$VCPKG_REF \
        -t hpccsystems/platform-build-$1:$GITHUB_BRANCH \
        "$SCRIPT_DIR/."

    # docker push hpccsystems/platform-build-$1:$VCPKG_REF
    # docker push hpccsystems/platform-build-$1:$GITHUB_BRANCH

    CMAKE_OPTIONS_EXTRA=""
    if [ "$1" == "centos-7" ]; then
        CMAKE_OPTIONS_EXTRA="-DVCPKG_TARGET_TRIPLET=x64-centos-7-dynamic"
    elif [ "$1" == "amazonlinux" ]; then
        CMAKE_OPTIONS_EXTRA="-DVCPKG_TARGET_TRIPLET=x64-amazonlinux-dynamic"
    fi
    mkdir -p $HOME/.ccache
    docker run --rm \
        --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached \
        --mount source="$HOME/.ccache",target=/root/.ccache,type=bind,consistency=cached \
        hpccsystems/platform-build-$1:$VCPKG_REF \
        "cmake -S /hpcc-dev/HPCC-Platform -B /hpcc-dev/HPCC-Platform/build-$1 ${CMAKE_OPTIONS} ${CMAKE_OPTIONS_EXTRA} -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache && \
        cmake --build /hpcc-dev/HPCC-Platform/build-$1 --target install --parallel $(nproc) && \
        /etc/init.d/hpcc-init start"

    sudo chown -R $(id -u):$(id -g) ./build-$1
# docker run -it --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached build-ubuntu-22.04:latest bash
}

# doBuild ubuntu-23.10
# doBuild ubuntu-20.04
# doBuild amazonlinux
# doBuild ubuntu-22.04
# doBuild centos-8
doBuild centos-7

wait

# docker build --progress plain --pull --rm -f "$SCRIPT_DIR/core.dockerfile" \
#     -t $DOCKER_USERNAME/core:$GITHUB_REF \
#     -t $DOCKER_USERNAME/core:latest \
#     "build-ubuntu-22.04" 
# docker push $DOCKER_USERNAME/core:$GITHUB_REF
# docker push $DOCKER_USERNAME/core:latest

# docker run -it -d -p 8010:8010 core:latest touch /var/log/HPCCSystems/myesp/esp.log && /etc/init.d/hpcc-init start && tail -f /var/log/HPCCSystems/myesp/esp.log
