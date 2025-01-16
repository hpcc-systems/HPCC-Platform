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
GITHUB_BRANCH=${GITHUB_BRANCH:-master}
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

CMAKE_ALL_OPTIONS="-G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DHPCC_SOURCE_DIR=/hpcc-dev/HPCC-Platform -DCONTAINERIZED=OFF -DCPACK_STRIP_FILES=ON -DINCLUDE_PLUGINS=ON -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF -DUSE_CPPUNIT=ON -DSUPPRESS_REMBED=ON -DSUPPRESS_V8EMBED=ON"
CMAKE_OPENBLAS_OPTIONS="-G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DHPCC_SOURCE_DIR=/hpcc-dev/HPCC-Platform -DCONTAINERIZED=OFF -DCPACK_STRIP_FILES=OFF -DECLBLAS=ON -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF"
CMAKE_PLATFORM_OPTIONS="-G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DHPCC_SOURCE_DIR=/hpcc-dev/HPCC-Platform -DCONTAINERIZED=OFF -DCPACK_STRIP_FILES=ON -DPLATFORM=ON -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF"

function doBuild() {
    # docker pull "hpccsystems/platform-build-base-$1:$VCPKG_REF" || true
    # docker pull "hpccsystems/platform-build-$1:$VCPKG_REF" || true
    # docker pull "hpccsystems/platform-build-$1:$GITHUB_BRANCH" || true

    docker buildx build --progress plain --rm -f "$SCRIPT_DIR/$1.dockerfile" \
        --build-arg DOCKER_NAMESPACE=$DOCKER_USERNAME \
        --build-arg VCPKG_REF=$VCPKG_REF \
        --cache-from hpccsystems/platform-build-$1:$VCPKG_REF \
        --cache-from hpccsystems/platform-build-$1:$GITHUB_BRANCH \
        -t hpccsystems/platform-build-$1:$VCPKG_REF \
        -t hpccsystems/platform-build-$1:$GITHUB_BRANCH \
        "$SCRIPT_DIR/."

    # docker push hpccsystems/platform-build-$1:$VCPKG_REF
    # docker push hpccsystems/platform-build-$1:$GITHUB_BRANCH

    rm -f ./vcpkg/vcpkg 
    mkdir -p ./build-$1
    CMAKE_OPTIONS_EXTRA="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"

    mkdir -p $HOME/.ccache
    docker run --rm \
        --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached \
        --mount source="$(realpath ~)/.cache/vcpkg",target=/root/.cache/vcpkg,type=bind,consistency=cached \
        --mount source="$HOME/.ccache",target=/root/.ccache,type=bind,consistency=cached \
        hpccsystems/platform-build-$1:$VCPKG_REF \
        "rm -rf /hpcc-dev/HPCC-Platform/build-$1/CMakeCache.txt /hpcc-dev/HPCC-Platform/build-$1/CMakeFiles && \
        cmake -S /hpcc-dev/HPCC-Platform -B /hpcc-dev/HPCC-Platform/build-$1 ${CMAKE_ALL_OPTIONS} ${CMAKE_OPTIONS_EXTRA} && \
        cmake --build /hpcc-dev/HPCC-Platform/build-$1 --parallel && \
        echo 'Done'"

    rm -f ./vcpkg/vcpkg 

# sudo chown -R $(id -u):$(id -g) ./build-$1
# docker run -it --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached build-ubuntu-22.04:latest bash
}

function cleanup() {
    kill $(jobs -p)
    rm -f ./vcpkg/vcpkg || true
}

trap 'cleanup; exit' EXIT

# ./vcpkg/bootstrap-vcpkg.sh
mkdir -p ./vcpkg-logs

if [ "$1" != "" ]; then
    doBuild $1
else
    doBuild ubuntu-24.04 &> vcpkg-logs/ubuntu-24.04.log &
    doBuild ubuntu-22.04 &> vcpkg-logs/ubuntu-22.04.log &
    doBuild ubuntu-20.04 &> vcpkg-logs/ubuntu-20.04.log &
    doBuild rockylinux-8 &> vcpkg-logs/rockylinux-8.log &
    doBuild centos-7 &> vcpkg-logs/centos-7.log & 
fi

wait

# docker build --progress plain --pull --rm -f "$SCRIPT_DIR/core.dockerfile" \
#     -t $DOCKER_USERNAME/core:$GITHUB_REF \
#     -t $DOCKER_USERNAME/core:latest \
#     "build-ubuntu-22.04" 
# docker push $DOCKER_USERNAME/core:$GITHUB_REF
# docker push $DOCKER_USERNAME/core:latest

# docker run -it -d -p 8010:8010 core:latest touch /var/log/HPCCSystems/myesp/esp.log && /etc/init.d/hpcc-init start && tail -f /var/log/HPCCSystems/myesp/esp.log
