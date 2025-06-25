#!/bin/bash
set -e

function detect_arch() {
    if [ -z "$ARCH" ]; then
        if [ "$(uname -m)" == "x86_64" ]; then
            ARCH="x64"
        elif [ "$(uname -m)" == "aarch64" ]; then
            ARCH="arm64"
        elif [ "$(uname -m)" == "arm64" ]; then
            ARCH="arm64"
        else
            echo "Unsupported architecture: $(uname -m)"
            exit 1
        fi
    fi
    if [ "$ARCH" != "x64" ] && [ "$ARCH" != "arm64" ]; then
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi
}

function set_globals() {
    detect_arch
    SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
    ROOT_DIR=$(git rev-parse --show-toplevel)

    set +e
    if [ -f "$ROOT_DIR/.env" ]; then
        export $(grep -v '^#' "$ROOT_DIR/.env" | sed -e 's/\r$//' | xargs) > /dev/null
        docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
    fi
    set -e

    GITHUB_ACTOR="${GITHUB_ACTOR:-hpcc-systems}"
    GITHUB_TOKEN="${GITHUB_TOKEN:-none}"
    GITHUB_REF=$(git rev-parse --short=8 HEAD)
    pushd $ROOT_DIR/vcpkg
    VCPKG_REF=$(git rev-parse --short=8 HEAD)
    popd
    if [ "$ARCH" == "arm64" ]; then
        VCPKG_REF="$VCPKG_REF-arm"
    fi

    GITHUB_BRANCH=$(git log -50 --pretty=format:"%D" | tr ',' '\n' | grep 'upstream/' | awk 'NR==1 {sub("upstream/", ""); print}' | xargs)
    GITHUB_BRANCH=${GITHUB_BRANCH:-master}
    DOCKER_USERNAME="${DOCKER_USERNAME:-hpccbuilds}"
    DOCKER_PASSWORD="${DOCKER_PASSWORD:-none}"

    CMAKE_BUILD_TYPE="RelWithDebInfo"
    CMAKE_ALL_OPTIONS="-DHPCC_SOURCE_DIR=/hpcc-dev/HPCC-Platform -DCONTAINERIZED=OFF -DCPACK_STRIP_FILES=ON -DINCLUDE_PLUGINS=ON -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF -DUSE_CPPUNIT=ON -DSUPPRESS_REMBED=ON -DSUPPRESS_V8EMBED=ON"
    CMAKE_OPENBLAS_OPTIONS="-DHPCC_SOURCE_DIR=/hpcc-dev/HPCC-Platform -DCONTAINERIZED=OFF -DCPACK_STRIP_FILES=OFF -DECLBLAS=ON -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF"
    CMAKE_PLATFORM_OPTIONS="-DHPCC_SOURCE_DIR=/hpcc-dev/HPCC-Platform -DCONTAINERIZED=OFF -DCPACK_STRIP_FILES=ON -DPLATFORM=ON -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF"

    USER_ID=$(id -u)
    GROUP_ID=$(id -g)
}

function print_globals() {
    echo "ARCH: $ARCH"
    echo "ROOT_DIR: $ROOT_DIR"
    echo "SCRIPT_DIR: $SCRIPT_DIR"
    echo "GITHUB_REF: $GITHUB_REF"
    echo "VCPKG_REF: $VCPKG_REF"
    echo "GITHUB_BRANCH: $GITHUB_BRANCH"
    echo "CMAKE_ALL_OPTIONS: $CMAKE_ALL_OPTIONS"
    echo "CMAKE_OPENBLAS_OPTIONS: $CMAKE_OPENBLAS_OPTIONS"
    echo "CMAKE_PLATFORM_OPTIONS: $CMAKE_PLATFORM_OPTIONS"
    echo "GITHUB_ACTOR: $GITHUB_ACTOR"
    echo "GITHUB_TOKEN: $GITHUB_TOKEN"
    echo "DOCKER_USERNAME: $DOCKER_USERNAME"
    echo "DOCKER_PASSWORD: $DOCKER_PASSWORD"
    echo "USER_ID: $USER_ID"
    echo "GROUP_ID: $GROUP_ID"
}

function cleanup() {
    rm -f $ROOT_DIR/vcpkg/vcpkg || true
    kill $(jobs -p)
}

function doBuild() {

    docker buildx build --progress plain --rm -f "$SCRIPT_DIR/$1.dockerfile" \
        --build-arg VCPKG_REF=$VCPKG_REF \
        --build-arg GROUP_ID=$GROUP_ID \
        --build-arg USER_ID=$USER_ID \
        --cache-from hpccsystems/platform-build-$1:$VCPKG_REF \
        --cache-from hpccsystems/platform-build-$1:$GITHUB_BRANCH \
        -t hpccsystems/platform-build-$1:$VCPKG_REF \
        -t hpccsystems/platform-build-$1:$GITHUB_BRANCH \
        "$SCRIPT_DIR/."

    rm -f $ROOT_DIR/vcpkg/vcpkg 

    mkdir -p $ROOT_DIR/build/$1
    CMAKE_OPTIONS_EXTRA="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    if [ "$1" == "wasm32-emscripten" ]; then
        CMAKE_BUILD_TYPE="MinSizeRel"
        CMAKE_OPTIONS_EXTRA="-DEMSCRIPTEN=ON $CMAKE_OPTIONS_EXTRA"
    fi

    mkdir -p $HOME/.ccache
    docker run --rm \
        --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached \
        --mount source="$(realpath ~)/.cache/vcpkg",target=/root/.cache/vcpkg,type=bind,consistency=cached \
        --mount source="$HOME/.ccache",target=/root/.ccache,type=bind,consistency=cached \
        hpccsystems/platform-build-$1:$VCPKG_REF \
        "rm -rf /hpcc-dev/HPCC-Platform/build/$1/CMakeCache.txt /hpcc-dev/HPCC-Platform/build/$1/CMakeFiles && \
        cmake -S /hpcc-dev/HPCC-Platform -B /hpcc-dev/HPCC-Platform/build/$1 -G Ninja -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ${CMAKE_ALL_OPTIONS} ${CMAKE_OPTIONS_EXTRA} && \
        cmake --build /hpcc-dev/HPCC-Platform/build/$1 --parallel || \
        cat /hpcc-dev/vcpkg_buildtrees/detect_compiler/config-x64-linux-dynamic-rel-err.log && \
        echo 'Done'"
    # docker run -it --mount source="$(pwd)",target=/hpcc-dev/HPCC-Platform,type=bind,consistency=cached build-ubuntu-22.04:latest bash

    rm -f $ROOT_DIR/vcpkg/vcpkg 
}

set_globals
print_globals

if [ "$1" != "" ]; then
    doBuild $1
else
    trap 'cleanup; exit' EXIT
    mkdir -p $ROOT_DIR/build/docker-logs
    doBuild wasm32-emscripten &> $ROOT_DIR/build/docker-logs/wasm32-emscripten.log &
    doBuild ubuntu-24.04 &> $ROOT_DIR/build/docker-logs/ubuntu-24.04.log &
    doBuild ubuntu-22.04 &> $ROOT_DIR/build/docker-logs/ubuntu-22.04.log &
    doBuild ubuntu-20.04 &> $ROOT_DIR/build/docker-logs/ubuntu-20.04.log &
    doBuild rockylinux-8 &> $ROOT_DIR/build/docker-logs/rockylinux-8.log &
    doBuild centos-7 &> $ROOT_DIR/build/docker-logs/centos-7.log & 
    wait
fi
