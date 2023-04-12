#!/bin/bash
set -e

globals() {
    SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )"
    ROOT_DIR=$(git rev-parse --show-toplevel)

    export $(grep -v '^#' $ROOT_DIR/.env | sed -e 's/\r$//' | xargs) > /dev/null

    GIT_REF=$(git rev-parse --short=8 HEAD)
    GIT_BRANCH=$(git branch --show-current)

    pushd $ROOT_DIR/vcpkg
    VCPKG_REF=$(git rev-parse --short=8 HEAD)
    popd
    DOCKER_USERNAME="${DOCKER_USERNAME:-hpccbuilds}"

    CMAKE_OPTIONS="-G Ninja -DVCPKG_FILES_DIR=/hpcc-dev -DCPACK_THREADS=$(docker info --format '{{.NCPU}}') -DUSE_OPTIONAL=OFF -DCONTAINERIZED=ON -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON"

    HPCC_BUILD="hpcc_build_$MODE"
}

create_build_image() {
    echo "--- Create 'build-$BUILD_OS:$VCPKG_REF' image---"
    docker build --rm -f "$SCRIPT_DIR/vcpkg/$BUILD_OS.dockerfile" \
        -t build-$BUILD_OS:$VCPKG_REF \
        --build-arg DOCKER_NAMESPACE=$DOCKER_USERNAME \
        --build-arg VCPKG_REF=$VCPKG_REF \
            "$SCRIPT_DIR/." 
}

create_platform_core_image() {
    local base=$1
    echo "--- Create 'platform-core:$MODE' image ---"
    docker build --rm -f "$SCRIPT_DIR/vcpkg/platform-core-$BUILD_OS.dockerfile" \
        -t platform-core:$MODE \
        --build-arg BASE_IMAGE=$base \
            "$SCRIPT_DIR/vcpkg/." 
}

finalize_platform_core_image() {
    local crc=$1
    local cmd=$2
    local image_name=platform-core:$MODE
    echo "--- Finalize '$image_name' image ---"
    if [ "$ACTION" == "incr" ] && [[ $(docker images -q incr-core:$MODE 2> /dev/null) ]]; then
        local image_name=incr-core:$MODE
        echo "--- Incremental '$image_name' image ---"
    fi
    CONTAINER=$(docker run -d \
        --mount source=hpcc_src,target=/hpcc-dev/HPCC-Platform,type=volume \
        --mount source=$HPCC_BUILD,target=/hpcc-dev/build,type=volume \
        $image_name "tail -f /dev/null")
    docker exec --user root $CONTAINER /bin/bash -c "$cmd"
    docker exec --user root $CONTAINER /bin/bash -c "eclcc -pch"
    docker commit $CONTAINER hpccsystems/platform-core:$GIT_BRANCH-$MODE-$crc
    docker stop $CONTAINER
    docker rm $CONTAINER
    docker tag hpccsystems/platform-core:$GIT_BRANCH-$MODE-$crc incr-core:$MODE
}

clean() {
    echo "--- Clean  ---"
    docker volume rm hpcc_src hpcc_build hpcc_build_debug hpcc_build_release hpcc_opt 2>/dev/null || true
}

run() {
    local cmd=$1
    docker run --rm \
        --mount source=$ROOT_DIR,target=/hpcc-dev/HPCC-Platform-local,type=bind,readonly \
        --mount source=hpcc_src,target=/hpcc-dev/HPCC-Platform,type=volume \
        --mount source="$ROOT_DIR/.git",target=/hpcc-dev/HPCC-Platform/.git,type=bind \
        --mount source=$HPCC_BUILD,target=/hpcc-dev/build,type=volume \
        build-$BUILD_OS:$VCPKG_REF \
            "cd /hpcc-dev/HPCC-Platform && \
            $cmd"
}

init_hpcc_src() {
    echo "--- Init hpcc_src volume ---"
    if ! docker volume ls -q -f name=hpcc_src | grep -q hpcc_src; then
        echo "--- git reset ---"
        run "git reset --hard --recurse-submodules"
    fi
}

reconfigure() {
    echo "--- Clean cmake cache ---"
    init_hpcc_src
    run "rm -rf /hpcc-dev/HPCC-Platform/vcpkg/vcpkg && \
        rm -rf /hpcc-dev/build/CMakeCache.txt CMakeFiles"
}

configure() {
    local options=$1
    echo "--- cmake config $options ---"
    run "cmake -S /hpcc-dev/HPCC-Platform -B /hpcc-dev/build $options"
}

fetch_build_type() {
    echo $(run "grep 'CMAKE_BUILD_TYPE:' /hpcc-dev/build/CMakeCache.txt | cut -d '=' -f 2")
}

md5_hash() {
    if command -v md5sum > /dev/null; then
        # Use md5sum on Linux
        md5sum "$1" | cut -d ' ' -f 1
    else
        # Use md5 on macOS
        md5 -r "$1" | cut -d ' ' -f 1
    fi
}

calc_diffs() {
    init_hpcc_src
    local rsync_tmp_basename=$(basename "$RSYNC_TMP_FILE")
    run "git ls-files --modified --exclude-standard" > $RSYNC_TMP_FILE
    run "rsync -av --delete --files-from=/hpcc-dev/HPCC-Platform-local/$rsync_tmp_basename /hpcc-dev/HPCC-Platform-local/ /hpcc-dev/HPCC-Platform/"

    pushd $ROOT_DIR >/dev/null
    git status --short | grep '^ M' | cut -c4- > $RSYNC_TMP_FILE
    local tmp=$(mktemp)
    echo "$GIT_REF" > $tmp
    while read file; do
        if [ -f "$file" ]; then
            md5_hash "$file" >> $tmp
        fi
    done < $RSYNC_TMP_FILE
    local crc=$(md5_hash "$tmp")
    rm $tmp
    popd >/dev/null
    echo $crc
}

sync_files() {
    echo "--- Sync files ---"
    local rsync_tmp_basename=$(basename "$RSYNC_TMP_FILE")
    run "rsync -av --delete --files-from=/hpcc-dev/HPCC-Platform-local/$rsync_tmp_basename /hpcc-dev/HPCC-Platform-local/ /hpcc-dev/HPCC-Platform/ && 
        git submodule update --init --recursive"
}

check_cache() {
    local crc=$1
    echo "--- Check cache hpccsystems/platform-core:$GIT_BRANCH-$MODE-$crc ---"
    image_count=$(docker images --quiet hpccsystems/platform-core:$GIT_BRANCH-$MODE-$crc | wc -l)
    if [ $image_count -gt 0 ]; then
        echo "--- Image already exists  --- "
        echo "docker run --entrypoint /bin/bash -it hpccsystems/platform-core:$GIT_BRANCH-$MODE-$crc"
        echo "hpccsystems/platform-core:$GIT_BRANCH-$MODE-$crc"
        rm $RSYNC_TMP_FILE
        exit 0
    fi
}

build() {
    create_build_image
    if [ "$MODE" = "release" ]; then
        local base=ubuntu:jammy-20230308
        local build_type="Release"
        local cmake_options="-DCMAKE_BUILD_TYPE=$build_type"
    elif [ "$MODE" = "debug" ]; then
        local base=build-$BUILD_OS:$VCPKG_REF
        local build_type="Debug"
        local cmake_options="-DCMAKE_BUILD_TYPE=$build_type"
    else
        echo "Invalid build mode: $MODE"
        usage
        exit 1
    fi

    if [ "$RECONFIGURE" -eq 1 ]; then
        reconfigure
    fi

    create_platform_core_image $base

    RSYNC_TMP_FILE=$(mktemp "$ROOT_DIR/tempfile.XXXXXX")

    local crc=$(calc_diffs | tail -1)

    check_cache $crc

    sync_files

    rm $RSYNC_TMP_FILE

    local cmakecache_exists=$(run "test -e /hpcc-dev/build/CMakeCache.txt && echo '1' || echo '0'")

    if [ "$RECONFIGURE" -eq 1 ] || [ "$cmakecache_exists" == "0" ]; then
        configure "$CMAKE_OPTIONS $cmake_options"
    fi

    if [ "$MODE" = "release" ]; then
        run "rm -rf /hpcc-dev/build/*.deb || true"
        run "cmake --build /hpcc-dev/build --parallel --target package"
        finalize_platform_core_image $crc \
            "dpkg -i /hpcc-dev/build/hpccsystems-platform*.deb"
    elif [ "$MODE" = "debug" ]; then
        run "cmake --build /hpcc-dev/build --parallel"
        finalize_platform_core_image $crc \
            "cmake --build /hpcc-dev/build --parallel --target install"
    fi

    echo "docker run --entrypoint /bin/bash -it hpccsystems/platform-core:$GIT_BRANCH-$crc"
    echo "hpccsystems/platform-core:$GIT_BRANCH-$crc"
    exit 0
}

incr() {
    MODE=debug
    build
}

function cleanup() {
    rm $RSYNC_TMP_FILE 2> /dev/null || true
}

trap cleanup EXIT

status() {
    echo "SCRIPT_DIR: $SCRIPT_DIR"
    echo "ROOT_DIR: $ROOT_DIR"
    echo "GIT_REF: $GIT_REF"
    echo "GIT_BRANCH: $GIT_BRANCH"
    echo "VCPKG_REF: $VCPKG_REF"
    echo "DOCKER_USERNAME: $DOCKER_USERNAME"
    echo "ACTION: $ACTION"
    echo "MODE: $MODE"
    echo "RECONFIGURE: $RECONFIGURE"
    echo "BUILD_OS: $BUILD_OS"
    echo "HPCC_BUILD: $HPCC_BUILD"
}

# Print usage information
usage() {
    echo "Usage: $0 [-h] {clean|build|incr} [-m MODE] [-r] [OS]"
    echo "  -h, --help     display this help message"
    echo "  clean          remove all build artifacts"
    echo "  build          build the project"
    echo "  incr           perform an incremental build (faster version of 'build -m debug')"
    echo "  status         display environment variables"
    echo "  -m, --mode     specify the build mode (debug or release)"
    echo "                 default mode is release"
    echo "  -r, --reconfigure reconfigure CMake before building"
}

# Set default values
ACTION=
MODE=release
RECONFIGURE=0
BUILD_OS="ubuntu-22.04"

# Parse command line arguments
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    clean)
        ACTION=clean
        shift # past argument
        ;;
    build)
        ACTION=build
        shift # past argument
        ;;
    incr)
        ACTION=incr
        shift # past argument
        ;;
    status)
        ACTION=status
        shift # past argument
        ;;
    -m|--mode)
        MODE="$2"
        shift # past argument
        shift # past value
        ;;
    -r|--reconfigure)
        RECONFIGURE=1
        shift # past argument
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)    # unknown option
        echo "Unknown option: $key"
        usage
        exit 1
        ;;
esac
done

globals

# Call the appropriate function based on the selected action
case $ACTION in
    clean)
        clean
        ;;
    build)
        build
        ;;
    incr)
        incr
        ;;
    status)
        status
        ;;
    *)
        echo "Invalid action selected: $ACTION"
        usage
        exit 1
        ;;
esac
