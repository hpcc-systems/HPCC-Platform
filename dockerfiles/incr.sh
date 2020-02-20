#!/bin/bash

# Use this script to build local images for testing, while leveraging the work already done in the most recent tag
# on the branch that your local branch was based on
#
# Pass in a get commit (that you previously built using this script) if you want to override the default calculation
# of the base build

HEAD=$(git rev-parse --short HEAD)-Debug
PREV=$1
[[ -z ${PREV} ]] && PREV=$(git log --format=format:%h $(git describe --abbrev=0 --tags)..HEAD | grep `docker images hpccsystems/platform-core --format {{.Tag}} | head -n 1`)
[[ -z ${PREV} ]] && PREV=$(git describe --abbrev=0 --tags)

BUILD_TYPE=Debug
BUILD_LABEL=${HEAD}-Debug

if [[ "$HEAD" == "$PREV$FORCE" ]]  # set environment variable FORCE before running to override this check
then
    echo Docker image hpccsystems/platform-core:${HEAD} already exists
else
    set -e
    
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    pushd $DIR 2>&1 > /dev/null
    
    echo Building local incremental images based on ${PREV}
        
    if [[ -n "$FORCE" ]] ; then
      docker image build -t hpccsystems/platform-build:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} --build-arg BASE_VER=7.8 --build-arg BUILD_TYPE=Debug platform-build/
    else
      docker image build -t hpccsystems/platform-build:${BUILD_LABEL} --build-arg BUILD_VER=${PREV} --build-arg COMMIT=${HEAD} platform-build-incremental/
    fi
    docker image build -t hpccsystems/platform-core:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} platform-core-debug/  
    
    docker image build -t hpccsystems/roxie:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} roxie/
    docker image build -t hpccsystems/dali:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} dali/
    docker image build -t hpccsystems/esp:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} esp/
    docker image build -t hpccsystems/eclccserver:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} eclccserver/
    docker image build -t hpccsystems/eclagent:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} eclagent/
    docker image build -t hpccsystems/toposerver:${BUILD_LABEL} --build-arg BUILD_VER=${HEAD} toposerver/
    
    echo Built hpccsystems/*:${HEAD}
fi
