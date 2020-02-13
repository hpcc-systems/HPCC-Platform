#!/bin/bash

BASE_VER=7.8                                    # The docker hub label for the platform-build-base image. Changes rarely.
BUILD_TAG=$(git describe --exact-match --tags)  # The git tag for the images we are building
BUILD_LABEL=${BUILD_TAG}                        # The docker hub label for all other components
BUILD_USER=hpcc-systems                         # The github repo owner
BUILD_TYPE=                                     # Set to Debug for a debug build, leave blank for default (RelWithDebInfo)

# These values are set in a GitHub workflow build

[[ -n ${INPUT_BUILD_USER} ]] && BUILD_USER=${INPUT_BUILD_USER}
[[ -n ${INPUT_BUILD_VER} ]] && BUILD_TAG=${INPUT_BUILD_VER}
[[ -n ${GITHUB_REPOSITORY} ]] && BUILD_USER=${GITHUB_REPOSITORY%/*}

if [[ -n ${INPUT_BUILDTYPE} ]] ; then
  BUILD_TYPE=$INPUT_BUILDTYPE
  BUILD_LABEL=${BUILD_TAG}-$INPUT_BUILDTYPE
else
  BUILD_TYPE=RelWithDebInfo
fi

if [[ -n ${INPUT_USERNAME} ]] ; then
  echo ${INPUT_PASSWORD} | docker login -u ${INPUT_USERNAME} --password-stdin ${INPUT_REGISTRY}
  PUSH=1
fi

if [[ -z ${BUILD_TAG} ]] ; then
  echo Current tag could not be located
  echo Perhaps you meant to run incr.sh ?
  exit 2
fi

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR 2>&1 > /dev/null

build_image() {
  local name=$1
  local label=$2
  [[ -z ${label} ]] && label=$BUILD_LABEL

  if ! docker pull hpccsystems/${name}:${label} ; then
    docker image build -t hpccsystems/${name}:${label} \
       --build-arg BASE_VER=${BASE_VER} \
       --build-arg BUILD_TAG=${BUILD_TAG} \
       --build-arg BUILD_LABEL=${BUILD_LABEL} \
       --build-arg BUILD_USER=${BUILD_USER} \
       --build-arg BUILD_TYPE=${BUILD_TYPE} \
       ${name}/ 
    if [ "$PUSH" = "1" ] ; then
      docker push hpccsystems/${name}:${label}
    fi
  fi
}

build_image platform-build-base ${BASE_VER}
build_image platform-build
build_image platform-core
build_image roxie
build_image dali
build_image esp
build_image eclccserver
build_image eclagent
build_image toposerver
build_image thormaster
build_image thorslave

if [[ -n ${INPUT_PASSWORD} ]] ; then
  echo "::set-output name=${BUILD_LABEL}"
  docker logout
fi
