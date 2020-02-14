#!/bin/bash
HEAD=$(git rev-parse --short HEAD)

PREV=$1
BUILD_USER=$2
[[ -z ${BUILD_USER} ]] && BUILD_USER=${INPUT_BUILD_USER}
[[ -z ${BUILD_USER} ]] && BUILD_USER=${GITHUB_REPOSITORY%/*}
[[ -z ${BUILD_USER} ]] && BUILD_USER=hpcc-systems

[[ -z ${PREV} ]] && BUILD_VER=$(git rev-parse --short HEAD^)

set -e

build_image() {
  local name=$1
  local ver=$2
  [[ -z $ver ]] || local usever="--build-arg BUILD_VER=$ver"
  local useuser="--build-arg BUILD_USER=$BUILD_USER"

  if ! docker pull hpccsystems/${name}:${ver} ; then
    docker image build -t hpccsystems/${name}:${ver} ${usever} ${useuser} ${name}/ 
    if [ "$PUSH" = "1" ] ; then
      docker push hpccsystems/${name}:${ver}
    fi
  fi
}

echo HERE1

#more - only do this if head does not match BUILD_VER
docker image build -t hpccsystems/platform-build:${HEAD} --build-arg=BUILD_USER=${BUILD_USER} --build-arg BUILD_VER=${PREV} --build-arg COMMIT=${HEAD} platform-build-incremental/

echo HERE2

build_image platform-core ${HEAD}

build_image roxie ${HEAD}
build_image dali ${HEAD}
build_image esp ${HEAD}
build_image eclccserver ${HEAD}
build_image eclagent ${HEAD}
build_image thormaster ${HEAD}
build_image thorslave ${HEAD}
