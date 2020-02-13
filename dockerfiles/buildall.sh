#!/bin/bash

#eval $(minikube docker-env)
#docker rm $(docker ps -q -f 'status=exited')
#docker rmi $(docker images -q -f "dangling=true")

BASE_VER=7.8
BUILD_VER=$1
BUILD_USER=$2
[[ -z ${BUILD_USER} ]] && BUILD_USER=${INPUT_BUILD_USER}
[[ -z ${BUILD_USER} ]] && BUILD_USER=${GITHUB_REPOSITORY%/*}
[[ -z ${BUILD_USER} ]] && BUILD_USER=hpcc-systems
[[ -z ${BUILD_VER} ]] && BUILD_VER=${INPUT_BUILD_VER}
[[ -z ${BUILD_VER} ]] && BUILD_VER=$(git describe --exact-match --tags)

if [[ -n ${INPUT_USERNAME} ]] ; then
  echo ${INPUT_PASSWORD} | docker login -u ${INPUT_USERNAME} --password-stdin ${INPUT_REGISTRY}
  PUSH=1
fi

if [[ -z ${BUILD_VER} ]] ; then
  echo Current tag could not be located
  exit 2
fi

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR 2>&1 > /dev/null

build_image() {
  local name=$1
  local ver=$2
  local base="$3"
  [[ -z $ver ]] || local usever="--build-arg BUILD_VER=$ver"
  [[ -z $base ]] || local usebase="--build-arg BASE_VER=$base"
  local useuser="--build-arg BUILD_USER=$BUILD_USER"

  if ! docker pull hpccsystems/${name}:${ver} ; then
    docker image build -t hpccsystems/${name}:${ver} ${usever} ${usebase} ${useuser} ${name}/ 
    if [ "$PUSH" = "1" ] ; then
      docker push hpccsystems/${name}:${ver}
    fi
  fi
}

build_image platform-build-base ${BASE_VER}
build_image platform-build ${BUILD_VER} ${BASE_VER}
build_image platform-core ${BUILD_VER}
build_image roxie ${BUILD_VER}
build_image dali ${BUILD_VER}
build_image esp ${BUILD_VER}
build_image eclccserver ${BUILD_VER}
build_image eclagent ${BUILD_VER}
build_image thormaster ${BUILD_VER}
build_image thorslave ${BUILD_VER}

if [[ -n ${INPUT_PASSWORD} ]] ; then
  echo "::set-output name=${BUILD_VER}"
  docker logout
fi
