#!/bin/bash

##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################

# Build script to create and publish Docker containers corresponding to a GitHub tag
# This script is normally invoked via GitHub actions, whenever a new tag is pushed 

BASE_VER=7.12                                   # The docker hub label for the platform-build-base image. Changes rarely.
BUILD_TAG=$(git describe --exact-match --tags)  # The git tag for the images we are building
BUILD_LABEL=${BUILD_TAG}                        # The docker hub label for all other components
BUILD_USER=hpcc-systems                         # The github repo owner
BUILD_TYPE=                                     # Set to Debug for a debug build, leave blank for default (RelWithDebInfo)
USE_CPPUNIT=1

#BUILD_ML=all #ml,gnn,gnn-gpu
ml_features=(
  'ml'
  'gnn'
  'gnn-gpu'
)

# These values are set in a GitHub workflow build

[[ -n ${INPUT_BUILD_USER} ]] && BUILD_USER=${INPUT_BUILD_USER}
[[ -n ${INPUT_BUILD_VER} ]] && BUILD_TAG=${INPUT_BUILD_VER}
[[ -n ${GITHUB_REPOSITORY} ]] && BUILD_USER=${GITHUB_REPOSITORY%/*}

if [[ -n ${INPUT_USERNAME} ]] ; then
  echo ${INPUT_PASSWORD} | docker login -u ${INPUT_USERNAME} --password-stdin ${INPUT_REGISTRY}
  PUSH=1
fi

if [[ -n ${INPUT_BUILD_THREADS} ]] ; then
  BUILD_THREADS=$INPUT_BUILD_THREADS
fi

if [[ -z ${BUILD_TAG} ]] ; then
  echo Current tag could not be located
  echo Perhaps you meant to run incr.sh ?
  exit 2
fi

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR 2>&1 > /dev/null

. ../cmake_modules/parse_cmake.sh
parse_cmake
set_tag

if [[ -n ${INPUT_BUILDTYPE} ]] ; then
  BUILD_TYPE=$INPUT_BUILDTYPE
  BUILD_LABEL=${HPCC_SHORT_TAG}-$INPUT_BUILDTYPE
else
  BUILD_LABEL=${HPCC_SHORT_TAG}
  BUILD_TYPE=RelWithDebInfo
  USE_CPPUNIT=0
fi

if [[ "$HPCC_MATURITY" = "release" ]] && [[ "$INPUT_LATEST" = "1" ]] ; then
  LATEST=1
fi

build_image() {
  local name=$1
  local label=$2
  [[ -z ${label} ]] && label=$BUILD_LABEL

  if ! docker pull hpccsystems/${name}:${label} ; then
    docker image build -t hpccsystems/${name}:${label} \
       --build-arg BASE_VER=${BASE_VER} \
       --build-arg DOCKER_REPO=hpccsystems \
       --build-arg BUILD_TAG=${BUILD_TAG} \
       --build-arg BUILD_LABEL=${BUILD_LABEL} \
       --build-arg BUILD_USER=${BUILD_USER} \
       --build-arg BUILD_TYPE=${BUILD_TYPE} \
       --build-arg USE_CPPUNIT=${USE_CPPUNIT} \
       --build-arg BUILD_THREADS=${BUILD_THREADS} \
       ${name}/ 
  fi
  push_image $name $label
}

push_image() {
  local name=$1
  local label=$2
  if [ "$LATEST" = "1" ] ; then
    docker tag hpccsystems/${name}:${label} hpccsystems/${name}:latest
    if [ "$PUSH" = "1" ] ; then
      docker push hpccsystems/${name}:${label}
      docker push hpccsystems/${name}:latest
    fi
  else
    if [ "$PUSH" = "1" ] ; then
      docker push hpccsystems/${name}:${label}
    fi
  fi
}

build_ml_image() {
  [ -z "$BUILD_ML" ] && return
  features=()
  if [ "$BUILD_ML" = "all" ]
  then
    features=(${ml_features[@]})
  else
    for feature in ${BUILD_ML}
    do
      found=false
      for ml_feature in ${ml_features[@]}
      do
        if [[ $ml_feature == $feature ]]
	then
	  features+=(${feature})
	  found=true
	  break
        fi
      done
      if [ "$found" = "false" ]
      then
	printf "\nUnknown ML feature %s\n" "$feature"
      fi
    done
  fi

  for feature in ${features[@]}
  do
     echo "build_ml $feature"
     build_ml $feature
  done

}

build_ml() {
  local name=$1
  local label=$2
  [[ -z ${label} ]] && label=$BUILD_LABEL
  docker image build -t hpccsystems/platform-${name}:${label} \
     --build-arg DOCKER_REPO=hpccsystems \
     --build-arg BUILD_LABEL=${label} \
     ml/${name}/
}

build_image platform-build-base ${BASE_VER}
build_image platform-build
build_image platform-core
build_ml_image

if [[ -n ${INPUT_PASSWORD} ]] ; then
  echo "::set-output name=${BUILD_LABEL}"
  docker logout
fi
