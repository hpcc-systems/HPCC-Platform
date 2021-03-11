#!/bin/bash

SCRIPT_DIR=$(dirname $BASH_SOURCE)

##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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
DOCKER_REPO=hpccsystems
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
[[ -n ${INPUT_DOCKER_REPO} ]] && DOCKER_REPO=${INPUT_DOCKER_REPO}

if [[ -n ${INPUT_BUILD_THREADS} ]] ; then
  BUILD_THREADS=$INPUT_BUILD_THREADS
fi

if [[ -z ${BUILD_TAG} ]] ; then
  echo Current tag could not be located
  echo Perhaps you meant to run incr.sh ?
  exit 2
fi

set -e

if [[ -z ${INPUT_BUILD_LABEL} ]]; then
  . ${SCRIPT_DIR}/../cmake_modules/parse_cmake.sh
  parse_cmake
  set_tag
  BUILD_LABEL=${HPCC_SHORT_TAG}
else
  BUILD_LABEL=${INPUT_BUILD_LABEL}
fi

if [[ -n ${INPUT_BUILD_TYPE} ]] ; then
  BUILD_LABEL=${BUILD_LABEL}-$INPUT_BUILD_TYPE
  BUILD_TYPE=$INPUT_BUILD_TYPE
else
  BUILD_TYPE=RelWithDebInfo
  USE_CPPUNIT=0
fi

if [[ "$HPCC_MATURITY" = "release" ]] && [[ "$INPUT_LATEST" = "1" ]] ; then
  LATEST=1
fi

build_image() {
  local name=$1
  local label=$2
  local buildTag=$3
  [[ -z ${label} ]] && label=$BUILD_LABEL
  [[ -z ${buildTag} ]] && buildTag=$BUILD_TAG

  if ! docker pull ${DOCKER_REPO}/${name}:${label} ; then
    docker image build -t ${DOCKER_REPO}/${name}:${label} \
       --build-arg BASE_VER=${BASE_VER} \
       --build-arg DOCKER_REPO=${DOCKER_REPO} \
       --build-arg BUILD_TAG=${buildTag} \
       --build-arg BUILD_LABEL=${BUILD_LABEL} \
       --build-arg BUILD_USER=${BUILD_USER} \
       --build-arg BUILD_TYPE=${BUILD_TYPE} \
       --build-arg USE_CPPUNIT=${USE_CPPUNIT} \
       --build-arg BUILD_THREADS=${BUILD_THREADS} \
       --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} \
       ${name}/
  fi
  push_image $name $label
}

push_image() {
  local name=$1
  local label=$2
  if [ "$LATEST" = "1" ] ; then
    docker tag ${DOCKER_REPO}/${name}:${label} ${DOCKER_REPO}/${name}:latest
    if [ "$PUSH" = "1" ] ; then
      docker push ${DOCKER_REPO}/${name}:${label}
      docker push ${DOCKER_REPO}/${name}:latest
    fi
  else
    if [ "$PUSH" = "1" ] ; then
      docker push ${DOCKER_REPO}/${name}:${label}
    fi
  fi
}

build_ml_image() {
  label=$1
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
     build_ml $feature $label
  done

}

build_ml() {
  local name=$1
  local label=$2
  [[ -z ${label} ]] && label=$BUILD_LABEL
  docker image build -t ${DOCKER_REPO}/platform-${name}:${label} \
     --build-arg DOCKER_REPO=${DOCKER_REPO} \
     --build-arg BUILD_LABEL=${label} \
     ml/${name}/
}
