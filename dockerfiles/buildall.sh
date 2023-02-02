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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd $SCRIPT_DIR 2>&1 > /dev/null

set -e

. ${SCRIPT_DIR}/../cmake_modules/parse_cmake.sh ${SCRIPT_DIR}/../version.cmake
parse_cmake
set_tag $HPCC_PROJECT

. ./buildall-common.sh

if [[ -n ${INPUT_USERNAME} ]] ; then
  echo ${INPUT_PASSWORD} | docker login -u ${INPUT_USERNAME} --password-stdin ${INPUT_REGISTRY}
  PUSH=1
fi

if [[ -n ${INPUT_LN_USERNAME} ]] ; then
  echo ${INPUT_LN_PASSWORD} | docker login -u ${INPUT_LN_USERNAME} --password-stdin ${INPUT_LN_REGISTRY}
  PUSH_LN=1
fi

BUILD_ML=    # all or ml,gnn,gnn-gpu
[[ -n ${INPUT_BUILD_ML} ]] && BUILD_ML=${INPUT_BUILD_ML}

BUILD_LN=
[[ -n ${INPUT_BUILD_LN} ]] && BUILD_LN=${INPUT_BUILD_LN}

LNB_TOKEN=
[[ -n ${INPUT_LNB_TOKEN} ]] && LNB_TOKEN=${INPUT_LNB_TOKEN}

set -e

ml_features=(
  'ml'
  'gnn'
  'gnn-gpu'
)

build_ml_images() {
  [ -z "$BUILD_ML" ] && return

  local label=$1
  [[ -z ${label} ]] && label=$BUILD_LABEL
  features=()
  if [ "$BUILD_ML" = "all" ]
  then
    features=(${ml_features[@]})
  else
    for feature in $(echo ${BUILD_ML} | sed 's/,/ /g')
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
     build_image "platform-$feature"
  done
}

if [[ -n "$BUILD_LN" ]]; then
  set_tag "internal"
  GITHUB_TOKEN=${LNB_TOKEN}
  lnBuildTag=${BUILD_TAG/community_/internal_}
  build_image platform-build-ln ${BUILD_LABEL} ${lnBuildTag}
  build_image platform-core-ln ${BUILD_LABEL} ${lnBuildTag} --build-arg BUILD_TAG_OVERRIDE=${HPCC_LONG_TAG}
elif [[ -z "$BUILD_ML" ]]; then
  build_image platform-build
  build_image platform-core
else
  docker pull ${DOCKER_REPO}/platform-core:${BUILD_LABEL}
  build_ml_images
fi

if [[ -n ${INPUT_PASSWORD} ]] ; then
  echo "::set-output name=${BUILD_LABEL}"
  docker logout
fi

#cleanup any github secrets stored for BuildKit mounting
if [[ -n {INPUT_SIGNING_SECRET} ]] ; then
  rm -rf private.key
fi

if [[ -n {INPUT_SIGNING_PASSPHRASE} ]] ; then
  rm -rf passphrase.txt
fi
