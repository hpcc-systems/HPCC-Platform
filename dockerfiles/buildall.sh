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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR 2>&1 > /dev/null

. ./buildall-common.sh

if [[ -n ${INPUT_USERNAME} ]] ; then
  echo ${INPUT_PASSWORD} | docker login -u ${INPUT_USERNAME} --password-stdin ${INPUT_REGISTRY}
  PUSH=1
fi

set -e


build_image platform-build-base ${BASE_VER}
build_image platform-build
build_image platform-core
build_ml_image

if [[ -n ${INPUT_PASSWORD} ]] ; then
  echo "::set-output name=${BUILD_LABEL}"
  docker logout
fi
